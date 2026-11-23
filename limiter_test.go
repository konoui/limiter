package limiter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"
)

// 2 tokens per 4 seconds, burst 4: emission 2s, tolerance 6s, single shard
func testConfig() *Config {
	return &Config{
		TableName:        "dummy_table",
		TokenPerInterval: 2,
		BucketSize:       4,
		Interval:         4 * time.Second,
	}
}

var (
	emission  = int64(2 * time.Second)
	tolerance = int64(6 * time.Second)
)

func fixTime(t *testing.T, d time.Duration) int64 {
	t.Helper()
	base := time.Unix(1_000_000, 0)
	timeNow = func() time.Time { return base.Add(d) }
	t.Cleanup(func() { timeNow = time.Now })
	return base.Add(d).UnixNano()
}

func newAttr(t *testing.T, item *ddbItem) map[string]types.AttributeValue {
	t.Helper()
	attr, err := attributevalue.MarshalMap(item)
	if err != nil {
		t.Fatal(err)
	}
	return attr
}

func ccf(t *testing.T, item *ddbItem) error {
	t.Helper()
	e := &types.ConditionalCheckFailedException{Message: aws.String("ccf")}
	if item != nil {
		e.Item = newAttr(t, item)
	}
	return e
}

func names(in *dynamodb.UpdateItemInput) string {
	ret := []string{}
	for _, v := range in.ExpressionAttributeNames {
		ret = append(ret, v)
	}
	return strings.Join(ret, ",")
}

// registered makes every bucket id registered (the registration item exists).
func registered(t *testing.T, c *MockDDBClient) {
	t.Helper()
	meta, err := attributevalue.MarshalMap(&registrationItem{BucketIDShardID: "x" + metaSuffix, RegisteredAt: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.EXPECT().GetItem(gomock.Any(), gomock.Any()).Return(&dynamodb.GetItemOutput{Item: meta}, nil).AnyTimes()
}

// idle path input
func isIdle(in *dynamodb.UpdateItemInput) bool {
	return !strings.Contains(*in.ConditionExpression, "BETWEEN")
}

func TestRateLimit_Allow(t *testing.T) {
	tests := []struct {
		name      string
		anonymous bool
		failOpen  bool
		mocker    func(t *testing.T, c *MockDDBClient, now int64)
		want      Decision
		wantErr   error
	}{
		{
			name: "idle bucket is allowed with a single write",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
						// registration is checked by the registration item; shard items are created on demand
						if !isIdle(in) || !strings.Contains(*in.ConditionExpression, "attribute_not_exists") {
							t.Errorf("shard item should be created on demand: %s", *in.ConditionExpression)
						}
						if !strings.Contains(names(in), attrTTL) {
							t.Errorf("ttl must be set so that idle shard items expire")
						}
						if in.ReturnValuesOnConditionCheckFailure != types.ReturnValuesOnConditionCheckFailureAllOld {
							t.Errorf("old item is required on condition check failure")
						}
						return &dynamodb.UpdateItemOutput{}, nil
					})
			},
			want: Decision{Allowed: true, Remaining: 3},
		},
		{
			name: "busy bucket is allowed via the busy path",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				old := now + 2*emission
				gomock.InOrder(
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, &ddbItem{TAT: old})),
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
							if isIdle(in) {
								t.Errorf("want busy path: %s", *in.ConditionExpression)
							}
							return &dynamodb.UpdateItemOutput{Attributes: newAttr(t, &ddbItem{TAT: old + emission})}, nil
						}),
				)
			},
			// tat = now + 3 emissions: (6s - 6s) / 2s + 1
			want: Decision{Allowed: true, Remaining: 1},
		},
		{
			name: "exhausted bucket is denied without another write",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, ccf(t, &ddbItem{TAT: now + tolerance + emission}))
			},
			want: Decision{Allowed: false, RetryAfter: 2 * time.Second},
		},
		{
			name: "concurrent idle update falls back to the idle path again",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				gomock.InOrder(
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, &ddbItem{TAT: now - 1})),
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil),
				)
			},
			want: Decision{Allowed: true, Remaining: 3},
		},
		{
			name: "heavy contention is denied for an emission interval",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, ccf(t, &ddbItem{TAT: now + emission})).Times(maxAttempts)
			},
			want: Decision{Allowed: false, RetryAfter: 2 * time.Second},
		},
		{
			name: "registered bucket whose shard item does not exist yet is created",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				gomock.InOrder(
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, nil)),
					c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil),
				)
			},
			want: Decision{Allowed: true, Remaining: 3},
		},
		{
			name:      "anonymous bucket is created with ttl",
			anonymous: true,
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
						if !strings.Contains(*in.ConditionExpression, "attribute_not_exists") {
							t.Errorf("anonymous mode must allow a missing item: %s", *in.ConditionExpression)
						}
						if !strings.Contains(names(in), attrTTL) {
							t.Errorf("ttl must be set in anonymous mode")
						}
						return &dynamodb.UpdateItemOutput{}, nil
					})
			},
			want: Decision{Allowed: true, Remaining: 3},
		},
		{
			name: "dynamodb throttling is denied by default",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")})
			},
			want:    Decision{Allowed: false},
			wantErr: ErrStoreThrottled,
		},
		{
			name:     "dynamodb throttling is allowed with fail-open",
			failOpen: true,
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")})
			},
			want:    Decision{Allowed: true},
			wantErr: ErrStoreThrottled,
		},
		{
			name: "internal error",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.InternalServerError{Message: aws.String("boom")})
			},
			wantErr: ErrInternal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := fixTime(t, 0)
			c := NewMockDDBClient(gomock.NewController(t))
			tt.mocker(t, c, now)
			registered(t, c)

			cfg := testConfig()
			cfg.AdditionalConfig = &AdditionalConfig{FailOpen: tt.failOpen}
			if tt.anonymous {
				cfg.AdditionalConfig.Anonymous = &AnonymousConfig{RecordTTL: time.Hour}
			}
			rl, err := New(cfg, c)
			if err != nil {
				t.Fatal(err)
			}
			got, err := rl.Allow(context.Background(), "dummy")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want error %v, got %v", tt.wantErr, err)
			}
			if got != tt.want {
				t.Errorf("want %+v, got %+v", tt.want, got)
			}
		})
	}
}

func TestRateLimit_Allow_invalidInput(t *testing.T) {
	rl, err := New(testConfig(), NewMockDDBClient(gomock.NewController(t)))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", strings.Repeat("a", 513)} {
		d, err := rl.Allow(context.Background(), id)
		if !errors.Is(err, ErrInvalidBucketID) || d.Allowed {
			t.Errorf("want denied with %v, got %+v %v", ErrInvalidBucketID, d, err)
		}
	}
}

// A denied shard is remembered until it is allowed again, so DynamoDB is not called meanwhile.
func TestRateLimit_Allow_denyCache(t *testing.T) {
	now := fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
		Return(nil, ccf(t, &ddbItem{TAT: now + tolerance + emission}))

	rl, err := New(testConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if d, err := rl.Allow(ctx, "dummy"); err != nil || d.Allowed || d.RetryAfter != 2*time.Second {
		t.Fatalf("unexpected %+v %v", d, err)
	}

	fixTime(t, time.Second)
	d, err := rl.Allow(ctx, "dummy")
	if err != nil || d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("should be denied by the cache for the rest 1s: %+v %v", d, err)
	}

	// expired: DynamoDB is called again
	fixTime(t, 2*time.Second)
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil)
	if d, err := rl.Allow(ctx, "dummy"); err != nil || !d.Allowed {
		t.Fatalf("should be allowed after retry-after: %+v %v", d, err)
	}
}

// A denied shard is skipped and another shard is tried, so a request is not denied while other shards have tokens.
func TestRateLimit_Allow_shardFallback(t *testing.T) {
	now := fixTime(t, 0)
	shardOrder = func(n int) []int { return []int{0, 1} }
	t.Cleanup(func() { shardOrder = rand.Perm })

	// 2 shards: emission 2ms, tolerance 998ms per shard
	cfg := &Config{TableName: "dummy_table", TokenPerInterval: 1000, BucketSize: 1000, Interval: time.Second, Shards: 2}
	const shardEmission, shardTolerance = int64(2 * time.Millisecond), int64(998 * time.Millisecond)
	exhausted := func(retry int64) *ddbItem { return &ddbItem{TAT: now + shardTolerance + retry} }
	shardOf := func(in *dynamodb.UpdateItemInput) string {
		return in.Key[attrBucketIDShardID].(*types.AttributeValueMemberS).Value
	}

	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	reader, opt := newTestMeter()
	rl, err := New(cfg, c, opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rl.gcra.shards) != 2 {
		t.Fatalf("want 2 shards, got %d", len(rl.gcra.shards))
	}
	ctx := context.Background()

	// shard 0 is exhausted, shard 1 allows
	gomock.InOrder(
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
				if shardOf(in) != "k#0" {
					t.Errorf("want shard 0 first, got %s", shardOf(in))
				}
				return nil, ccf(t, exhausted(3*shardEmission))
			}),
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
				if shardOf(in) != "k#1" {
					t.Errorf("want fallback to shard 1, got %s", shardOf(in))
				}
				return &dynamodb.UpdateItemOutput{}, nil
			}),
	)
	if d, err := rl.Allow(ctx, "k"); err != nil || !d.Allowed {
		t.Fatalf("should fall back to shard 1: %+v %v", d, err)
	}

	// shard 0 is skipped by the cache without a write; shard 1 is now exhausted
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			if shardOf(in) != "k#1" {
				t.Errorf("shard 0 should be skipped, got %s", shardOf(in))
			}
			return nil, ccf(t, exhausted(shardEmission))
		})
	d, err := rl.Allow(ctx, "k")
	if err != nil || d.Allowed {
		t.Fatalf("both shards are exhausted: %+v %v", d, err)
	}
	if d.RetryAfter != time.Duration(shardEmission) {
		t.Errorf("retry-after should be the earliest of the shards: %v", d.RetryAfter)
	}

	// all shards are cached: no write
	if d, err := rl.Allow(ctx, "k"); err != nil || d.Allowed {
		t.Fatalf("should be denied by the cache: %+v %v", d, err)
	}

	got := collectCounters(t, reader)["limiter.decisions"]
	for attrs, want := range map[string]int64{
		"limiter.result=allowed,limiter.source=dynamodb,limiter.table=dummy_table": 1,
		"limiter.result=denied,limiter.source=dynamodb,limiter.table=dummy_table":  1,
		"limiter.result=denied,limiter.source=cache,limiter.table=dummy_table":     1,
	} {
		if got[attrs] != want {
			t.Errorf("%s: want %d, got %d", attrs, want, got[attrs])
		}
	}
}

// Writes per request are bounded even when many shards are exhausted.
func TestRateLimit_Allow_shardTriesAreBounded(t *testing.T) {
	now := fixTime(t, 0)
	// 10 shards
	cfg := &Config{TableName: "dummy_table", TokenPerInterval: 5000, BucketSize: 5000, Interval: time.Second}
	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
		Return(nil, ccf(t, &ddbItem{TAT: now + int64(time.Hour)})).Times(maxShardTries)
	rl, err := New(cfg, c)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := rl.Allow(context.Background(), "k"); err != nil || d.Allowed {
		t.Fatalf("want denied: %+v %v", d, err)
	}
}

func getItemOut(t *testing.T, item any) *dynamodb.GetItemOutput {
	t.Helper()
	if item == nil {
		return &dynamodb.GetItemOutput{}
	}
	attrs, err := attributevalue.MarshalMap(item)
	if err != nil {
		t.Fatal(err)
	}
	return &dynamodb.GetItemOutput{Item: attrs}
}

func pkOf(in *dynamodb.GetItemInput) string {
	return in.Key[attrBucketIDShardID].(*types.AttributeValueMemberS).Value
}

func TestRateLimit_Allow_registration(t *testing.T) {
	type legacyItem struct {
		BucketIDShardID string `dynamodbav:"bucket_id_shard_id"`
		TokenCount      int64  `dynamodbav:"token_count"`
	}
	tests := []struct {
		name    string
		meta    any
		shard0  any
		wantErr error
	}{
		{name: "registration item exists", meta: &registrationItem{BucketIDShardID: "k#meta"}},
		{
			name:   "registered by the token bucket version (no registration item)",
			shard0: &legacyItem{BucketIDShardID: "k#0", TokenCount: 3},
		},
		{
			// e.g. revoked by deleting the registration item. the shard item remains.
			name:    "shard item without the registration item is not registered",
			shard0:  &ddbItem{BucketIDShardID: "k#0", TAT: 1},
			wantErr: ErrInvalidBucketID,
		},
		{name: "nothing exists", wantErr: ErrInvalidBucketID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixTime(t, 0)
			c := NewMockDDBClient(gomock.NewController(t))
			c.EXPECT().GetItem(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
					if !aws.ToBool(in.ConsistentRead) {
						t.Error("registration must be read consistently")
					}
					switch pkOf(in) {
					case "k#meta":
						return getItemOut(t, tt.meta), nil
					case "k#0":
						return getItemOut(t, tt.shard0), nil
					}
					t.Fatalf("unexpected key %s", pkOf(in))
					return nil, nil
				}).MaxTimes(2)
			if tt.wantErr == nil {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil).Times(2)
			}

			rl, err := New(testConfig(), c)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				// the second call is served by the registration cache or the negative cache
				d, err := rl.Allow(context.Background(), "k")
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("[%d] want %v, got %v", i, tt.wantErr, err)
				}
				if (err == nil) != d.Allowed {
					t.Errorf("[%d] unexpected decision %+v", i, d)
				}
			}
		})
	}
}

func TestRateLimit_shardExpiry(t *testing.T) {
	now := fixTime(t, 0)
	s := shardParams{emission: 2 * time.Second, tolerance: 6 * time.Second, burst: 4}
	base := time.Unix(0, now)

	registered := &RateLimit{}
	if got, want := registered.shardExpiry(now, s), base.Add(8*time.Second+shardTTLMargin).Unix(); got != want {
		t.Errorf("registered: want %d, got %d", want, got)
	}
	// a long record_ttl is kept
	long := &RateLimit{anonymous: true, ttl: 24 * time.Hour}
	if got, want := long.shardExpiry(now, s), base.Add(24*time.Hour).Unix(); got != want {
		t.Errorf("long record_ttl: want %d, got %d", want, got)
	}
	// a short record_ttl must not delete an item before its TAT passes
	short := &RateLimit{anonymous: true, ttl: time.Second}
	if got, want := short.shardExpiry(now, s), base.Add(8*time.Second+shardTTLMargin).Unix(); got != want {
		t.Errorf("short record_ttl: want %d, got %d", want, got)
	}
}

func TestRateLimit_RevokeTokens(t *testing.T) {
	fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	deleted := []string{}
	c.EXPECT().DeleteItem(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			deleted = append(deleted, in.Key[attrBucketIDShardID].(*types.AttributeValueMemberS).Value)
			return &dynamodb.DeleteItemOutput{}, nil
		}).Times(2)
	rl, err := New(testConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rl.rcache.AddUntil("k", timeNow().Add(time.Minute))
	if err := rl.RevokeTokens(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(deleted, ",") != "k#meta,k#0" {
		t.Errorf("registration item and the legacy shard 0 should be deleted: %v", deleted)
	}
	// this instance stops allowing it immediately, without reading DynamoDB
	if _, err := rl.Allow(ctx, "k"); !errors.Is(err, ErrInvalidBucketID) {
		t.Errorf("want %v, got %v", ErrInvalidBucketID, err)
	}
	if err := rl.RevokeTokens(ctx, ""); !errors.Is(err, ErrInvalidBucketID) {
		t.Errorf("want %v, got %v", ErrInvalidBucketID, err)
	}
}

func TestRateLimit_Allow_registrationReadFailure(t *testing.T) {
	fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	c.EXPECT().GetItem(gomock.Any(), gomock.Any()).
		Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")})
	rl, err := New(testConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	d, err := rl.Allow(context.Background(), "k")
	if !errors.Is(err, ErrStoreThrottled) || d.Allowed {
		t.Fatalf("want denied with %v, got %+v %v", ErrStoreThrottled, d, err)
	}
	if rl.ncache.Contains("k") {
		t.Error("a read failure must not be cached as unregistered")
	}
}

// An unregistered id is cached per limiter with a TTL, and PrepareTokens clears it.
func TestRateLimit_Allow_negativeCache(t *testing.T) {
	fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	c.EXPECT().GetItem(gomock.Any(), gomock.Any()).Return(&dynamodb.GetItemOutput{}, nil).Times(2)

	a, err := New(testConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(testConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := a.Allow(ctx, "unknown"); !errors.Is(err, ErrInvalidBucketID) {
		t.Fatal(err)
	}
	// served by the cache: no DynamoDB call
	if _, err := a.Allow(ctx, "unknown"); !errors.Is(err, ErrInvalidBucketID) || !strings.Contains(err.Error(), "cached") {
		t.Fatalf("should be served by the negative cache: %v", err)
	}
	if other.ncache.Contains("unknown") {
		t.Error("negative cache must not be shared between limiters")
	}

	c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).Return(&dynamodb.BatchWriteItemOutput{}, nil)
	if err := a.PrepareTokens(ctx, "unknown"); err != nil {
		t.Fatal(err)
	}
	if a.ncache.Contains("unknown") {
		t.Error("PrepareTokens should remove the id from the negative cache")
	}
	if !a.rcache.Contains("unknown") {
		t.Error("PrepareTokens should add the id to the registration cache")
	}

	a.ncache.AddUntil("expiring", timeNow().Add(defaultNegativeCacheTTL))
	fixTime(t, defaultNegativeCacheTTL)
	if a.ncache.Contains("expiring") {
		t.Error("entry should expire")
	}
}

func TestRateLimit_PrepareTokens(t *testing.T) {
	t.Run("puts a full bucket per shard and the registration item last", func(t *testing.T) {
		c := NewMockDDBClient(gomock.NewController(t))
		c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
				reqs := in.RequestItems["dummy_table"]
				if len(reqs) != 2 {
					t.Fatalf("want 1 shard and the registration item, got %d", len(reqs))
				}
				item := new(ddbItem)
				if err := attributevalue.UnmarshalMap(reqs[0].PutRequest.Item, item); err != nil {
					t.Fatal(err)
				}
				if item.BucketIDShardID != "key#0" || item.TAT != 0 {
					t.Errorf("unexpected item %+v", item)
				}
				reg := new(registrationItem)
				if err := attributevalue.UnmarshalMap(reqs[1].PutRequest.Item, reg); err != nil {
					t.Fatal(err)
				}
				if reg.BucketIDShardID != "key#meta" || reg.RegisteredAt == 0 {
					t.Errorf("unexpected registration item %+v", reg)
				}
				return &dynamodb.BatchWriteItemOutput{}, nil
			})
		rl, err := New(testConfig(), c)
		if err != nil {
			t.Fatal(err)
		}
		if err := rl.PrepareTokens(context.Background(), "key"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("throttling does not panic and records a metric", func(t *testing.T) {
		fixTime(t, 0)
		c := NewMockDDBClient(gomock.NewController(t))
		c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")})
		reader, opt := newTestMeter()
		rl, err := New(testConfig(), c, opt)
		if err != nil {
			t.Fatal(err)
		}
		if err := rl.PrepareTokens(context.Background(), "key"); !errors.Is(err, ErrStoreThrottled) {
			t.Errorf("want %v, got %v", ErrStoreThrottled, err)
		}
		got := collectCounters(t, reader)["limiter.dynamodb.throttles"]
		if got["aws.dynamodb.operation=BatchWriteItem,limiter.table=dummy_table"] != 1 {
			t.Errorf("throttle metric should be recorded: %v", got)
		}
	})

	t.Run("retry stops on context cancellation", func(t *testing.T) {
		c := NewMockDDBClient(gomock.NewController(t))
		ctx, cancel := context.WithCancel(context.Background())
		c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
				cancel()
				return &dynamodb.BatchWriteItemOutput{UnprocessedItems: in.RequestItems}, nil
			})
		rl, err := New(testConfig(), c)
		if err != nil {
			t.Fatal(err)
		}
		if err := rl.PrepareTokens(ctx, "key"); !errors.Is(err, context.Canceled) {
			t.Errorf("want %v, got %v", context.Canceled, err)
		}
	})

	t.Run("invalid bucket id", func(t *testing.T) {
		rl, err := New(testConfig(), NewMockDDBClient(gomock.NewController(t)))
		if err != nil {
			t.Fatal(err)
		}
		if err := rl.PrepareTokens(context.Background(), ""); !errors.Is(err, ErrInvalidBucketID) {
			t.Errorf("want %v, got %v", ErrInvalidBucketID, err)
		}
	})
}

// Errors must be a single line, keep sentinel errors, and never contain the raw bucket id.
func TestRateLimit_errorsAndLogsAreRedacted(t *testing.T) {
	const secret = "sk-secret-api-key" //nolint:gosec // fake key to check redaction
	for _, upstream := range []error{
		&types.InternalServerError{Message: aws.String("boom")},
		&types.ProvisionedThroughputExceededException{Message: aws.String("slow down")},
	} {
		t.Run(fmt.Sprintf("%T", upstream), func(t *testing.T) {
			fixTime(t, 0)
			c := NewMockDDBClient(gomock.NewController(t))
			registered(t, c)
			c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, upstream)
			logs := &bytes.Buffer{}
			reader, meterOpt := newTestMeter()
			rl, err := New(testConfig(), c, meterOpt,
				WithLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
			if err != nil {
				t.Fatal(err)
			}
			_, err = rl.Allow(context.Background(), secret)
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), "\n") {
				t.Errorf("error must be a single line: %q", err.Error())
			}
			metrics := fmt.Sprint(collectCounters(t, reader))
			for name, s := range map[string]string{"error": err.Error(), "metrics": metrics, "logs": logs.String()} {
				if strings.Contains(s, secret) {
					t.Errorf("%s must not contain the raw bucket id: %s", name, s)
				}
			}
			if !strings.Contains(err.Error(), redactID(secret)) {
				t.Errorf("error should contain the redacted id: %s", err.Error())
			}
		})
	}
}

// newTestMeter returns an option recording metrics into an in-memory reader.
func newTestMeter() (*sdkmetric.ManualReader, Opt) {
	reader := sdkmetric.NewManualReader()
	return reader, WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
}

// collectCounters returns counter values keyed by metric name and encoded attributes ("k=v,k=v").
func collectCounters(t *testing.T, reader *sdkmetric.ManualReader) map[string]map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	ret := map[string]map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			ret[m.Name] = map[string]int64{}
			for _, dp := range sum.DataPoints {
				ret[m.Name][dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Value
			}
		}
	}
	return ret
}

// The library logs only at debug level. Errors are returned to the caller and must not be logged twice.
func TestRateLimit_logsOnlyAtDebug(t *testing.T) {
	now := fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	gomock.InOrder(
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, &types.InternalServerError{Message: aws.String("boom")}),
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, &ddbItem{TAT: now + emission})).Times(maxAttempts),
	)
	logs := &bytes.Buffer{}
	rl, err := New(testConfig(), c, WithLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := rl.Allow(ctx, "error"); !errors.Is(err, ErrInternal) {
		t.Fatalf("want %v, got %v", ErrInternal, err)
	}
	if d, err := rl.Allow(ctx, "contention"); err != nil || d.Allowed {
		t.Fatalf("want denied by contention: %+v %v", d, err)
	}
	if logs.Len() != 0 {
		t.Errorf("no logs above debug level are expected: %s", logs.String())
	}
}

func TestRateLimit_metrics_contention(t *testing.T) {
	now := fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, &ddbItem{TAT: now + emission})).Times(maxAttempts)
	reader, opt := newTestMeter()
	rl, err := New(testConfig(), c, opt)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := rl.Allow(context.Background(), "k"); err != nil || d.Allowed || d.RetryAfter != 2*time.Second {
		t.Fatalf("want denied for an emission interval: %+v %v", d, err)
	}
	got := collectCounters(t, reader)["limiter.decisions"]
	if got["limiter.result=contention,limiter.source=dynamodb,limiter.table=dummy_table"] != 1 {
		t.Errorf("contention should be counted separately from the rate limit: %v", got)
	}
}

func TestRateLimit_metrics(t *testing.T) {
	now := fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	registered(t, c)
	gomock.InOrder(
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil),
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, &ddbItem{TAT: now + tolerance + emission})),
		c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
			Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")}),
	)
	reader, opt := newTestMeter()
	rl, err := New(testConfig(), c, opt)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = rl.Allow(ctx, "a") // allowed
	_, _ = rl.Allow(ctx, "b") // denied by dynamodb
	_, _ = rl.Allow(ctx, "b") // denied by the local cache
	_, _ = rl.Allow(ctx, "c") // dynamodb throttled
	_, _ = rl.Allow(ctx, "")  // invalid

	got := collectCounters(t, reader)
	want := map[string]map[string]int64{
		"limiter.decisions": {
			"limiter.result=allowed,limiter.source=dynamodb,limiter.table=dummy_table": 1,
			"limiter.result=denied,limiter.source=dynamodb,limiter.table=dummy_table":  1,
			"limiter.result=denied,limiter.source=cache,limiter.table=dummy_table":     1,
			"limiter.result=error,limiter.source=dynamodb,limiter.table=dummy_table":   1,
			"limiter.result=invalid,limiter.source=cache,limiter.table=dummy_table":    1,
		},
		"limiter.dynamodb.throttles": {
			"aws.dynamodb.operation=UpdateItem,limiter.table=dummy_table": 1,
		},
	}
	for name, points := range want {
		for attrs, v := range points {
			if got[name][attrs] != v {
				t.Errorf("%s{%s}: want %d, got %d (all: %v)", name, attrs, v, got[name][attrs], got[name])
			}
		}
		if len(got[name]) != len(points) {
			t.Errorf("%s: unexpected data points %v", name, got[name])
		}
	}
}

// ---- DynamoDB Local (make start-local) ----

func testClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "123456")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "123456") //nolint:gosec // dummy credentials for DynamoDB Local
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000")
	})
}

func testRateLimit(t *testing.T, cfg *Config) *RateLimit {
	t.Helper()
	client := testClient(t)
	cfg.TableName = "test_gcra_table"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := CreateTable(ctx, cfg.TableName, client); err != nil {
		// never skip silently on CI
		if os.Getenv("CI") != "" {
			t.Fatalf("DynamoDB Local is not available: %v", err)
		}
		t.Skipf("DynamoDB Local is not available (run make start-local): %v", err)
	}
	l, err := New(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	// deny cache hides DynamoDB behavior in these tests
	l.dcache = nil
	return l
}

func uniqueID(prefix string) string {
	return prefix + "-" + uuid.NewString()
}

func TestRateLimit_WithDynamoDBLocal(t *testing.T) {
	ctx := context.Background()

	t.Run("burst, deny, then refill", func(t *testing.T) {
		l := testRateLimit(t, testConfig())
		id := uniqueID("burst")
		fixTime(t, 0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		for i, want := range []int64{3, 2, 1, 0} {
			d, err := l.Allow(ctx, id)
			if err != nil || !d.Allowed || d.Remaining != want {
				t.Fatalf("[%d] want allowed with %d remaining: %+v %v", i, want, d, err)
			}
		}
		d, err := l.Allow(ctx, id)
		if err != nil || d.Allowed || d.RetryAfter != 2*time.Second {
			t.Fatalf("want denied for 2s: %+v %v", d, err)
		}
		fixTime(t, 2*time.Second)
		if d, err := l.Allow(ctx, id); err != nil || !d.Allowed {
			t.Fatalf("want allowed after 2s: %+v %v", d, err)
		}
	})

	t.Run("steady traffic keeps being allowed", func(t *testing.T) {
		l := testRateLimit(t, testConfig())
		id := uniqueID("steady")
		fixTime(t, 0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			fixTime(t, time.Duration(i)*2*time.Second)
			if d, err := l.Allow(ctx, id); err != nil || !d.Allowed {
				t.Fatalf("[%d] want allowed: %+v %v", i, d, err)
			}
		}
	})

	t.Run("unregistered id", func(t *testing.T) {
		l := testRateLimit(t, testConfig())
		if _, err := l.Allow(ctx, uniqueID("unknown")); !errors.Is(err, ErrInvalidBucketID) {
			t.Fatalf("want %v, got %v", ErrInvalidBucketID, err)
		}
	})

	t.Run("anonymous creates an item with ttl", func(t *testing.T) {
		cfg := testConfig()
		cfg.AdditionalConfig = &AdditionalConfig{Anonymous: &AnonymousConfig{RecordTTL: time.Hour}}
		l := testRateLimit(t, cfg)
		id := uniqueID("anon")
		now := fixTime(t, 0)
		if d, err := l.Allow(ctx, id); err != nil || !d.Allowed {
			t.Fatalf("want allowed: %+v %v", d, err)
		}
		out, err := l.client.c.(*dynamodb.Client).GetItem(ctx, &dynamodb.GetItemInput{
			TableName: &l.tableName, Key: buildPartitionKey(id, 0), ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			t.Fatal(err)
		}
		item := new(ddbItem)
		if err := attributevalue.UnmarshalMap(out.Item, item); err != nil {
			t.Fatal(err)
		}
		// record_ttl (1h) is shorter than the latest TAT + margin (6s + 2s + 1h)
		if want := time.Unix(0, now).Add(time.Hour + 8*time.Second).Unix(); item.TTL != want {
			t.Errorf("ttl: want %d, got %d", want, item.TTL)
		}
	})

	t.Run("changing the number of shards keeps registered ids valid", func(t *testing.T) {
		before := testRateLimit(t, testConfig())
		id := uniqueID("reshard")
		fixTime(t, 0)
		if err := before.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		cfg := testConfig()
		cfg.Shards = 2
		after := testRateLimit(t, cfg)
		for i := 0; i < 4; i++ {
			if d, err := after.Allow(ctx, id); err != nil {
				t.Fatalf("[%d] registered id must stay valid: %+v %v", i, d, err)
			}
		}
	})

	t.Run("RevokeTokens revokes the id", func(t *testing.T) {
		l := testRateLimit(t, testConfig())
		id := uniqueID("revoke")
		fixTime(t, 0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		if d, err := l.Allow(ctx, id); err != nil || !d.Allowed {
			t.Fatalf("want allowed: %+v %v", d, err)
		}
		if err := l.RevokeTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		// another instance (no registration cache entry)
		other := testRateLimit(t, testConfig())
		if _, err := other.Allow(ctx, id); !errors.Is(err, ErrInvalidBucketID) {
			t.Fatalf("want %v, got %v", ErrInvalidBucketID, err)
		}
	})

	t.Run("concurrent requests never exceed the burst", func(t *testing.T) {
		l := testRateLimit(t, testConfig())
		id := uniqueID("concurrent")
		fixTime(t, 0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if d, err := l.Allow(ctx, id); err == nil && d.Allowed {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		if n := allowed.Load(); n > 4 || n == 0 {
			t.Errorf("allowed %d requests, want 1..4", n)
		}
	})
}
