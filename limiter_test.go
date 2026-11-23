package limiter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	"go.uber.org/mock/gomock"
	"github.com/google/uuid"
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
	uuidNewRandom = func() (uuid.UUID, error) { return uuid.MustParse("fd8107dc-686b-4c62-95e6-070c490a002f"), nil }
	t.Cleanup(func() { timeNow = time.Now; uuidNewRandom = uuid.NewRandom })
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
		wantCalls int
	}{
		{
			name: "idle bucket is allowed with a single write",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
						if !isIdle(in) || !strings.Contains(*in.ConditionExpression, "attribute_exists") {
							t.Errorf("registered mode must require the item: %s", *in.ConditionExpression)
						}
						if strings.Contains(names(in), attrTTL) {
							t.Errorf("ttl must not be set in registered mode")
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
			name: "unregistered bucket id",
			mocker: func(t *testing.T, c *MockDDBClient, now int64) {
				c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, nil))
			},
			wantErr: ErrInvalidBucketID,
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
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
		Return(nil, ccf(t, &ddbItem{TAT: now + tolerance + emission}))

	metrics := &bytes.Buffer{}
	rl, err := New(testConfig(), c, WithEMFMetrics(metrics))
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
	if n := strings.Count(metrics.String(), "\n"); n != 2 {
		t.Errorf("a throttle metric should be emitted per denial, got %d", n)
	}

	// expired: DynamoDB is called again
	fixTime(t, 2*time.Second)
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil)
	if d, err := rl.Allow(ctx, "dummy"); err != nil || !d.Allowed {
		t.Fatalf("should be allowed after retry-after: %+v %v", d, err)
	}
}

// An unregistered id is cached per limiter with a TTL, and PrepareTokens clears it.
func TestRateLimit_Allow_negativeCache(t *testing.T) {
	fixTime(t, 0)
	c := NewMockDDBClient(gomock.NewController(t))
	c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, ccf(t, nil))

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

	a.ncache.AddUntil("expiring", timeNow().Add(defaultNegativeCacheTTL))
	fixTime(t, defaultNegativeCacheTTL)
	if a.ncache.Contains("expiring") {
		t.Error("entry should expire")
	}
}

func TestRateLimit_PrepareTokens(t *testing.T) {
	t.Run("puts a full bucket per shard", func(t *testing.T) {
		c := NewMockDDBClient(gomock.NewController(t))
		c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
				reqs := in.RequestItems["dummy_table"]
				if len(reqs) != 1 {
					t.Fatalf("want 1 shard, got %d", len(reqs))
				}
				item := new(ddbItem)
				if err := attributevalue.UnmarshalMap(reqs[0].PutRequest.Item, item); err != nil {
					t.Fatal(err)
				}
				if item.BucketIDShardID != "key#0" || item.TAT != 0 {
					t.Errorf("unexpected item %+v", item)
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

	t.Run("throttling does not panic and emits a metric", func(t *testing.T) {
		fixTime(t, 0)
		c := NewMockDDBClient(gomock.NewController(t))
		c.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any()).
			Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")})
		metrics := &bytes.Buffer{}
		rl, err := New(testConfig(), c, WithEMFMetrics(metrics))
		if err != nil {
			t.Fatal(err)
		}
		if err := rl.PrepareTokens(context.Background(), "key"); !errors.Is(err, ErrStoreThrottled) {
			t.Errorf("want %v, got %v", ErrStoreThrottled, err)
		}
		if !strings.Contains(metrics.String(), `"Operation":"BatchWriteItem"`) {
			t.Errorf("throttle metric should be emitted: %s", metrics.String())
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
	const secret = "sk-secret-api-key"
	for _, upstream := range []error{
		&types.InternalServerError{Message: aws.String("boom")},
		&types.ProvisionedThroughputExceededException{Message: aws.String("slow down")},
	} {
		t.Run(fmt.Sprintf("%T", upstream), func(t *testing.T) {
			fixTime(t, 0)
			c := NewMockDDBClient(gomock.NewController(t))
			c.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(nil, upstream)
			metrics, logs := &bytes.Buffer{}, &bytes.Buffer{}
			rl, err := New(testConfig(), c, WithEMFMetrics(metrics),
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
			for name, s := range map[string]string{"error": err.Error(), "metrics": metrics.String(), "logs": logs.String()} {
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

// BucketID must not be a metric dimension (high cardinality) and each EMF record must end with a newline.
func TestEMFFormat(t *testing.T) {
	out := &bytes.Buffer{}
	outputLog(out, buildThrottleMetric("table", "bucket", "0"))
	outputLog(out, buildDDBThrottleMetric("table", "UpdateItem", "bucket", "0"))

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), out.String())
	}
	for _, m := range []CloudWatchMetrics{throttleMetrics, ddbThrottleMetrics} {
		for _, dims := range m.Dimensions {
			for _, d := range dims {
				if d == "BucketID" {
					t.Errorf("BucketID must not be a dimension: %v", dims)
				}
			}
		}
	}
}

func Test_splitPartitionKey(t *testing.T) {
	tests := []struct {
		input              string
		wantBucket, wantID string
		wantErr            bool
	}{
		{input: "test#1", wantBucket: "test", wantID: "1"},
		{input: "test#a#1", wantBucket: "test#a", wantID: "1"},
		{input: "test#", wantErr: true},
		{input: "#1", wantErr: true},
		{input: "test", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			b, s, err := splitPartitionKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if b != tt.wantBucket || s != tt.wantID {
				t.Errorf("got %q %q", b, s)
			}
		})
	}
}

// ---- DynamoDB Local (make start-local) ----

func testClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "123456")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "123456")
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
		if want := time.Unix(0, now).Add(time.Hour).Unix(); item.TTL != want {
			t.Errorf("ttl: want %d, got %d", want, item.TTL)
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
