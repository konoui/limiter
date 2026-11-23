package limiter

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	mock "github.com/konoui/limiter/mock_limiter"
)

func regressionConfig() *Config {
	return &Config{
		TokenPerInterval: 2,
		BucketSize:       2 * 2,
		Interval:         3 * time.Second,
		TableName:        "dummy_table",
	}
}

func attrNames(in *dynamodb.UpdateItemInput) map[string]bool {
	ret := map[string]bool{}
	for _, v := range in.ExpressionAttributeNames {
		ret[v] = true
	}
	return ret
}

func attrNumbers(in *dynamodb.UpdateItemInput) map[string]bool {
	ret := map[string]bool{}
	for _, v := range in.ExpressionAttributeValues {
		if n, ok := v.(*types.AttributeValueMemberN); ok {
			ret[n.Value] = true
		}
	}
	return ret
}

// The optimistic lock condition of refill-token must reference the partition key attribute.
// Previously it referenced a non-existent "bucket_id" attribute, which made the condition always true.
// The refill must also carry the elapsed remainder over to the next refill.
func TestRegression_RefillTokenInput(t *testing.T) {
	t.Cleanup(func() { timeNow = time.Now })
	timeNow = myNow(0)
	cfg := regressionConfig()

	now := timeNow()
	// 1.5 intervals elapsed: refill one interval and carry the half interval over
	lastUpdated := now.Add(-cfg.Interval * 3 / 2)
	wantLastUpdated := lastUpdated.Add(cfg.Interval)

	ctrl := gomock.NewController(t)
	client := mock.NewMockDDBClient(ctrl)
	client.EXPECT().
		GetItem(gomock.Any(), gomock.Any()).
		Return(&dynamodb.GetItemOutput{Item: newAttr(t, &ddbItem{
			TokenCount:  0,
			LastUpdated: lastUpdated.UnixMilli(),
		})}, nil)
	client.EXPECT().
		UpdateItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			names := attrNames(in)
			if !names[attrBucketIDShardID] {
				t.Errorf("condition must reference %s: %v", attrBucketIDShardID, in.ExpressionAttributeNames)
			}
			if names["bucket_id"] {
				t.Errorf("condition must not reference bucket_id: %v", in.ExpressionAttributeNames)
			}
			if !strings.Contains(*in.ConditionExpression, "attribute_not_exists") {
				t.Errorf("unexpected condition: %s", *in.ConditionExpression)
			}

			nums := attrNumbers(in)
			if !nums[int64String(wantLastUpdated.UnixMilli())] {
				t.Errorf("last_updated should be %d: %v", wantLastUpdated.UnixMilli(), in.ExpressionAttributeValues)
			}
			if nums[int64String(now.UnixMilli())] {
				t.Errorf("last_updated must not be now: %v", in.ExpressionAttributeValues)
			}
			return &dynamodb.UpdateItemOutput{}, nil
		})

	rl, err := New(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	throttle, err := rl.ShouldThrottle(context.Background(), "dummy")
	if throttle || err != nil {
		t.Errorf("unexpected result: throttle %v, err %v", throttle, err)
	}
}

// subtract-token must not update last_updated.
// Previously every consumption reset last_updated and refilling never happened under steady traffic.
func TestRegression_SubtractTokenKeepsLastUpdated(t *testing.T) {
	t.Cleanup(func() { timeNow = time.Now })
	timeNow = myNow(0)
	cfg := regressionConfig()

	ctrl := gomock.NewController(t)
	client := mock.NewMockDDBClient(ctrl)
	client.EXPECT().
		GetItem(gomock.Any(), gomock.Any()).
		Return(&dynamodb.GetItemOutput{Item: newAttr(t, &ddbItem{
			TokenCount:  2,
			LastUpdated: timeNow().UnixMilli(),
		})}, nil)
	client.EXPECT().
		UpdateItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
			if attrNames(in)["last_updated"] {
				t.Errorf("subtract-token must not update last_updated: %s", *in.UpdateExpression)
			}
			return &dynamodb.UpdateItemOutput{}, nil
		})

	rl, err := New(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	throttle, err := rl.ShouldThrottle(context.Background(), "dummy")
	if throttle || err != nil {
		t.Errorf("unexpected result: throttle %v, err %v", throttle, err)
	}
}

// A token just created may not be visible to an eventually consistent read.
// It must be confirmed with a consistent read and must not be stored into the negative cache.
func TestRegression_ConsistentReadBeforeInvalid(t *testing.T) {
	t.Cleanup(func() { timeNow = time.Now })
	timeNow = myNow(0)
	cfg := regressionConfig()

	ctrl := gomock.NewController(t)
	client := mock.NewMockDDBClient(ctrl)
	gomock.InOrder(
		client.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			Return(&dynamodb.GetItemOutput{}, nil),
		client.EXPECT().
			GetItem(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				if !aws.ToBool(in.ConsistentRead) {
					t.Errorf("second read must be consistent")
				}
				return &dynamodb.GetItemOutput{Item: newAttr(t, &ddbItem{
					TokenCount:  2,
					LastUpdated: timeNow().UnixMilli(),
				})}, nil
			}),
	)
	client.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil)

	rl, err := New(cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	throttle, err := rl.ShouldThrottle(context.Background(), "new-token")
	if throttle || err != nil {
		t.Errorf("unexpected result: throttle %v, err %v", throttle, err)
	}
	if rl.ncache.Contains("new-token") {
		t.Errorf("valid token must not be in the negative cache")
	}
}

// Each limiter must own its negative cache, and entries must expire.
func TestRegression_NegativeCacheIsolatedAndExpires(t *testing.T) {
	t.Cleanup(func() { timeNow = time.Now })
	timeNow = myNow(0)

	ctrl := gomock.NewController(t)
	client := mock.NewMockDDBClient(ctrl)

	a, err := New(regressionConfig(), client)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(regressionConfig(), client)
	if err != nil {
		t.Fatal(err)
	}

	a.ncache.Add("invalid")
	if !a.ncache.Contains("invalid") {
		t.Fatal("should be cached")
	}
	if b.ncache.Contains("invalid") {
		t.Error("negative cache must not be shared between limiters")
	}

	timeNow = myNow(defaultNegativeCacheTTL)
	if a.ncache.Contains("invalid") {
		t.Error("entry should be expired")
	}
	if a.ncache.Len() != 0 {
		t.Errorf("expired entry should be removed: %d", a.ncache.Len())
	}
}

func TestRegression_NegativeCacheDisabled(t *testing.T) {
	cfg := regressionConfig()
	cfg.AdditionalConfig = &AdditionalConfig{NegativeCache: &NegativeCacheConfig{Size: 0}}
	rl, err := New(cfg, mock.NewMockDDBClient(gomock.NewController(t)))
	if err != nil {
		t.Fatal(err)
	}
	rl.ncache.Add("invalid")
	if rl.ncache.Contains("invalid") {
		t.Error("size 0 should disable the cache")
	}
}

// BatchWriteItem returns a nil output with a throttling error.
// The metrics hook must not dereference the output.
func TestRegression_BatchWriteItemThrottleDoesNotPanic(t *testing.T) {
	t.Cleanup(func() { timeNow = time.Now })
	t.Cleanup(func() { uuidNewRandom = uuid.NewRandom })
	timeNow = myNow(0)
	uuidNewRandom = myUUIDNewRandom(t)

	ctrl := gomock.NewController(t)
	client := mock.NewMockDDBClient(ctrl)
	client.EXPECT().
		BatchWriteItem(gomock.Any(), gomock.Any()).
		Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("throttled")})

	out := &bytes.Buffer{}
	rl, err := New(regressionConfig(), client, WithEMFMetrics(out))
	if err != nil {
		t.Fatal(err)
	}
	err = rl.PrepareTokens(context.Background(), "dummy")
	if !errors.Is(err, ErrRateLimitExceeded) {
		t.Errorf("want %v, got %v", ErrRateLimitExceeded, err)
	}
	if !strings.Contains(out.String(), `"Operation":"BatchWriteItem"`) {
		t.Errorf("throttle metric should be emitted: %s", out.String())
	}
}

// BucketID must not be a metric dimension (high cardinality) and each EMF record must end with a newline.
func TestRegression_EMFFormat(t *testing.T) {
	out := &bytes.Buffer{}
	outputLog(out, buildThrottleMetric("table", "bucket", "0"))
	outputLog(out, buildDDBThrottleMetric("table", "GetItem", "bucket", "0"))

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
	if !strings.Contains(lines[0], `"BucketID":"bucket"`) {
		t.Errorf("BucketID should be kept as a property: %s", lines[0])
	}
}

// Errors must be a single line, keep sentinel errors, and never contain the raw bucket id.
func TestRegression_ErrorMessages(t *testing.T) {
	const secret = "sk-secret-api-key"
	tests := []struct {
		name   string
		mocker func(client *mock.MockDDBClient)
		want   error
	}{
		{
			name: "internal error",
			mocker: func(client *mock.MockDDBClient) {
				client.EXPECT().GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, &ddbItem{TokenCount: 0, LastUpdated: 0})}, nil)
				client.EXPECT().UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.InternalServerError{Message: aws.String("boom")})
			},
			want: ErrInternal,
		},
		{
			name: "dynamodb throttling",
			mocker: func(client *mock.MockDDBClient) {
				client.EXPECT().GetItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ProvisionedThroughputExceededException{Message: aws.String("slow down")})
			},
			want: ErrRateLimitExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := mock.NewMockDDBClient(gomock.NewController(t))
			tt.mocker(client)
			metrics := &bytes.Buffer{}
			logs := &bytes.Buffer{}
			rl, err := New(regressionConfig(), client,
				WithEMFMetrics(metrics),
				WithLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
			if err != nil {
				t.Fatal(err)
			}
			_, err = rl.ShouldThrottle(context.Background(), secret)
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
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

// requires DynamoDB Local (make start-local)
func TestRegression_WithDynamoDBLocal(t *testing.T) {
	t.Run("steady traffic still refills tokens", func(t *testing.T) {
		t.Cleanup(func() { timeNow = time.Now })
		ctx := context.Background()
		cfg := &Config{TokenPerInterval: 2, BucketSize: 4, Interval: 3 * time.Second}
		l := testRateLimit(t, cfg)
		id := "steady-" + int64String(int64(pickIndex(1000000)))

		timeNow = myNow(0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		// consume one token before an interval elapses: 3 tokens left
		timeNow = myNow(2 * time.Second)
		if token, err := l.getToken(ctx, logger, id, 0); err != nil || token != 1 {
			t.Fatalf("token %d, err %v", token, err)
		}
		// an interval has elapsed since PrepareTokens. one token is refilled up to the bucket size.
		// previously the consumption above reset the refill clock and no token was refilled here.
		timeNow = myNow(cfg.Interval + 500*time.Millisecond)
		token, err := l.getToken(ctx, logger, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		if want := cfg.BucketSize; token != want {
			t.Errorf("want %d, got %d", want, token)
		}
	})

	t.Run("concurrent refill with the same last_updated fails", func(t *testing.T) {
		t.Cleanup(func() { timeNow = time.Now })
		ctx := context.Background()
		cfg := &Config{TokenPerInterval: 2, BucketSize: 4, Interval: 3 * time.Second}
		l := testRateLimit(t, cfg)
		id := "concurrent-" + int64String(int64(pickIndex(1000000)))

		timeNow = myNow(0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(cfg.BucketSize); i++ {
			if err := l.subtractToken(ctx, logger, id, 0, timeNow().UnixMilli()); err != nil {
				t.Fatal(err)
			}
		}

		base := myNow(0)().UnixMilli()
		next := myNow(cfg.Interval)().UnixMilli()
		if err := l.refillToken(ctx, logger, id, 0, 1, base, next, next); err != nil {
			t.Fatalf("first refill should succeed: %v", err)
		}
		err := l.refillToken(ctx, logger, id, 0, 1, base, next, next)
		if !isErrConditionalCheckFailed(err) {
			t.Fatalf("second refill with the stale last_updated must fail with CCF: %v", err)
		}
		item, err := l.getTokenItem(ctx, logger, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		if item.TokenCount != 1 {
			t.Errorf("tokens must be refilled only once: %d", item.TokenCount)
		}
	})
}
