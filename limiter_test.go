package limiter

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	mock "github.com/konoui/limiter/mock_limiter"
)

func testClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	ep := aws.Endpoint{
		PartitionID:       "aws",
		URL:               "http://localhost:8000",
		HostnameImmutable: true,
	}
	optFunc := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == dynamodb.ServiceID {
				return ep, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
	opt := config.WithEndpointResolverWithOptions(optFunc)
	cfg, err := config.LoadDefaultConfig(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	return client
}

func testRateLimit(t *testing.T, b *TokenBucket) *RateLimit {
	t.Helper()
	client := testClient(t)
	tableName := "test_buckets_table"
	l := New(tableName,
		b,
		client,
	)
	if err := CreateTable(context.Background(), tableName, client); err != nil {
		t.Fatal(err)
	}
	return l
}

func myUUIDNewRandom(t *testing.T) func() (uuid.UUID, error) {
	return func() (uuid.UUID, error) {
		id, err := uuid.Parse("fd8107dc-686b-4c62-95e6-070c490a002f")
		if err != nil {
			t.Fatal(err)
		}
		return id, nil
	}
}

func myNow(add time.Duration) func() time.Time {
	return func() time.Time {
		return time.UnixMilli(1000000000 + add.Milliseconds())
	}
}

func newAttr(t *testing.T, ddbItem *ddbItem) map[string]types.AttributeValue {
	attr, err := attributevalue.MarshalMap(ddbItem)
	if err != nil {
		t.Fatal(err)
	}
	return attr
}

func TestRateLimit_ShouldThrottleMock(t *testing.T) {
	base := int64(2)
	burst := base * 2
	interval := 3 * time.Second
	errUnexpected := errors.New("unexpected error")

	tests := []struct {
		name       string
		mocker     func(client *mock.MockDDBClient)
		throttle   bool
		err        error
		metricFile string
	}{
		{
			name: "throttle when time is not passed and existing token is zero",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:     0,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
			},
			throttle:   true,
			metricFile: "token-run-out.json",
		},
		{
			name: "invalid bucket id",
			mocker: func(client *mock.MockDDBClient) {
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					// empty item
					Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{}}, nil)
			},
			throttle:   true,
			metricFile: "empty-result.json",
			err:        ErrInvalidBucketID,
		},
		{
			name: "not throttle when time is passed",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:     0,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().Add(-interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, nil)
			},
			throttle: false,
		},
		{
			name: "getItem return rate limit exceeded",
			mocker: func(client *mock.MockDDBClient) {
				msg := "my error"
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.LimitExceededException{Message: &msg})
			},
			throttle:   true,
			err:        ErrRateLimitExceeded,
			metricFile: "get-item-rate-limit-exceeded.json",
		},
		{
			name: "if updateItem return rate limit exceeded then return current token",
			mocker: func(client *mock.MockDDBClient) {
				msg := "my error"
				item := &ddbItem{
					TokenCount:     0,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().Add(-interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.LimitExceededException{Message: &msg})
			},
			throttle:   true,
			err:        ErrRateLimitExceeded,
			metricFile: "update-item-rate-limit-exceeded.json",
		},
		{
			name: "1. updateItem of refillToken return ConditionalCheckFailed then fallback to updateItem of subtractToken and return 1",
			mocker: func(client *mock.MockDDBClient) {
				msg := "my error"
				item := &ddbItem{
					TokenCount:     0,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().Add(-interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: &msg})
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, nil)
			},
			throttle: false,
			err:      nil,
		},
		{
			name: "2. updateItem of refillToken return ConditionalCheckFailed then fallback to updateItem of subtractToken and return current token",
			mocker: func(client *mock.MockDDBClient) {
				msg := "my error"
				item := &ddbItem{
					TokenCount:     1,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().Add(-interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: &msg})
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, errUnexpected)
			},
			throttle: false,
			err:      errUnexpected,
		},
		{
			name: "2. updateItem of subtractToken return ConditionalCheckFailed then token run out",
			mocker: func(client *mock.MockDDBClient) {
				msg := "my error"
				item := &ddbItem{
					TokenCount:     1,
					ShardBurstSize: burst,
					LastUpdated:    timeNow().UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: &msg})
			},
			throttle:   true,
			metricFile: "subtract-token-conditional-check-failed.json",
			err:        nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(func() { timeNow = time.Now })
			t.Cleanup(func() { uuidNewRandom = uuid.NewRandom })
			timeNow = myNow(0)
			uuidNewRandom = myUUIDNewRandom(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := mock.NewMockDDBClient(ctrl)
			tt.mocker(client)

			bucket, err := NewTokenBucket(base, burst, WithInterval(interval))
			if err != nil {
				t.Fatal(err)
			}

			out := &bytes.Buffer{}
			rl := New("dummy_table", bucket, client, WithEMFMetrics(out))
			throttle, err := rl.ShouldThrottle(context.Background(), "dummy")
			if throttle != tt.throttle {
				t.Errorf("want: %v, got: %v", tt.throttle, throttle)
			}
			if !errors.Is(err, tt.err) {
				t.Errorf("want: %v, got: %v", err, tt.err)
			}

			// golden test
			if tt.throttle || errors.Is(err, ErrRateLimitExceeded) {
				gotJSON := out.String()
				if false {
					writeTestFile(t, gotJSON, tt.metricFile)
				}
				wantJSON := readTestFile(t, tt.metricFile)
				if gotJSON != wantJSON {
					t.Errorf("want: %s\ngot: %s", wantJSON, gotJSON)
				}
			}
		})
	}
}

func TestRateLimit_ShouldThrottleWithDynamoDBLocal(t *testing.T) {
	if v := os.Getenv("SKIP_DOCKER_TEST"); v != "" {
		t.Log("skip dynamo db local test")
		t.Skip()
	}
	t.Run("throttle", func(t *testing.T) {
		t.Cleanup(func() { timeNow = time.Now })

		ctx := context.Background()
		base := int64(2)
		burst := base * 2
		interval := 3 * time.Second
		bucket, err := NewTokenBucket(base, burst, WithInterval(interval))
		if err != nil {
			t.Fatal(err)
		}

		l := testRateLimit(t, bucket)

		id := int64String(int64(pickIndex(100000)))

		// base time
		timeNow = myNow(0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}

		t.Logf("bucket-id %s\n", id)
		for i := 0; i < int(burst); i++ {
			throttled, err := l.ShouldThrottle(ctx, id)
			if err != nil {
				t.Errorf("[%d] unexpected throttle error %v", i, err)
			}
			if throttled {
				t.Errorf("[%d] unexpected throttle", i)
			}
		}

		throttled, err := l.ShouldThrottle(context.Background(), id)
		if err != nil {
			t.Errorf("unexpected throttle error %v", err)
		}
		if !throttled {
			item, _ := l.getItem(context.Background(), id, 0)
			t.Errorf("should non throttle: %v", item.TokenCount)
		}

		// wait interval and refill base value
		// 0 + base
		timeNow = myNow(interval)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err := l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := base; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill base value
		// base -1 + base
		timeNow = myNow(interval + interval)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err = l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := burst - 1; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill
		timeNow = myNow(interval + interval + interval)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err = l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := burst; token != want {
			t.Errorf("want %d but got %d", want, token)
		}
	})
}

func TestRateLimit_calculateRefillToken(t *testing.T) {
	tests := []struct {
		name     string
		want     int64
		interval time.Duration
		base     int64
		wait     time.Duration
		cur      int64
	}{
		{
			name:     "not wait",
			interval: 2 * time.Second,
			base:     10,
			want:     0,
			wait:     0 * time.Second,
		},
		{
			name:     "waiting an interval add one base",
			interval: 2 * time.Second,
			base:     10,
			want:     10,
			wait:     2 * time.Second,
		},
		{
			name:     "waiting an interval and half add one base",
			interval: 2 * time.Second,
			base:     10,
			want:     10,
			wait:     3 * time.Second,
		},
		{
			name:     "waiting some intervals add to bucket size",
			interval: 2 * time.Second,
			base:     10,
			want:     20,
			wait:     40 * time.Second,
		},
		{
			name:     "current 1 and waiting interval add one base",
			interval: 2 * time.Second,
			base:     10,
			cur:      1,
			want:     10,
			wait:     2 * time.Second,
		},
		{
			name:     "current 1 and waiting some interval add bucket size -1",
			interval: 2 * time.Second,
			base:     10,
			cur:      1,
			want:     19,
			wait:     40 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := NewTokenBucket(tt.base, tt.base*2, WithInterval(tt.interval))
			if err != nil {
				t.Fatal(err)
			}
			now := timeNow()
			l := &RateLimit{
				client:    nil,
				bucket:    bucket,
				tableName: "dummy",
			}
			got := l.calculateRefillToken(&ddbItem{
				LastUpdated:    now.Add(-tt.wait).UnixMilli(),
				ShardBurstSize: tt.base * 2,
				TokenCount:     tt.cur,
				BucketShardID:  0,
			}, now.UnixMilli())

			if got != tt.want {
				t.Errorf("RateLimit.calculateRefillToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func readTestFile(t *testing.T, filename string) string {
	data, err := os.ReadFile(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeTestFile(t *testing.T, data, filename string) {
	err := os.WriteFile(filepath.Join("testdata", filename), []byte(data), 0600)
	if err != nil {
		t.Fatal(err)
	}
}
