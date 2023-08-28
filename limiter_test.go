package limiter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	mock "github.com/konoui/limiter/mock_limiter"
)

var logger = slog.New(slog.NewJSONHandler(io.Discard, nil))

func testClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "123456")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "123456")
	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
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

func testRateLimit(t *testing.T, cfg *Config, opts ...Opt) *RateLimit {
	t.Helper()
	client := testClient(t)
	cfg.TableName = "test_buckets_table"
	l, err := New(
		cfg,
		client,
		opts...,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := CreateTable(context.Background(), cfg.TableName, client); err != nil {
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
	cfg := Config{
		TokenPerInterval: 2,
		BucketSize:       2 * 2,
		Interval:         3 * time.Second,
		TableName:        "dummy_table",
		AdditionalConfig: &AdditionalConfig{
			NegativeCache: &NegativeCacheConfig{
				Size: 0,
			},
		},
	}
	tests := []struct {
		name       string
		mocker     func(client *mock.MockDDBClient)
		throttle   bool
		err        error
		metricFile string
		update     bool
	}{
		{
			name: "throttle when time is not passed and existing token is zero",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  0,
					LastUpdated: timeNow().UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
			},
			throttle:   true,
			metricFile: "token-run-out.json",
		},
		{
			name: "throttle when bucket id is invalid",
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
			name: "not throttle when token is refilled due to passed time",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  0,
					LastUpdated: timeNow().Add(-cfg.Interval).UnixMilli(),
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
			name: "throttle when getItem returns API rate limit exceeded",
			mocker: func(client *mock.MockDDBClient) {
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.LimitExceededException{Message: aws.String("my limit exceeded error")})
			},
			throttle:   true,
			err:        ErrRateLimitExceeded,
			metricFile: "get-item-rate-limit-exceeded.json",
		},
		{
			name: "throttle when updateItem of refillToken returns DDB API rate limit exceeded",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  1,
					LastUpdated: timeNow().UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.LimitExceededException{Message: aws.String("CCF error")})
			},
			throttle:   true,
			err:        ErrRateLimitExceeded,
			metricFile: "update-item-rate-limit-exceeded.json",
		},
		{
			name: "not throttle if updateItem of refillToken returns CCF and fallback updateItem of subtractToken succeeded then one token is available at least",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  0,
					LastUpdated: timeNow().Add(-cfg.Interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: aws.String("CCF error")})
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, nil)
			},
			throttle: false,
			err:      nil,
		},
		{
			name: "not throttle if updateItem of refillToken returns CCF and fallback updateItem of subtractToken failed then returns zero token",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  1,
					LastUpdated: timeNow().Add(-cfg.Interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: aws.String("CCF error")})
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.InternalServerError{Message: aws.String("my internal server error")})
			},
			metricFile: "fallback-subtract-token-failed-due-to-internal-error.json",
			throttle:   true,
			err:        ErrInternal,
		},
		{
			name: "throttle if updateItem of refillToken returns an internal error",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  2,
					LastUpdated: timeNow().Add(-cfg.Interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.InternalServerError{Message: aws.String("my internal server error")})
			},
			throttle:   true,
			metricFile: "refill-token-with-fail-opt-internal-server-error.json",
			err:        ErrInternal,
		},
		{
			name: "throttle if updateItem of refillToken returns CCF and fallback updateItem of subtractToken returns CCF then toke run out",
			mocker: func(client *mock.MockDDBClient) {
				msg := aws.String("CCF")
				item := &ddbItem{
					TokenCount:  1,
					LastUpdated: timeNow().Add(-cfg.Interval).UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: msg})
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: msg})
			},
			throttle:   true,
			metricFile: "refill-token-fallback-to-subtract-token-return-unexpected-error.json",
			err:        nil,
		},
		{
			name: "throttle if updateItem of subtractToken returns CCF then token run out",
			mocker: func(client *mock.MockDDBClient) {
				item := &ddbItem{
					TokenCount:  1,
					LastUpdated: timeNow().UnixMilli(),
				}
				client.EXPECT().
					GetItem(gomock.Any(), gomock.Any()).
					Return(&dynamodb.GetItemOutput{Item: newAttr(t, item)}, nil)
				client.EXPECT().
					UpdateItem(gomock.Any(), gomock.Any()).
					Return(nil, &types.ConditionalCheckFailedException{Message: aws.String("CCF error")})
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

			out := &bytes.Buffer{}
			rl, err := New(&cfg, client, WithEMFMetrics(out))
			if err != nil {
				t.Fatal(err)
			}
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
				if tt.update {
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

func TestRateLimit_ShouldThrottleWithNegativeCache(t *testing.T) {
	cfg := Config{
		TokenPerInterval: 2,
		BucketSize:       2 * 2,
		Interval:         3 * time.Second,
		TableName:        "dummy_table",
		AdditionalConfig: &AdditionalConfig{
			NegativeCache: &NegativeCacheConfig{
				Size: 2,
			},
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mock.NewMockDDBClient(ctrl)
	client.EXPECT().
		GetItem(gomock.Any(), gomock.Any()).
		Return(&dynamodb.GetItemOutput{}, nil)

	l, err := New(&cfg, client)
	if err != nil {
		t.Fatal(err)
	}

	throttle, err := l.ShouldThrottle(context.Background(), "invalid-bucket-id")
	if !errors.Is(err, ErrInvalidBucketID) {
		t.Errorf("want: %v, got: %v", ErrInvalidBucketID, err)
	}
	if !throttle {
		t.Errorf("should throttle")
	}

	throttle, err = l.ShouldThrottle(context.Background(), "invalid-bucket-id")
	if !(errors.Is(err, ErrInvalidBucketID) && strings.Contains(err.Error(), "negative cache") && throttle) {
		t.Errorf("want: %v, got: %v throttle result: %v", ErrInvalidBucketID, err, throttle)
	}
}

func TestRateLimit_ShouldThrottleWithDynamoDBLocal(t *testing.T) {
	t.Run("anonymous", func(t *testing.T) {
		t.Cleanup(func() { timeNow = time.Now })

		timeNow = myNow(0)
		ttl := 24 * time.Hour
		wantTTL := timeNow().Unix() + int64(ttl.Seconds())

		ctx := context.Background()
		cfg := &Config{
			TokenPerInterval: 2,
			BucketSize:       2 * 2,
			Interval:         3 * time.Second,
			AdditionalConfig: &AdditionalConfig{
				Anonymous: &AnonymousConfig{
					RecordTTL: ttl,
				},
			},
		}

		l := testRateLimit(t, cfg)
		id := int64String(int64(pickIndex(100000)))

		t.Logf("bucket-id %s\n", id)
		throttle, err := l.ShouldThrottle(ctx, id)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if throttle {
			t.Errorf("unexpected throttle: %v", throttle)
			return
		}

		item, _ := l.getTokenItem(ctx, slog.Default(), id, 0)
		if item.TTL != wantTTL {
			t.Errorf("unexpected ttl: want: %d, got: %d", wantTTL, item.TTL)
		}
	})
	t.Run("throttle", func(t *testing.T) {
		t.Cleanup(func() { timeNow = time.Now })

		ctx := context.Background()
		cfg := &Config{
			TokenPerInterval: 2,
			BucketSize:       2 * 2,
			Interval:         3 * time.Second,
		}
		l := testRateLimit(t, cfg)

		id := int64String(int64(pickIndex(100000)))

		// base time
		timeNow = myNow(0)
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}

		t.Logf("bucket-id %s\n", id)
		for i := 0; i < int(cfg.BucketSize); i++ {
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
			item, _ := l.getTokenItem(context.Background(), logger, id, 0)
			t.Errorf("should non throttle: %v", item.TokenCount)
		}

		// wait interval and refill base value
		// 0 + base
		timeNow = myNow(cfg.Interval)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err := l.getToken(ctx, logger, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := cfg.TokenPerInterval; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill base value
		// base -1 + base
		timeNow = myNow(cfg.Interval * 2)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err = l.getToken(ctx, logger, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := cfg.BucketSize - 1; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill
		timeNow = myNow(cfg.Interval * 3)
		t.Logf("getToken %v\n", timeNow().UnixMilli())
		token, err = l.getToken(ctx, logger, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := cfg.BucketSize; token != want {
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
			bucket, err := newTokenBucket(tt.base, tt.base*2, tt.interval)
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
				LastUpdated: now.Add(-tt.wait).UnixMilli(),
				TokenCount:  tt.cur,
				shardID:     0,
			}, now.UnixMilli())

			if got != tt.want {
				t.Errorf("RateLimit.calculateRefillToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func readTestFile(t *testing.T, filename string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeTestFile(t *testing.T, data, filename string) {
	t.Helper()
	err := os.WriteFile(filepath.Join("testdata", filename), []byte(data), 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_splitPartitionKey(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantBucketID string
		wantShardID  string
		wantErr      bool
	}{
		{
			name:         "input",
			input:        fmt.Sprintf("test%s1", delimiter),
			wantBucketID: "test",
			wantShardID:  "1",
		},
		{
			name:         "check last index",
			input:        fmt.Sprintf("test%sa%s1", delimiter, delimiter),
			wantBucketID: "test#a",
			wantShardID:  "1",
		},
		{
			name:    "invalid input",
			input:   fmt.Sprintf("test%s", delimiter),
			wantErr: true,
		},
		{
			name:    "invalid input",
			input:   "test",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucketID, gotShardID, err := splitPartitionKey(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitPartitionKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotBucketID != tt.wantBucketID {
				t.Errorf("splitPartitionKey() gotBucketID = %v, want %v", gotBucketID, tt.wantBucketID)
			}
			if gotShardID != tt.wantShardID {
				t.Errorf("splitPartitionKey() gotShardID = %v, want %v", gotShardID, tt.wantShardID)
			}
		})
	}
}
