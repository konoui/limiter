package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

func mynow(add time.Duration) func() time.Time {
	return func() time.Time {
		return time.Unix(1000000000+int64(add.Seconds()), 0)
	}
}

func TestRateLimit_ShouldThrottle(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, err := NewTokenBucket(0, 0)
		if err == nil {
			t.Fatal("non error")
		}
	})

	t.Run("throttle", func(t *testing.T) {
		t.Cleanup(func() { TimeNow = time.Now })

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
		TimeNow = mynow(0)
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
		TimeNow = mynow(interval)
		t.Logf("getToken %v\n", TimeNow().Unix())
		token, err := l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := base; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill base value
		// base -1 + base
		TimeNow = mynow(interval + interval)
		t.Logf("getToken %v\n", TimeNow().Unix())
		token, err = l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := burst - 1; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// wait interval and refill
		TimeNow = mynow(interval + interval + interval)
		t.Logf("getToken %v\n", TimeNow().Unix())
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
			now := TimeNow().Unix()
			l := &RateLimit{
				client:    nil,
				bucket:    bucket,
				tableName: "dummy",
			}
			got := l.calculateRefillToken(&ddbItem{
				LastUpdated:    now - int64(tt.wait.Seconds()),
				ShardBurstSize: tt.base * 2,
				TokenCount:     tt.cur,
				BucketShardID:  0,
			}, now)

			if got != tt.want {
				t.Errorf("RateLimit.calculateRefillToken() = %v, want %v", got, tt.want)
			}
		})
	}
}
