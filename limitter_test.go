package limitter

import (
	"context"
	"sync"
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
	optFunc := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
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

func TestRateLimit_ShouldThrottle(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		ctx := context.Background()
		bucket := NewTokenBucket(0, 0)
		l := testRateLimit(t, bucket)
		id := int64String(int64(pickIndex(10000)))
		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}

		throttled, err := l.ShouldThrottle(context.Background(), id)
		if err != nil {
			t.Errorf("throttle error %v", err)
		}
		if !throttled {
			t.Errorf("unexpected non throttling")
		}
	})

	t.Run("throttle", func(t *testing.T) {
		ctx := context.Background()
		base := int64(5)
		interval := 2 * time.Second
		bucket := NewTokenBucket(base, base*2, WithInterval(interval))
		l := testRateLimit(t, bucket)

		id := int64String(int64(pickIndex(100000)))

		if err := l.PrepareTokens(ctx, id); err != nil {
			t.Fatal(err)
		}

		t.Logf("bucket-id %s\n", id)
		wg := sync.WaitGroup{}
		for i := 0; i < 10; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				throttled, err := l.ShouldThrottle(ctx, id)
				if err != nil {
					t.Errorf("[%d] throttle error %v", i, err)
				}
				if throttled {
					t.Errorf("[%d] unexpected throttling", i)
				}
			}()
		}
		wg.Wait()
		throttled, err := l.ShouldThrottle(context.Background(), id)
		if err != nil {
			t.Errorf("throttle error %v", err)
		}
		if !throttled {
			t.Errorf("unexpected non throttling")
		}

		// refil
		time.Sleep(interval)
		token, err := l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := base; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// refil to burst cap
		time.Sleep(interval)
		token, err = l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		// base -1 tokens(consumed by previous getToken) + refilled base tokens
		if want := base*2 - 1; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

		// refil
		time.Sleep(interval)
		token, err = l.getToken(ctx, id, 0)
		if err != nil {
			t.Errorf("get-token error %v", err)
		}
		if want := base * 2; token != want {
			t.Errorf("want %d but got %d", want, token)
		}

	})
}
