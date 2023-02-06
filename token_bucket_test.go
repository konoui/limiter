package limiter

import (
	"errors"
	"testing"
	"time"
)

func Test_NewBucket(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, err := NewTokenBucket(0, 0)
		if !errors.Is(err, errInvalidRateLimitArg) {
			t.Fatal("non error")
		}
	})
	t.Run("invalid arg", func(t *testing.T) {
		_, err := NewTokenBucket(2, 1)
		if !errors.Is(err, errInvalidRateLimitBucketSize) {
			t.Fatal("non error")
		}
	})

	t.Run("distribute", func(t *testing.T) {
		b, err := NewTokenBucket(611, 1300, WithInterval(1*time.Second))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cond1 := b.numOfShards == 3
		cond2 := len(b.bucketSizePerShard) == 3 && b.bucketSizePerShard[0] == 434 && b.bucketSizePerShard[1] == 433 && b.bucketSizePerShard[2] == 433
		cond3 := len(b.tokenPerShardPerInterval) == 3 && b.tokenPerShardPerInterval[0] == 204 && b.tokenPerShardPerInterval[1] == 204 && b.tokenPerShardPerInterval[2] == 203
		if !(cond1 && cond2 && cond3) {
			t.Errorf("num: %v, bucketSizePerShard: %v, TokenPerShardPerInterval %v", b.numOfShards, b.bucketSizePerShard, b.tokenPerShardPerInterval)
		}
	})
}
