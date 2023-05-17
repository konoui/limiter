package limiter

import (
	"errors"
	"testing"
	"time"
)

func Test_NewBucket(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, err := newTokenBucket(0, 0, 1*time.Second)
		if !errors.Is(err, errInvalidRateLimitArg) {
			t.Fatal("non error")
		}
	})
	t.Run("invalid arg", func(t *testing.T) {
		_, err := newTokenBucket(2, 1, 1*time.Second)
		if !errors.Is(err, errInvalidRateLimitBucketSize) {
			t.Fatal("non error")
		}
	})
	t.Run("invalid interval", func(t *testing.T) {
		_, err := newTokenBucket(2, 4, 1*time.Millisecond)
		if !errors.Is(err, errInvalidInterval) {
			t.Fatal("non error")
		}
	})

	t.Run("distribute", func(t *testing.T) {
		b, err := newTokenBucket(611, 1300, 1*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cond1 := b.numOfShards == 3
		cond2 := len(b.bucketSizePerShard) == 3 && b.bucketSizePerShard[0] == 434 && b.bucketSizePerShard[1] == 433 && b.bucketSizePerShard[2] == 433
		cond3 := len(b.tokensPerShardPerInterval) == 3 && b.tokensPerShardPerInterval[0] == 204 && b.tokensPerShardPerInterval[1] == 204 && b.tokensPerShardPerInterval[2] == 203
		if !(cond1 && cond2 && cond3) {
			t.Errorf("num: %v, bucketSizePerShard: %v, TokenPerShardPerInterval %v", b.numOfShards, b.bucketSizePerShard, b.tokensPerShardPerInterval)
		}
	})
}
