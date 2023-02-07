package limiter

import (
	"math"
	"time"
)

var DefaultInterval = 60 * time.Second

type TokenBucket struct {
	// numOfShards presents number of shard
	numOfShards int64
	// tokenPerInterval presents rateLimit per shard
	tokenPerShardPerInterval []int64
	bucketSizePerShard       []int64
	interval                 time.Duration
}

// NewTokenBucket return a token bucket instance.
// rateLimit is a variable that tokens will be added per `interval`.
// bucketSize is a variable that maximum number of tokens to store bucket.
// `interval` is 60 second by default.
func newTokenBucket(rateLimit, bucketSize int64, interval time.Duration) (*TokenBucket, error) {
	if rateLimit <= 0 {
		return nil, errInvalidRateLimitArg
	}

	if rateLimit*2 > bucketSize {
		return nil, errInvalidRateLimitBucketSize
	}

	maxRate := 500 * interval.Seconds()
	numOfShards := int64(math.Ceil(float64(bucketSize) / maxRate))
	baseTokens := distribute(rateLimit, numOfShards)
	bucketSizePerShard := distribute(bucketSize, numOfShards)
	b := &TokenBucket{
		numOfShards:              numOfShards,
		tokenPerShardPerInterval: baseTokens,
		bucketSizePerShard:       bucketSizePerShard,
		interval:                 interval,
	}
	return b, nil
}

func (b *TokenBucket) makeShards() []int64 {
	shardIDs := make([]int64, b.numOfShards)
	for i := int64(0); i < b.numOfShards; i++ {
		shardIDs[i] = i
	}
	return shardIDs
}

func distribute(token, numOfShard int64) []int64 {
	base := token / numOfShard
	extra := token % numOfShard
	shards := make([]int64, numOfShard)
	for i := int64(0); i < numOfShard; i++ {
		add := int64(0)
		if i < extra {
			add = 1
		}
		shards[i] = base + add
	}
	return shards
}
