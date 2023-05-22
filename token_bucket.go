package limiter

import (
	"math"
	"time"
)

var DefaultInterval = 60 * time.Second

const (
	writeCapacityUnitPerPartition = 1000
	maxWriteCount                 = 2 // worst case: refillToken and fallback subtractToken
	maxRatePerSec                 = writeCapacityUnitPerPartition / maxWriteCount
)

type TokenBucket struct {
	// numOfShards presents number of shard
	numOfShards int64
	// tokensPerInterval presents rateLimit per shard
	tokensPerShardPerInterval []int64
	bucketSizePerShard        []int64
	interval                  time.Duration
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

	if interval < 1*time.Second {
		return nil, errInvalidInterval
	}

	numOfShards := int64(math.Ceil(float64(bucketSize) / maxRatePerSec))
	baseTokens := distribute(rateLimit, numOfShards)
	bucketSizePerShard := distribute(bucketSize, numOfShards)
	b := &TokenBucket{
		numOfShards:               numOfShards,
		tokensPerShardPerInterval: baseTokens,
		bucketSizePerShard:        bucketSizePerShard,
		interval:                  interval,
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
