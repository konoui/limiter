package limiter

import (
	"math"
	"time"
)

const DefaultInterval = 60

type TokenBucket struct {
	// numOfShards presents number of shard
	numOfShards int64
	// baseTokens presents rateLimit per shard
	baseTokens []int64
	// burstTokens presents bucketSize per shard
	burstTokens []int64
	config      *tokenBucketConfig
}

type tokenBucketConfig struct {
	interval time.Duration
}

type Option func(t *tokenBucketConfig)

func WithInterval(interval time.Duration) Option {
	return func(c *tokenBucketConfig) {
		c.interval = interval
	}
}

// NewTokenBucket return a token bucket instance.
// rateLimit is a variable that tokens will be added per `interval`.
// bucketSize is a variable that maximum number of tokens to store bucket.
// `interval` is 60 second by default.
func NewTokenBucket(rateLimit, bucketSize int64, opts ...Option) (*TokenBucket, error) {
	config := &tokenBucketConfig{
		interval: DefaultInterval * time.Second,
	}
	for _, opt := range opts {
		opt(config)
	}

	if rateLimit <= 0 {
		return nil, errInvalidRateLimitArg
	}

	if rateLimit*2 > bucketSize {
		return nil, errInvalidRateLimitBucketSize
	}

	maxRate := 500 * config.interval.Seconds()
	numOfShards := int64(math.Ceil(float64(bucketSize) / maxRate))
	baseTokens := distribute(rateLimit, numOfShards)
	burstTokens := distribute(bucketSize, numOfShards)
	b := &TokenBucket{
		numOfShards: numOfShards,
		baseTokens:  baseTokens,
		burstTokens: burstTokens,
		config:      config,
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
