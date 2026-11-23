package limiter

import (
	"time"
)

// DefaultInterval is used when Config.Interval is zero.
const DefaultInterval = 60 * time.Second

const (
	writeCapacityUnitPerPartition = 1000
	// worst case of an allowed request: the idle path fails and the busy path succeeds
	maxWritesPerRequest = 2
	maxRatePerShard     = writeCapacityUnitPerPartition / maxWritesPerRequest
)

// shardParams holds GCRA (Generic Cell Rate Algorithm) parameters of a shard.
//
// GCRA keeps only a theoretical arrival time (TAT) per key.
// A request at `now` is allowed when TAT <= now + tolerance, and then TAT becomes max(TAT, now) + emission.
// This is equivalent to a token bucket that refills one token every `emission` up to `burst` tokens.
type shardParams struct {
	// emission is the time to refill one token (interval / tokens per interval)
	emission time.Duration
	// tolerance is how far TAT may be ahead of now: (burst - 1) * emission
	tolerance time.Duration
	burst     int64
}

type gcra struct {
	shards []shardParams
}

// newGCRA returns GCRA parameters.
// rate is the number of tokens refilled per interval and burst is the maximum number of tokens.
// The bucket is split into shards so that a single DynamoDB partition does not exceed its write capacity.
func newGCRA(rate, burst int64, interval time.Duration) (*gcra, error) {
	if rate <= 0 {
		return nil, errInvalidRateLimitArg
	}
	if rate > burst {
		return nil, errInvalidRateLimitBucketSize
	}
	if interval < time.Second {
		return nil, errInvalidInterval
	}

	numOfShards := (burst + maxRatePerShard - 1) / maxRatePerShard
	// every shard needs at least one token per interval, otherwise it never refills
	if numOfShards > rate {
		numOfShards = rate
	}

	rates := distribute(rate, numOfShards)
	bursts := distribute(burst, numOfShards)
	shards := make([]shardParams, numOfShards)
	for i := range shards {
		emission := time.Duration(int64(interval) / rates[i])
		shards[i] = shardParams{
			emission:  emission,
			tolerance: emission * time.Duration(bursts[i]-1),
			burst:     bursts[i],
		}
	}
	return &gcra{shards: shards}, nil
}

func distribute(total, n int64) []int64 {
	base := total / n
	extra := total % n
	ret := make([]int64, n)
	for i := int64(0); i < n; i++ {
		ret[i] = base
		if i < extra {
			ret[i]++
		}
	}
	return ret
}

// remaining estimates the number of requests that can be allowed immediately after TAT became tat.
func (s shardParams) remaining(now, tat int64) int64 {
	diff := now + int64(s.tolerance) - tat
	if diff < 0 {
		// Go truncates toward zero. floor(diff/emission) + 1 <= 0 for any negative diff.
		return 0
	}
	r := diff/int64(s.emission) + 1
	if r > s.burst {
		return s.burst
	}
	return r
}

// retryAfter returns how long to wait until a request is allowed.
func (s shardParams) retryAfter(now, tat int64) time.Duration {
	d := time.Duration(tat - int64(s.tolerance) - now)
	if d < 0 {
		return 0
	}
	return d
}
