package limiter

import (
	"fmt"
	"math"
	"time"
)

// DefaultInterval is used when Config.Interval is zero.
const DefaultInterval = 60 * time.Second

const (
	// DynamoDB allows up to 1000 WCU per second for a partition key
	writeCapacityUnitPerPartition = 1000
	// worst case of an allowed request: the idle path fails and the busy path succeeds
	maxWritesPerRequest = 2
	// allowed requests per second a shard can take
	maxAllowedPerShardPerSecond = writeCapacityUnitPerPartition / maxWritesPerRequest
)

// defaultShards returns the number of shards so that a shard stays under the write capacity of a partition.
//
// GCRA (a token bucket) allows at most burst + rate * T/interval requests in any window T.
// For T = 1s, this is the worst number of allowed requests per second regardless of how many instances send them.
func defaultShards(rate, burst int64, interval time.Duration) int64 {
	peakPerSecond := float64(burst) + float64(rate)*float64(time.Second)/float64(interval)
	return int64(math.Ceil(peakPerSecond / maxAllowedPerShardPerSecond))
}

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
// shards is computed by defaultShards when zero.
func newGCRA(rate, burst int64, interval time.Duration, shards int64) (*gcra, error) {
	if rate <= 0 {
		return nil, errInvalidRateLimitArg
	}
	if rate > burst {
		return nil, errInvalidRateLimitBucketSize
	}
	if interval < time.Second {
		return nil, errInvalidInterval
	}
	if shards < 0 {
		return nil, errInvalidShards
	}
	// every shard needs at least one token per interval, otherwise it never refills
	if shards > rate {
		return nil, fmt.Errorf("shards (%d) must be less than or equal to token_per_interval (%d): %w", shards, rate, errInvalidShards)
	}

	numOfShards := shards
	if numOfShards == 0 {
		numOfShards = min(max(defaultShards(rate, burst, interval), 1), rate)
	}

	rates := distribute(rate, numOfShards)
	bursts := distribute(burst, numOfShards)
	params := make([]shardParams, numOfShards)
	for i := range params {
		emission := time.Duration(int64(interval) / rates[i])
		params[i] = shardParams{
			emission:  emission,
			tolerance: emission * time.Duration(bursts[i]-1),
			burst:     bursts[i],
		}
	}
	return &gcra{shards: params}, nil
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
