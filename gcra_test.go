package limiter

import (
	"errors"
	"testing"
	"time"
)

func Test_newGCRA(t *testing.T) {
	tests := []struct {
		name          string
		rate, burst   int64
		interval      time.Duration
		wantErr       error
		wantShards    int
		wantEmission  time.Duration
		wantTolerance time.Duration
	}{
		{
			name: "single shard", rate: 20, burst: 40, interval: time.Minute,
			wantShards: 1, wantEmission: 3 * time.Second, wantTolerance: 39 * 3 * time.Second,
		},
		{
			name: "burst equal to rate", rate: 10, burst: 10, interval: 10 * time.Second,
			wantShards: 1, wantEmission: time.Second, wantTolerance: 9 * time.Second,
		},
		{
			name: "sharded by burst", rate: 1000, burst: 1000, interval: time.Second,
			wantShards: 2, wantEmission: 2 * time.Millisecond, wantTolerance: 499 * 2 * time.Millisecond,
		},
		{
			// previously a shard could get zero rate and never refill
			name: "shards are capped by rate", rate: 1, burst: 1000, interval: time.Second,
			wantShards: 1, wantEmission: time.Second, wantTolerance: 999 * time.Second,
		},
		{name: "zero rate", rate: 0, burst: 1, interval: time.Second, wantErr: errInvalidRateLimitArg},
		{name: "rate larger than burst", rate: 2, burst: 1, interval: time.Second, wantErr: errInvalidRateLimitBucketSize},
		{name: "short interval", rate: 1, burst: 1, interval: time.Millisecond, wantErr: errInvalidInterval},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := newGCRA(tt.rate, tt.burst, tt.interval)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want %v, got %v", tt.wantErr, err)
			}
			if err != nil {
				return
			}
			if len(g.shards) != tt.wantShards {
				t.Fatalf("shards: want %d, got %d", tt.wantShards, len(g.shards))
			}
			s := g.shards[0]
			if s.emission != tt.wantEmission || s.tolerance != tt.wantTolerance {
				t.Errorf("want emission %v tolerance %v, got %v %v", tt.wantEmission, tt.wantTolerance, s.emission, s.tolerance)
			}
		})
	}
}

// simulate GCRA without DynamoDB to check it behaves as a token bucket
func TestGCRA_behavesAsTokenBucket(t *testing.T) {
	g, err := newGCRA(2, 4, 4*time.Second) // a token every 2s, up to 4 tokens
	if err != nil {
		t.Fatal(err)
	}
	s := g.shards[0]
	var tat int64
	allow := func(now int64) (bool, int64, time.Duration) {
		if tat > now+int64(s.tolerance) {
			return false, 0, s.retryAfter(now, tat)
		}
		tat = max(tat, now) + int64(s.emission)
		return true, s.remaining(now, tat), 0
	}
	sec := int64(time.Second)
	base := 1000 * sec

	// burst of 4
	for i, wantRemaining := range []int64{3, 2, 1, 0} {
		ok, remaining, _ := allow(base)
		if !ok || remaining != wantRemaining {
			t.Fatalf("[%d] want allowed with %d remaining, got %v %d", i, wantRemaining, ok, remaining)
		}
	}
	ok, _, retry := allow(base)
	if ok || retry != 2*time.Second {
		t.Fatalf("5th request should be denied for 2s, got %v %v", ok, retry)
	}
	// steady traffic every 2s is always allowed. a consumption does not delay refilling.
	for i := int64(1); i <= 5; i++ {
		if ok, _, _ := allow(base + 2*i*sec); !ok {
			t.Fatalf("request at +%ds should be allowed", 2*i)
		}
	}
	// after a long idle period the bucket is full but not over-filled
	now := base + 1000*sec
	for i := 0; i < 4; i++ {
		if ok, _, _ := allow(now); !ok {
			t.Fatalf("[%d] should be allowed after idle", i)
		}
	}
	if ok, _, _ := allow(now); ok {
		t.Fatal("bucket must not exceed the burst size")
	}
}
