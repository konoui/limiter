package limiter

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	defaultNegativeCacheSize = 1000
	defaultNegativeCacheTTL  = 5 * time.Minute
)

// negativeCache remembers invalid bucket ids for a limited time.
// Each RateLimit owns its own instance so that limiters for different tables do not affect each other.
// A nil *negativeCache is valid and behaves as a disabled cache.
type negativeCache struct {
	c   *lru.Cache[string, time.Time]
	ttl time.Duration
}

// newNegativeCache returns nil (disabled) when size is zero.
func newNegativeCache(size int, ttl time.Duration) *negativeCache {
	if size <= 0 {
		return nil
	}
	c, err := lru.New[string, time.Time](size)
	if err != nil {
		return nil
	}
	return &negativeCache{c: c, ttl: ttl}
}

func (n *negativeCache) Contains(key string) bool {
	if n == nil {
		return false
	}
	expiresAt, ok := n.c.Get(key)
	if !ok {
		return false
	}
	if !timeNow().Before(expiresAt) {
		n.c.Remove(key)
		return false
	}
	return true
}

func (n *negativeCache) Add(key string) {
	if n == nil {
		return
	}
	n.c.Add(key, timeNow().Add(n.ttl))
}

func (n *negativeCache) Len() int {
	if n == nil {
		return 0
	}
	return n.c.Len()
}
