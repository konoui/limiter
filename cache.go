package limiter

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	defaultNegativeCacheSize = 1000
	defaultNegativeCacheTTL  = 5 * time.Minute
	defaultDenyCacheSize     = 10000

	defaultRegistrationCacheSize = 10000
	defaultRegistrationCacheTTL  = time.Minute
)

// expiringCache is a size bounded LRU whose entries expire at a given time.
// Each RateLimit owns its own instances so that limiters for different tables do not affect each other.
// A nil *expiringCache is valid and behaves as a disabled cache.
type expiringCache struct {
	c *lru.Cache[string, time.Time]
}

// newExpiringCache returns nil (disabled) when size is zero.
func newExpiringCache(size int) *expiringCache {
	if size <= 0 {
		return nil
	}
	c, err := lru.New[string, time.Time](size)
	if err != nil {
		return nil
	}
	return &expiringCache{c: c}
}

// Get returns the expiry of the key if it exists and has not expired.
func (e *expiringCache) Get(key string) (time.Time, bool) {
	if e == nil {
		return time.Time{}, false
	}
	expiresAt, ok := e.c.Get(key)
	if !ok {
		return time.Time{}, false
	}
	if !timeNow().Before(expiresAt) {
		e.c.Remove(key)
		return time.Time{}, false
	}
	return expiresAt, true
}

func (e *expiringCache) Contains(key string) bool {
	_, ok := e.Get(key)
	return ok
}

func (e *expiringCache) AddUntil(key string, expiresAt time.Time) {
	if e == nil {
		return
	}
	e.c.Add(key, expiresAt)
}

func (e *expiringCache) Remove(key string) {
	if e == nil {
		return
	}
	e.c.Remove(key)
}

func (e *expiringCache) Len() int {
	if e == nil {
		return 0
	}
	return e.c.Len()
}
