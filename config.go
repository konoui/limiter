package limiter

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	// Name namespaces DynamoDB item keys ("<name>#<bucket id>#...") and is recorded as the limiter.name metric attribute.
	// Every limiter sharing a table must have a distinct name; otherwise a bucket id chosen by a client could
	// address another limiter's items. Optional for a limiter that owns its table. It must not contain "#".
	Name             string        `json:"name,omitempty"`
	TableName        string        `json:"table_name"`
	TokenPerInterval int64         `json:"token_per_interval"`
	BucketSize       int64         `json:"bucket_size"`
	Interval         time.Duration `json:"interval,omitempty"`
	// Shards is the number of shards per bucket. It is computed from the worst requests per second when zero.
	// Set it explicitly when the expected traffic per bucket id is known.
	// It can be changed at any time; shard items are created on demand.
	Shards           int64             `json:"shards,omitempty"`
	AdditionalConfig *AdditionalConfig `json:"additional,omitempty"`
}

type AdditionalConfig struct {
	NegativeCache *NegativeCacheConfig `json:"negative_cache,omitempty"`
	// RegistrationCache caches registered bucket ids. Revoked ids are allowed until the entry expires.
	RegistrationCache *RegistrationCacheConfig `json:"registration_cache,omitempty"`
	Anonymous         *AnonymousConfig         `json:"anonymous,omitempty"`
	// FailOpen allows requests when DynamoDB fails. Requests are denied by default (fail-closed).
	FailOpen bool `json:"fail_open,omitempty"`
}

// NegativeCacheConfig configures negative cache for invalid bucket ids.
// Size is the maximum number of entries. specifying 0 means to disable cache.
// TTL is how long an entry is kept. 5 minutes by default.
type NegativeCacheConfig struct {
	Size int           `json:"size"`
	TTL  time.Duration `json:"ttl,omitempty"`
}

// RegistrationCacheConfig configures the cache of registered bucket ids.
// Size is the maximum number of entries. specifying 0 means to disable cache (a read per request).
// TTL is how long an entry is kept. 1 minute by default.
type RegistrationCacheConfig struct {
	Size int           `json:"size"`
	TTL  time.Duration `json:"ttl,omitempty"`
}

// AnonymousConfig allows not registered bucket id.
// This is used for such as an IP address basis throttling.
type AnonymousConfig struct {
	RecordTTL time.Duration `json:"record_ttl"`
}

func cfgToOpts(c *Config) ([]Opt, error) {
	opts := make([]Opt, 0, 3)

	cfg := c.AdditionalConfig
	if cfg == nil {
		return opts, nil
	}

	if cfg.FailOpen {
		opts = append(opts, WithFailOpen())
	}

	if cfg.Anonymous != nil {
		ttl := cfg.Anonymous.RecordTTL
		minTTL := 1 * time.Second
		if ttl < minTTL {
			return nil, fmt.Errorf("anonymous.record_ttl is specified but value is less than %v", minTTL)
		}
		opts = append(opts, withAnonymous(ttl))
	}

	if cfg.NegativeCache != nil {
		size := cfg.NegativeCache.Size
		if size < 0 {
			return nil, errors.New("negative_cache.size is specified but value is less than zero")
		}
		ttl := cfg.NegativeCache.TTL
		if ttl < 0 {
			return nil, errors.New("negative_cache.ttl is specified but value is less than zero")
		}
		if ttl == 0 {
			ttl = defaultNegativeCacheTTL
		}
		opts = append(opts, withNegativeCache(size, ttl))
	}

	if cfg.RegistrationCache != nil {
		size := cfg.RegistrationCache.Size
		if size < 0 {
			return nil, errors.New("registration_cache.size is specified but value is less than zero")
		}
		ttl := cfg.RegistrationCache.TTL
		if ttl < 0 {
			return nil, errors.New("registration_cache.ttl is specified but value is less than zero")
		}
		if ttl == 0 {
			ttl = defaultRegistrationCacheTTL
		}
		opts = append(opts, withRegistrationCache(size, ttl))
	}
	return opts, nil
}

func NewConfig(r io.Reader) (*Config, error) {
	return newConfig(r)
}

func newConfig(ro io.Reader) (*Config, error) {
	cfg := new(Config)
	// reject unknown keys so that a typo is reported instead of silently becoming a zero value
	err := yaml.NewDecoder(ro, yaml.DisallowUnknownField()).Decode(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (cfg *Config) validate() error {
	if cfg.TableName == "" {
		return errors.New("table_name must not be empty")
	}
	// "a#b" could collide with name "a" and a bucket id "b#..."
	if strings.Contains(cfg.Name, delimiter) {
		return fmt.Errorf("name must not contain %q", delimiter)
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = DefaultInterval
	}
	if _, err := newGCRA(cfg.TokenPerInterval, cfg.BucketSize, interval, cfg.Shards); err != nil {
		return err
	}
	if _, err := cfgToOpts(cfg); err != nil {
		return err
	}
	return nil
}
