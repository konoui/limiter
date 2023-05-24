package limiter

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	TableName        string            `json:"table_name"`
	TokenPerInterval int64             `json:"token_per_interval"`
	BucketSize       int64             `json:"bucket_size"`
	Interval         time.Duration     `json:"interval,omitempty"`
	AdditionalConfig *AdditionalConfig `json:"additional,omitempty"`
}

type AdditionalConfig struct {
	NegativeCache *NegativeCacheConfig `json:"negative_cache,omitempty"`
	Anonymous     *AnonymousConfig     `json:"anonymous,omitempty"`
}

// WithNegativeCache configures negative cache entry size for invalid bucket ids.
// specifying 0 means to disable cache
type NegativeCacheConfig struct {
	Size int `json:"size"`
}

// AnonymousConfig allows not registered bucket id.
// This is used for such as an IP address basis throttling.
type AnonymousConfig struct {
	RecordTTL time.Duration `json:"record_ttl,"`
}

func cfgToOpts(cfg *AdditionalConfig) ([]Opt, error) {
	opts := make([]Opt, 0, 3)

	if cfg == nil {
		return opts, nil
	}

	if cfg.Anonymous != nil {
		ttl := cfg.Anonymous.RecordTTL
		max := 1 * time.Second
		if ttl < max {
			return nil, fmt.Errorf("anonymous.record_ttl is specified but value is less than %v", max)
		}
		opts = append(opts, withAnonymous(ttl))
	}

	if cfg.NegativeCache != nil {
		size := cfg.NegativeCache.Size
		if size < 0 {
			return nil, errors.New("negative_cache.size is specified but value is less than zero")
		}
		opts = append(opts, withNegativeCache(size))
	}
	return opts, nil
}

func NewConfig(r io.Reader) (*Config, error) {
	return newConfig(r)
}

func newConfig(ro io.Reader) (*Config, error) {
	cfg := new(Config)
	err := yaml.NewDecoder(ro).Decode(cfg)
	if err != nil {
		return nil, err
	}

	_, err = cfgToOpts(cfg.AdditionalConfig)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
