package limiter

import (
	"io"
	"time"

	"github.com/goccy/go-yaml"
)

type Config struct {
	TableName        string        `json:"table_name"`
	TokenPerInterval int64         `json:"token_per_interval"`
	BucketSize       int64         `json:"bucket_size"`
	Interval         time.Duration `json:"interval,omitempty"`
}

func NewConfig(r io.Reader) (*Config, error) {
	c, err := newConfig(r)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newConfig(ro io.Reader) (*Config, error) {
	cfg := new(Config)
	err := yaml.NewDecoder(ro).Decode(cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
