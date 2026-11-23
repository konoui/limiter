package main

import (
	"fmt"
	"io"

	"github.com/goccy/go-yaml"
	"github.com/konoui/limiter"
)

type Config map[string]*limiter.Config

func NewConfig(r io.Reader) (Config, error) {
	cfg := new(Config)
	// reject unknown keys so that a typo is reported instead of silently becoming a zero value
	if err := yaml.NewDecoder(r, yaml.DisallowUnknownField()).Decode(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return *cfg, nil
}
