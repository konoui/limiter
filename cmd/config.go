package main

import (
	"io"

	"github.com/goccy/go-yaml"
	"github.com/konoui/limiter"
)

type Config map[string]*limiter.Config

func NewConfig(r io.Reader) (Config, error) {
	cfg := new(Config)
	if err := yaml.NewDecoder(r).Decode(cfg); err != nil {
		return nil, err
	}
	return *cfg, nil
}
