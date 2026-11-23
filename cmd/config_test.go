package main

import (
	"os"
	"testing"

	"github.com/konoui/limiter"
)

// the example config is baked into the container image, so it must stay valid
func TestExampleConfig(t *testing.T) {
	f, err := os.Open("example-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatal(err)
	}
	rl := map[string]*limiter.RateLimit{}
	for key, c := range cfg {
		l, err := limiter.New(c, nil)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		rl[key] = l
	}
	if _, err := testHandler(rl); err != nil {
		t.Fatal(err)
	}
}
