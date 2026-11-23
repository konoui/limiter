package limiter

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewConfig_InvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantMsg string
	}{
		{
			name:    "typo in a key is reported",
			input:   "table_name: t\ntoken_per_intervel: 10\nbucket_size: 20\n",
			wantMsg: "token_per_intervel",
		},
		{
			name:    "missing token_per_interval names the actual key",
			input:   "table_name: t\nbucket_size: 20\n",
			wantMsg: "token_per_interval must be greater than zero",
		},
		{
			name:    "name must not contain the delimiter",
			input:   "name: a#b\ntable_name: t\ntoken_per_interval: 10\nbucket_size: 20\n",
			wantMsg: "name must not contain",
		},
		{
			name:    "missing table_name",
			input:   "token_per_interval: 10\nbucket_size: 20\n",
			wantMsg: "table_name must not be empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConfig(strings.NewReader(tt.input))
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantMsg)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     *Config
		wantErr  bool
	}{
		{
			name:     "parse config file",
			filename: "test-config.yaml",
			want: &Config{
				TableName:        "dummy_table",
				BucketSize:       40,
				TokenPerInterval: 20,
				Interval:         60 * time.Second,
			},
		},
		{
			name:     "parse config file",
			filename: "test-add-config.yaml",
			want: &Config{
				TableName:        "dummy_table",
				BucketSize:       40,
				TokenPerInterval: 20,
				Interval:         60 * time.Second,
				AdditionalConfig: &AdditionalConfig{
					NegativeCache: &NegativeCacheConfig{
						Size: 2,
					},
					Anonymous: &AnonymousConfig{
						RecordTTL: 24 * time.Hour,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tt.filename))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			got, err := NewConfig(f)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
