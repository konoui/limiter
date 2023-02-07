package limiter

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

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
