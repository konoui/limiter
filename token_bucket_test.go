package limiter

import (
	"testing"
)

func Test_NewBucket(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, err := NewTokenBucket(0, 0)
		if err == nil {
			t.Fatal("non error")
		}
	})
}
