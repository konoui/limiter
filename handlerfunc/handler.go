package handlerfunc

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/konoui/limiter"
)

type key struct{}
type GetKey = func(*http.Request) (string, error)

var (
	contextKey = &key{}
)

type Context struct {
	Err      error
	Throttle bool
	Status   int
	Token    string
}

// ResultToStatus is an utility.
// convert values returned by limiter.ShouldThrottle() to a http status code
func ResultToStatus(throttle bool, err error) int {
	switch {
	case err == nil:
		if throttle {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	case errors.Is(err, limiter.ErrInvalidBucketID):
		return http.StatusBadRequest
	case errors.Is(err, limiter.ErrRateLimitExceeded):
		if throttle {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	case errors.Is(err, limiter.ErrInternal):
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

func NewContext(ctx context.Context, lc *Context) context.Context {
	return context.WithValue(ctx, contextKey, lc)
}

func FromContext(ctx context.Context) (*Context, bool) {
	v, ok := ctx.Value(contextKey).(*Context)
	return v, ok
}

// NewLimitHandler will check whether rate limit exceeded or not.
// It provide *Context that includes throttle or not, http status and an an error.
// There is a case that status is ok but context has an error.
// We should use `Throttle“ to allow/deny requests. Assuming an `Err` is used as logging.
func NewHandler(rl limiter.Limiter, getKey GetKey) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := getKey(r)
		if err != nil {
			lc := &Context{
				Token:    key,
				Status:   http.StatusBadRequest,
				Err:      err,
				Throttle: true,
			}

			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		throttle, err := rl.ShouldThrottle(r.Context(), key)
		status := ResultToStatus(throttle, err)
		lc := &Context{
			Token:    key,
			Status:   status,
			Err:      err,
			Throttle: throttle,
		}
		*r = *r.WithContext(NewContext(r.Context(), lc))
	})
}

func NewPrepareTokenHandler(rl limiter.Preparer, generator func() (string, error)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			lc := &Context{
				Err:    fmt.Errorf("invalid method: %s", r.Method),
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		key, err := generator()
		if err != nil {
			lc := &Context{
				Err:    err,
				Status: http.StatusInternalServerError,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		if err := rl.PrepareTokens(r.Context(), key); err != nil {
			if errors.Is(err, limiter.ErrRateLimitExceeded) {
				lc := &Context{
					Err:    err,
					Status: http.StatusTooManyRequests,
				}
				*r = *r.WithContext(NewContext(r.Context(), lc))
				return
			}
			lc := &Context{
				Err:    err,
				Status: http.StatusInternalServerError,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		lc := &Context{
			Err:    nil,
			Status: http.StatusOK,
			Token:  key,
		}
		*r = *r.WithContext(NewContext(r.Context(), lc))
	})
}
