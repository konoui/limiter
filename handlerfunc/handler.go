package handlerfunc

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/konoui/limiter"
)

type key struct{}

var (
	contextKey    = &key{}
	uuidNewRandom = uuid.NewRandom
)

type Context struct {
	Err      error
	Throttle bool
	Status   int
	Token    string
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
func NewLimitHandler(rl limiter.Limiter, headerKey string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(headerKey)
		if token == "" {
			lc := &Context{
				Token:    token,
				Status:   http.StatusBadRequest,
				Err:      fmt.Errorf("%s header has no value", headerKey),
				Throttle: true,
			}

			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		throttle, err := rl.ShouldThrottle(r.Context(), token)
		if err != nil {
			lc := &Context{
				Token:    token,
				Status:   http.StatusInternalServerError,
				Err:      err,
				Throttle: throttle,
			}
			if errors.Is(err, limiter.ErrInvalidBucketID) {
				lc.Status = http.StatusBadRequest
			} else if errors.Is(err, limiter.ErrRateLimitExceeded) {
				status := http.StatusTooManyRequests
				if !throttle {
					status = http.StatusOK
				}
				lc.Status = status
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		if throttle {
			lc := &Context{
				Token:    token,
				Status:   http.StatusTooManyRequests,
				Err:      nil,
				Throttle: true,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		lc := &Context{
			Token:    token,
			Status:   http.StatusOK,
			Err:      nil,
			Throttle: false,
		}
		*r = *r.WithContext(NewContext(r.Context(), lc))
	})
}

func NewPrepareTokenHandler(rl limiter.Preparer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			lc := &Context{
				Err:    fmt.Errorf("invalid method: %s", r.Method),
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		uid, err := uuidNewRandom()
		if err != nil {
			lc := &Context{
				Err:    err,
				Status: http.StatusInternalServerError,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		key := uid.String()
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
