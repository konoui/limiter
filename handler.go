package limiter

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

type key int

const (
	throttleKey key = iota
	prepareKey
)

type ThrottleContext struct {
	Err      error
	Throttle bool
	Status   int
	Token    string
}

func WithThrottleContext(ctx context.Context, t ThrottleContext) context.Context {
	return context.WithValue(ctx, throttleKey, &t)
}

func FromThrottleContext(ctx context.Context) (ThrottleContext, bool) {
	v, ok := ctx.Value(ctx).(ThrottleContext)
	return v, ok
}

type PrepareContext struct {
	Err    error
	Status int
	Token  string
}

func WithPrepareContext(ctx context.Context, t PrepareContext) context.Context {
	return context.WithValue(ctx, prepareKey, &t)
}

func FromPrepareContext(ctx context.Context) (PrepareContext, bool) {
	v, ok := ctx.Value(ctx).(PrepareContext)
	return v, ok
}

func NewLimitHandler(rl Limiter, headerKey string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(headerKey)
		if token != "" {
			t := ThrottleContext{
				Token:    token,
				Status:   http.StatusBadRequest,
				Err:      fmt.Errorf("%s header is empty", headerKey),
				Throttle: true,
			}

			*r = *r.WithContext(WithThrottleContext(r.Context(), t))
			return
		}

		throttle, err := rl.ShouldThrottle(r.Context(), token)
		if err != nil {
			t := ThrottleContext{
				Token:    token,
				Status:   http.StatusInternalServerError,
				Err:      err,
				Throttle: true,
			}
			*r = *r.WithContext(WithThrottleContext(r.Context(), t))
			return
		}

		if throttle {
			t := ThrottleContext{
				Token:    token,
				Status:   http.StatusTooManyRequests,
				Err:      nil,
				Throttle: true,
			}
			*r = *r.WithContext(WithThrottleContext(r.Context(), t))
			return
		}

		t := ThrottleContext{
			Token:    token,
			Status:   http.StatusOK,
			Err:      nil,
			Throttle: false,
		}
		*r = *r.WithContext(WithThrottleContext(r.Context(), t))
	})
}

func NewPrepareTokenHandler(rl Preparer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			c := PrepareContext{
				Err:    fmt.Errorf("invalid method: %s", r.Method),
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(WithPrepareContext(r.Context(), c))
			return
		}

		uid, err := uuid.NewRandom()
		if err != nil {
			c := PrepareContext{
				Err:    err,
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(WithPrepareContext(r.Context(), c))
			return
		}

		key := uid.String()
		if err := rl.PrepareTokens(r.Context(), key); err != nil {
			c := PrepareContext{
				Err:    err,
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(WithPrepareContext(r.Context(), c))
			return
		}

		c := PrepareContext{
			Err:    nil,
			Status: http.StatusOK,
			Token:  key,
		}
		*r = *r.WithContext(WithPrepareContext(r.Context(), c))
	})
}
