package limiter

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

type key struct{}

var contextKey = &key{}

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

func NewLimitHandler(rl Limiter, headerKey string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(headerKey)
		if token == "" {
			lc := &Context{
				Token:    token,
				Status:   http.StatusBadRequest,
				Err:      fmt.Errorf("%s header is empty", headerKey),
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
				Throttle: true,
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

func NewPrepareTokenHandler(rl Preparer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			lc := &Context{
				Err:    fmt.Errorf("invalid method: %s", r.Method),
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		uid, err := uuid.NewRandom()
		if err != nil {
			lc := &Context{
				Err:    err,
				Status: http.StatusBadRequest,
			}
			*r = *r.WithContext(NewContext(r.Context(), lc))
			return
		}

		key := uid.String()
		if err := rl.PrepareTokens(r.Context(), key); err != nil {
			lc := &Context{
				Err:    err,
				Status: http.StatusBadRequest,
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
