package limiterhttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/konoui/limiter"
	"github.com/konoui/limiter/limiterhttp"
	mock "github.com/konoui/limiter/mock_limiter"
	"go.uber.org/mock/gomock"
)

const headerKey = "x-api-key"

var headerKeyFunc = limiterhttp.HeaderKey(headerKey)

func TestHeaderKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, err := headerKeyFunc(r); err == nil {
		t.Error("missing header should be an error")
	}
	r.Header.Set(headerKey, "k")
	if got, err := headerKeyFunc(r); err != nil || got != "k" {
		t.Errorf("want k, got %q %v", got, err)
	}
}

func TestMiddleware_skip(t *testing.T) {
	l := mock.NewMockLimiter(gomock.NewController(t)) // Allow must not be called
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if _, ok := limiterhttp.FromContext(r.Context()); ok {
			t.Error("skipped requests have no result")
		}
	})
	h := limiterhttp.Middleware(l, headerKeyFunc,
		limiterhttp.WithSkip(func(r *http.Request) bool { return r.Method == http.MethodOptions }))(next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodOptions, "/", nil))
	if !called || rec.Code != http.StatusOK {
		t.Errorf("skipped request should reach the next handler: called %v, status %d", called, rec.Code)
	}
}

func TestMiddleware_timeout(t *testing.T) {
	l := mock.NewMockLimiter(gomock.NewController(t))
	l.EXPECT().Allow(gomock.Any(), "k").DoAndReturn(func(ctx context.Context, _ string) (limiter.Decision, error) {
		<-ctx.Done() // a slow DynamoDB
		return limiter.Decision{}, fmt.Errorf("%w: %w", limiter.ErrInternal, ctx.Err())
	})
	h := limiterhttp.Middleware(l, headerKeyFunc, limiterhttp.WithTimeout(10*time.Millisecond))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("must not be called") }))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(headerKey, "k")
	rec := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec, req)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("timeout did not bound the latency: %v", elapsed)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("want 500, got %d", rec.Code)
	}
}

func TestMiddleware(t *testing.T) {
	tests := []struct {
		name          string
		noKey         bool
		decision      limiter.Decision
		err           error
		wantStatus    int
		wantNext      bool
		wantRetry     string
		wantRemaining string
		wantErrCalled bool
	}{
		{
			name:          "allowed",
			decision:      limiter.Decision{Allowed: true, Remaining: 3},
			wantStatus:    http.StatusOK,
			wantNext:      true,
			wantRemaining: "3",
		},
		{
			name:          "denied with retry-after rounded up",
			decision:      limiter.Decision{Allowed: false, RetryAfter: 1500 * time.Millisecond},
			wantStatus:    http.StatusTooManyRequests,
			wantRetry:     "2",
			wantRemaining: "0",
		},
		{
			name:          "missing key",
			noKey:         true,
			wantStatus:    http.StatusBadRequest,
			wantErrCalled: true,
		},
		{
			name:          "invalid bucket id",
			err:           limiter.ErrInvalidBucketID,
			wantStatus:    http.StatusBadRequest,
			wantErrCalled: true,
		},
		{
			name:          "dynamodb throttling is 503, not 429",
			err:           limiter.ErrStoreThrottled,
			wantStatus:    http.StatusServiceUnavailable,
			wantRetry:     "1",
			wantErrCalled: true,
		},
		{
			name:          "fail-open passes the request and reports the error",
			decision:      limiter.Decision{Allowed: true},
			err:           limiter.ErrStoreThrottled,
			wantStatus:    http.StatusOK,
			wantNext:      true,
			wantErrCalled: true,
		},
		{
			name:          "internal error",
			err:           limiter.ErrInternal,
			wantStatus:    http.StatusInternalServerError,
			wantErrCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := mock.NewMockLimiter(gomock.NewController(t))
			if !tt.noKey {
				l.EXPECT().Allow(gomock.Any(), "dummy").Return(tt.decision, tt.err)
			}

			nextCalled, errCalled := false, false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				res, ok := limiterhttp.FromContext(r.Context())
				if !ok || res.Key != "dummy" {
					t.Errorf("result should be in the context: %+v", res)
				}
				w.WriteHeader(http.StatusOK)
			})
			h := limiterhttp.Middleware(l, headerKeyFunc,
				limiterhttp.WithErrorHandler(func(*http.Request, error) { errCalled = true }))(next)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if !tt.noKey {
				req.Header.Set(headerKey, "dummy")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status: want %d, got %d", tt.wantStatus, rec.Code)
			}
			if nextCalled != tt.wantNext {
				t.Errorf("next called: want %v, got %v", tt.wantNext, nextCalled)
			}
			if errCalled != tt.wantErrCalled {
				t.Errorf("error handler called: want %v, got %v", tt.wantErrCalled, errCalled)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantRetry {
				t.Errorf("Retry-After: want %q, got %q", tt.wantRetry, got)
			}
			if got := rec.Header().Get("X-RateLimit-Remaining"); got != tt.wantRemaining {
				t.Errorf("X-RateLimit-Remaining: want %q, got %q", tt.wantRemaining, got)
			}
		})
	}
}

func TestPrepareHandler(t *testing.T) {
	newKey := func() (string, error) { return "new-token", nil }

	t.Run("issues a token", func(t *testing.T) {
		p := mock.NewMockPreparer(gomock.NewController(t))
		p.EXPECT().PrepareTokens(gomock.Any(), "new-token").Return(nil)
		rec := httptest.NewRecorder()
		limiterhttp.PrepareHandler(p, newKey).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/create", nil))
		if rec.Code != http.StatusCreated {
			t.Fatalf("want 201, got %d", rec.Code)
		}
		body := map[string]string{}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["token"] != "new-token" {
			t.Errorf("unexpected body %q: %v", rec.Body.String(), err)
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		p := mock.NewMockPreparer(gomock.NewController(t))
		rec := httptest.NewRecorder()
		limiterhttp.PrepareHandler(p, newKey).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/create", nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
			t.Errorf("want 405 with Allow header, got %d %q", rec.Code, rec.Header().Get("Allow"))
		}
	})

	for _, tt := range []struct {
		err  error
		want int
	}{
		{err: limiter.ErrStoreThrottled, want: http.StatusServiceUnavailable},
		{err: errors.New("boom"), want: http.StatusInternalServerError},
	} {
		t.Run(tt.err.Error(), func(t *testing.T) {
			p := mock.NewMockPreparer(gomock.NewController(t))
			p.EXPECT().PrepareTokens(gomock.Any(), gomock.Any()).Return(tt.err)
			rec := httptest.NewRecorder()
			limiterhttp.PrepareHandler(p, newKey).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/create", nil))
			if rec.Code != tt.want {
				t.Errorf("want %d, got %d", tt.want, rec.Code)
			}
			if rec.Body.String() == tt.err.Error()+"\n" {
				t.Error("error details must not be exposed")
			}
		})
	}
}
