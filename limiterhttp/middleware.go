// Package limiterhttp provides net/http middleware for the limiter.
package limiterhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/konoui/limiter"
)

// KeyFunc extracts a bucket id from a request, e.g. an API key header or a client IP address.
type KeyFunc func(*http.Request) (string, error)

// Result is the outcome of the rate limit check. It is stored in the request context.
type Result struct {
	Key      string
	Decision limiter.Decision
	// Err is for logging. A request may be allowed with an error when fail-open is enabled.
	Err error
	// Status is the HTTP status for the result. http.StatusOK means the request is passed to the next handler.
	Status int
}

type contextKey struct{}

// NewContext returns a context carrying res.
func NewContext(ctx context.Context, res *Result) context.Context {
	return context.WithValue(ctx, contextKey{}, res)
}

// FromContext returns the Result stored by Middleware.
func FromContext(ctx context.Context) (*Result, bool) {
	v, ok := ctx.Value(contextKey{}).(*Result)
	return v, ok
}

type options struct {
	onReject func(http.ResponseWriter, *http.Request, *Result)
	onError  func(*http.Request, error)
	skip     func(*http.Request) bool
	timeout  time.Duration
}

// Option configures Middleware and PrepareHandler.
type Option func(*options)

// WithSkip passes requests for which skip returns true to the next handler without rate limiting,
// e.g. health checks or CORS preflight requests. FromContext returns false for skipped requests.
func WithSkip(skip func(*http.Request) bool) Option {
	return func(o *options) { o.skip = skip }
}

// WithTimeout bounds the time spent on the limiter per request, including DynamoDB retries.
// On timeout the limiter returns an error, so the request is rejected with 500 unless fail-open is enabled.
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// WithRejectHandler customizes the response of rejected requests.
// Rate limit headers are already set when it is called.
func WithRejectHandler(f func(http.ResponseWriter, *http.Request, *Result)) Option {
	return func(o *options) { o.onReject = f }
}

// WithErrorHandler is called whenever the limiter returns an error, including fail-open cases.
func WithErrorHandler(f func(*http.Request, error)) Option {
	return func(o *options) { o.onError = f }
}

func newOptions(opts []Option) *options {
	o := &options{
		onReject: func(w http.ResponseWriter, _ *http.Request, res *Result) {
			// never expose internal error details to clients
			http.Error(w, http.StatusText(res.Status), res.Status)
		},
		onError: func(*http.Request, error) {},
		skip:    func(*http.Request) bool { return false },
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// StatusCode converts a result of limiter.Allow to an HTTP status code.
func StatusCode(d limiter.Decision, err error) int {
	switch {
	case errors.Is(err, limiter.ErrInvalidBucketID):
		return http.StatusBadRequest
	case err != nil && d.Allowed:
		// fail-open
		return http.StatusOK
	case errors.Is(err, limiter.ErrStoreThrottled):
		// server side capacity problem, not the client's rate limit
		return http.StatusServiceUnavailable
	case err != nil:
		return http.StatusInternalServerError
	case !d.Allowed:
		return http.StatusTooManyRequests
	default:
		return http.StatusOK
	}
}

// Middleware rejects requests over the rate limit and passes others to the next handler.
// The next handler can read the Result with FromContext.
func Middleware(l limiter.Limiter, keyFn KeyFunc, opts ...Option) func(http.Handler) http.Handler {
	o := newOptions(opts)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if o.skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			res := &Result{}
			key, err := keyFn(r)
			if err != nil {
				res.Err, res.Status = err, http.StatusBadRequest
			} else {
				res.Key = key
				res.Decision, res.Err = o.allow(r.Context(), l, key)
				res.Status = StatusCode(res.Decision, res.Err)
			}
			r = r.WithContext(NewContext(r.Context(), res))
			if res.Err != nil {
				o.onError(r, res.Err)
			}

			setHeaders(w, res)
			if res.Status != http.StatusOK {
				o.onReject(w, r, res)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (o *options) allow(ctx context.Context, l limiter.Limiter, key string) (limiter.Decision, error) {
	if o.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}
	return l.Allow(ctx, key)
}

// HeaderKey returns a KeyFunc that uses the value of the header, e.g. an API key.
func HeaderKey(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		if v := r.Header.Get(name); v != "" {
			return v, nil
		}
		return "", fmt.Errorf("%s header is required", name)
	}
}

// RemoteIP is a KeyFunc that uses the client IP address of the connection.
// RemoteAddr is "host:port" on net/http servers but only "host" on AWS Lambda (aws-lambda-go-api-proxy),
// where it is the source IP reported by API Gateway.
// X-Forwarded-For is intentionally ignored because clients can forge it unless a trusted proxy overwrites it.
func RemoteIP(r *http.Request) (string, error) {
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return ip, nil
	}
	if ip := net.ParseIP(r.RemoteAddr); ip != nil {
		return ip.String(), nil
	}
	return "", fmt.Errorf("invalid remote address: %q", r.RemoteAddr)
}

func setHeaders(w http.ResponseWriter, res *Result) {
	// Remaining is exact for a single shard and an underestimate of the whole bucket for sharded buckets.
	if res.Err == nil {
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Decision.Remaining, 10))
	}
	switch res.Status {
	case http.StatusTooManyRequests:
		w.Header().Set("Retry-After", retryAfterSeconds(res.Decision.RetryAfter))
	case http.StatusServiceUnavailable:
		w.Header().Set("Retry-After", "1")
	}
}

// Retry-After takes whole seconds. Round up so that a client does not retry too early.
func retryAfterSeconds(d time.Duration) string {
	return strconv.FormatInt(int64(math.Ceil(d.Seconds())), 10)
}

// PrepareHandler issues a new bucket id on POST and responds {"token": "<id>"}.
func PrepareHandler(p limiter.Preparer, newKey func() (string, error), opts ...Option) http.Handler {
	o := newOptions(opts)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		key, err := newKey()
		if err == nil {
			err = p.PrepareTokens(r.Context(), key)
		}
		if err != nil {
			o.onError(r, err)
			status := http.StatusInternalServerError
			if errors.Is(err, limiter.ErrStoreThrottled) {
				status = http.StatusServiceUnavailable
				w.Header().Set("Retry-After", "1")
			}
			http.Error(w, http.StatusText(status), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"token": key})
	})
}
