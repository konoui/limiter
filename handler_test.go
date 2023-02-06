package limiter_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/konoui/limiter"
	mock "github.com/konoui/limiter/mock_limiter"
)

func MiddlewareLimiter(rl limiter.LimitPreparer, headerKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := limiter.NewLimitHandler(rl, headerKey)
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

func MiddlewarePrepare(rl limiter.LimitPreparer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := limiter.NewPrepareTokenHandler(rl)
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

const headerKey = "authentication"

func Test_Handler(t *testing.T) {
	tests := []struct {
		name   string
		mocker func(rl *mock.MockLimitPreparer)
		req    func() *http.Request
		status int
		msg    string
	}{
		{
			name: "non throttle return ok",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					ShouldThrottle(gomock.Any(), gomock.Any()).
					Return(false, nil)
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Add(headerKey, "dummy")
				return req
			},
			status: http.StatusOK,
			msg:    "ok",
		},
		{
			name:   "header key is empty",
			mocker: func(rl *mock.MockLimitPreparer) {},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				return req
			},
			status: http.StatusBadRequest,
			msg:    fmt.Sprintf("%s header is empty", headerKey),
		},
		{
			name: "throttle return throttle",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					ShouldThrottle(gomock.Any(), gomock.Any()).
					Return(true, nil)
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Add(headerKey, "dummy")
				return req
			},
			status: http.StatusTooManyRequests,
			msg:    "throttle",
		},
		{
			name: "dynamodb api rate limit error",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					ShouldThrottle(gomock.Any(), gomock.Any()).
					Return(true, limiter.ErrRateLimitExceeded)
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Add(headerKey, "dummy")
				return req
			},
			status: http.StatusTooManyRequests,
			msg:    limiter.ErrRateLimitExceeded.Error(),
		},
		{
			name: "dynamodb api rate limit error, non throttle",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					ShouldThrottle(gomock.Any(), gomock.Any()).
					Return(false, limiter.ErrRateLimitExceeded)
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Add(headerKey, "dummy")
				return req
			},
			status: http.StatusOK,
			msg:    limiter.ErrRateLimitExceeded.Error(),
		},
		{
			name: "internal server error",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					ShouldThrottle(gomock.Any(), gomock.Any()).
					Return(true, errors.New("error"))
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				req.Header.Add(headerKey, "dummy")
				return req
			},
			status: http.StatusInternalServerError,
			msg:    "error",
		},
		{
			name: "create token",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					PrepareTokens(gomock.Any(), gomock.Any()).
					Return(nil)
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/create", nil)
				return req
			},
			status: http.StatusOK,
			msg:    "created",
		},
		{
			name: "create token error",
			mocker: func(rl *mock.MockLimitPreparer) {
				rl.EXPECT().
					PrepareTokens(gomock.Any(), gomock.Any()).
					Return(errors.New("error"))
			},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/create", nil)
				return req
			},
			status: http.StatusInternalServerError,
			msg:    "error",
		},
		{
			name:   "invalid method",
			mocker: func(rl *mock.MockLimitPreparer) {},
			req: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/create", nil)
				return req
			},
			status: http.StatusBadRequest,
			msg:    "invalid method: GET",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			rl := mock.NewMockLimitPreparer(ctrl)
			tt.mocker(rl)

			mux := http.NewServeMux()
			mux.Handle("/", MiddlewareLimiter(rl, headerKey, http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					c, ok := limiter.FromContext(r.Context())
					if !ok {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprintf(w, "internal server error")
						return
					}

					if c.Err != nil {
						w.WriteHeader(c.Status)
						fmt.Fprint(w, c.Err.Error())
						return
					}

					if c.Throttle {
						w.WriteHeader(c.Status)
						fmt.Fprintf(w, "throttle")
						return
					}

					w.WriteHeader(c.Status)
					fmt.Fprintf(w, "ok")
				},
			)))

			mux.Handle("/create", MiddlewarePrepare(rl,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					c, ok := limiter.FromContext(r.Context())
					if !ok {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprintf(w, "internal server error")
						return
					}
					if c.Err != nil {
						w.WriteHeader(c.Status)
						fmt.Fprint(w, c.Err.Error())
						return
					}

					w.WriteHeader(c.Status)
					fmt.Fprintf(w, "created")
				})))

			req := tt.req()
			got := httptest.NewRecorder()
			mux.ServeHTTP(got, req)
			if got.Code != tt.status {
				t.Errorf("want: %d, got: %d", tt.status, got.Code)
			}
			if got := got.Body.String(); got != tt.msg {
				t.Errorf("want: %s, got: %s", tt.msg, got)
			}
		})
	}
}
