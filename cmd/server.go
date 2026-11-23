package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/konoui/limiter"
	"github.com/konoui/limiter/limiterhttp"
)

// testHandler serves an example API.
//
//	GET /anon  rate limited by client IP address with the "anon" limiter
//	GET /*     rate limited by the x-api-key header with the "default" limiter
//
// API keys are issued by the create-token command. They are not issued over HTTP
// because an unauthenticated endpoint would let anyone mint keys and bypass the per-key limit.
func testHandler(r map[string]*limiter.RateLimit) (http.Handler, error) {
	drl, ok := r["default"]
	if !ok {
		return nil, fmt.Errorf("\"default\" is not defined in the config")
	}
	arl, ok := r["anon"]
	if !ok {
		return nil, fmt.Errorf("\"anon\" is not defined in the config")
	}

	onError := limiterhttp.WithErrorHandler(func(r *http.Request, err error) {
		logger.WarnContext(r.Context(), "limiter error", slog.String("path", r.URL.Path), slog.String("error", err.Error()))
	})
	ok200 := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// bound the latency added to every request when DynamoDB is slow
	timeout := limiterhttp.WithTimeout(500 * time.Millisecond)

	mux := http.NewServeMux()
	mux.Handle("/anon", limiterhttp.Middleware(arl, limiterhttp.RemoteIP, onError, timeout)(ok200))
	mux.Handle("/", limiterhttp.Middleware(drl, limiterhttp.HeaderKey("x-api-key"), onError, timeout)(ok200))
	return mux, nil
}

func StartServer(addr string, handler http.Handler) error {
	if addr != "" {
		logger.Info("starting", slog.String("addr", addr))
		server := &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           handler,
		}
		return server.ListenAndServe()
	}

	lambda.Start(httpadapter.NewV2(handler).ProxyWithContext)
	return nil
}
