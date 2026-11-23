package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/google/uuid"
	"github.com/konoui/limiter"
	"github.com/konoui/limiter/handlerfunc"
)

// testHandler serves an example API.
//
//	POST /create  issues an API key for the "default" limiter
//	GET  /anon    rate limited by client IP address with the "anon" limiter
//	GET  /*       rate limited by the x-api-key header with the "default" limiter
func testHandler(r map[string]*limiter.RateLimit) (http.Handler, error) {
	drl, ok := r["default"]
	if !ok {
		return nil, fmt.Errorf("\"default\" is not defined in the config")
	}
	arl, ok := r["anon"]
	if !ok {
		return nil, fmt.Errorf("\"anon\" is not defined in the config")
	}

	onError := handlerfunc.WithErrorHandler(func(r *http.Request, err error) {
		logger.WarnContext(r.Context(), "limiter error", slog.String("path", r.URL.Path), slog.String("error", err.Error()))
	})
	ok200 := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	mux := http.NewServeMux()
	mux.Handle("/create", handlerfunc.PrepareHandler(drl, newToken, onError))
	mux.Handle("/anon", handlerfunc.Middleware(arl, remoteIP, onError)(ok200))
	mux.Handle("/", handlerfunc.Middleware(drl, apiKey, onError)(ok200))
	return mux, nil
}

func newToken() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func apiKey(r *http.Request) (string, error) {
	if v := r.Header.Get("x-api-key"); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("x-api-key header is required")
}

// remoteIP returns the client IP address.
// RemoteAddr is "host:port" on net/http server but only "host" on Lambda (aws-lambda-go-api-proxy).
func remoteIP(r *http.Request) (string, error) {
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return ip, nil
	}
	if ip := net.ParseIP(r.RemoteAddr); ip != nil {
		return ip.String(), nil
	}
	return "", fmt.Errorf("invalid remote address: %q", r.RemoteAddr)
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
