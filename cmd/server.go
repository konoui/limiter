package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/konoui/limiter"
	"github.com/konoui/limiter/handlerfunc"
)

func MiddlewareLimiter(rl limiter.Limiter, getKey handlerfunc.GetKey, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := handlerfunc.NewHandler(rl, getKey)
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

func testHandler(r map[string]*limiter.RateLimit) (http.Handler, error) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := handlerfunc.FromContext(r.Context())
		if !ok {
			fmt.Fprintf(w, "unexpected error")
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		defer func() {
			if c.Err != nil {
				log.Println("Error", c.Err)
			}
		}()

		if c.Throttle {
			w.WriteHeader(c.Status)
			fmt.Fprintf(w, "throttle")
			return
		}

		w.WriteHeader(c.Status)
		fmt.Fprintf(w, "ok")
	})

	drl, ok := r["default"]
	if !ok {
		return nil, fmt.Errorf("default is not defined in config")
	}
	arl, ok := r["anon"]
	if !ok {
		return nil, fmt.Errorf("anon is not defined in config")
	}
	mux := http.NewServeMux()
	mux.Handle("/",
		MiddlewareLimiter(
			drl,
			func(r *http.Request) (string, error) { return r.Header.Get("x-api-key"), nil },
			h))
	mux.Handle("/anon",
		MiddlewareLimiter(
			arl,
			func(r *http.Request) (string, error) {
				ip, _, err := net.SplitHostPort(r.RemoteAddr)
				return ip, err
			},
			h))
	return mux, nil
}

func StartServer(addr string, handler http.Handler) error {
	if addr != "" {
		logger.Info("starting")
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
