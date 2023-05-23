package main

import (
	"fmt"
	"log"
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

func start(addr string, rl *limiter.RateLimit, headerKey string) error {
	mux := http.NewServeMux()
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
	mux.Handle("/",
		MiddlewareLimiter(
			rl,
			func(r *http.Request) (string, error) { return r.Header.Get(headerKey), nil },
			h))
	mux.Handle("/anon",
		MiddlewareLimiter(
			rl,
			func(r *http.Request) (string, error) { return r.RemoteAddr, nil },
			h))
	if addr != "" {
		log.Println("starting")
		server := &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 3 * time.Second,
			Handler:           mux,
		}
		return server.ListenAndServe()
	}

	lambda.Start(httpadapter.NewV2(mux).ProxyWithContext)
	return nil
}
