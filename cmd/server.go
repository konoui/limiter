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

func MiddlewareLimiter(rl limiter.Limiter, headerKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := handlerfunc.NewLimitHandler(rl, headerKey)
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

func start(addr string, rl *limiter.RateLimit, headerKey string) error {
	mux := http.NewServeMux()
	mux.Handle("/", MiddlewareLimiter(rl, headerKey,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				fmt.Fprintf(w, "throttle")
				w.WriteHeader(c.Status)
				return
			}

			w.WriteHeader(c.Status)
			fmt.Fprintf(w, "ok")
		})))

	if addr != "" {
		server := &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 3 * time.Second,
		}
		return server.ListenAndServe()
	}

	lambda.Start(httpadapter.NewV2(mux).ProxyWithContext)
	return nil
}
