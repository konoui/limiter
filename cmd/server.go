package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/konoui/limiter"
)

func MiddlewareLimiter(rl limiter.Limiter, headerKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := limiter.NewLimitHandler(rl, headerKey)
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

func start(addr string, rl *limiter.RateLimit, headerKey string) error {
	mux := http.NewServeMux()
	mux.Handle("/", MiddlewareLimiter(rl, headerKey,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, ok := limiter.FromContext(r.Context())
			if !ok {
				fmt.Fprintf(w, "unexpected error")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if c.Err != nil {
				fmt.Fprint(w, c.Err.Error())
				w.WriteHeader(c.Status)
				return
			}

			if c.Throttle {
				fmt.Fprintf(w, "throttle")
				w.WriteHeader(c.Status)
				return
			}

			w.WriteHeader(c.Status)
			fmt.Fprintf(w, "ok")
		})))

	mux.HandleFunc("/create", limiter.NewPrepareTokenHandler(rl))
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 3 * time.Second,
	}
	return server.ListenAndServe()
}
