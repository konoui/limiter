package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/konoui/limiter"
)

var (
	rl *limiter.RateLimit
)

func init() {
	if rl != nil {
		return
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	bucket := limiter.NewTokenBucket(10, 20)
	fmt.Printf("bucket %#v\n", bucket)
	// set global client
	rl = limiter.New("buckets_table", bucket, client)
}

func MiddlewareLimiter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := limiter.NewLimitHandler(rl, "X-API-Token")
		h.ServeHTTP(w, r)
		next.ServeHTTP(w, r)
	})
}

func main() {
	mux := http.NewServeMux()
	mux.Handle("/hello", MiddlewareLimiter(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			th, ok := limiter.FromContext(r.Context())
			if !ok {
				fmt.Fprintf(w, "unexpected error")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if th.Err != nil {
				fmt.Fprintf(w, th.Err.Error())
				w.WriteHeader(th.Status)
				return
			}

			if th.Throttle {
				fmt.Fprintf(w, "throttle")
				w.WriteHeader(th.Status)
				return
			}

			w.WriteHeader(th.Status)
			w.Write([]byte("ok"))
			return
		})))
	mux.HandleFunc("/create", limiter.NewPrepareTokenHandler(rl))
	lambda.Start(httpadapter.NewV2(mux).ProxyWithContext)
}
