package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/konoui/limiter"
)

type usage string

const (
	unknown usage = "zzzz"
	free    usage = "aaaa"
	usual   usage = "bbba"
	advance usage = "cccc"
)

func (u usage) String() string {
	return string(u)
}

func detectUsage(bucketID string) usage {
	switch {
	case strings.HasPrefix(bucketID, free.String()):
		return free
	case strings.HasPrefix(bucketID, usual.String()):
		return usual
	case strings.HasPrefix(bucketID, advance.String()):
		return advance
	default:
		return unknown
	}
}

var (
	tableName = getEnv("TABLE_NAME", "buckets_table")
	buckets   = map[usage]*limiter.TokenBucket{
		free:    limiter.NewTokenBucket(6, 12),
		usual:   limiter.NewTokenBucket(12, 24),
		advance: limiter.NewTokenBucket(12, 24),
		unknown: limiter.NewTokenBucket(1, 1),
	}
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	id := "bbbb-test-api-key"
	usage := detectUsage(id)
	bucket := buckets[usage]
	fmt.Printf("%#v\n", bucket)
	r := limiter.New(tableName,
		bucket,
		client,
	)

	if len(os.Args) > 1 && os.Args[1] == "init" {
		if err = r.PrepareTokens(ctx, id); err != nil {
			panic(err)
		}
		fmt.Println("prepare tokens")
		return
	}

	ng, err := r.ShouldThrottle(ctx, id)
	if err != nil {
		panic(err)
	}
	if ng {
		fmt.Println("throttle")
	} else {
		fmt.Println("ok")
	}
}
