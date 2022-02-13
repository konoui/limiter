package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/konoui/limitter"
)

var r *limitter.RateLimit

func init() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	bucket := limitter.NewTokenBucket(10, 20)
	fmt.Printf("bucket %#v\n", bucket)
	r = limitter.New("buckets_table",
		bucket,
		client,
	)
}

func handler(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	if request.RequestContext.HTTP.Method == "POST" {
		id := "new-bucket"
		if err := r.PrepareTokens(ctx, id); err != nil {
			return events.APIGatewayV2HTTPResponse{}, err
		}
		return events.APIGatewayV2HTTPResponse{
			Body:       fmt.Sprintf("created %s", id),
			StatusCode: 200,
		}, nil
	}

	token, ok := request.Headers["X-Api-Token"]
	if !ok {
		return events.APIGatewayV2HTTPResponse{
			Body:       fmt.Sprintf("X-Api-Token header: is empty"),
			StatusCode: 400,
		}, nil
	}
	throttle, err := r.ShouldThrottle(ctx, token)
	if err != nil {
		return events.APIGatewayV2HTTPResponse{}, err
	}

	if throttle {
		return events.APIGatewayV2HTTPResponse{
			Body:       fmt.Sprintf("throttle"),
			StatusCode: 429,
		}, nil
	}

	return events.APIGatewayV2HTTPResponse{
		Body:       fmt.Sprintf("hello"),
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(handler)
}
