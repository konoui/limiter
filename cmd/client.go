package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var (
	client *dynamodb.Client
)

func newDDBClient(ctx context.Context) (*dynamodb.Client, error) {
	if client != nil {
		return client, nil
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(cfg)
	return client, nil
}
