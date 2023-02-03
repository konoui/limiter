package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var (
	ddbClient *dynamodb.Client
)

func newDDBClient(ctx context.Context) (*dynamodb.Client, error) {
	if ddbClient != nil {
		return ddbClient, nil
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(cfg)
	return client, nil
}

func newCWLClient(ctx context.Context) (*cloudwatchlogs.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	client := cloudwatchlogs.NewFromConfig(cfg)
	return client, nil
}

func publishMetric(ctx context.Context, client *cloudwatchlogs.Client, input *cloudwatchlogs.PutLogEventsInput) error {
	_, err := client.PutLogEvents(ctx, input, func(o *cloudwatchlogs.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.SetHeaderValue("x-amzn-logs-format", "json/emf"))
	})
	return err
}

func ReadAndPublish(ctx context.Context, in io.Reader, c *cloudwatchlogs.Client, logGroup, stream string) error {
	d, err := io.ReadAll(in)
	if err != nil {
		return err
	}

	v := map[string]any{}
	if err := json.Unmarshal(d, &v); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}

	msg := string(d)
	timestamp := time.Now().UnixMilli()
	err = publishMetric(ctx, c, &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &logGroup,
		LogStreamName: &stream,
		LogEvents: []types.InputLogEvent{
			{
				Message:   &msg,
				Timestamp: &timestamp,
			},
		},
	})
	return err
}
