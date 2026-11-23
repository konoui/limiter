package limiter

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	smithymiddleware "github.com/aws/smithy-go/middleware"
)

// DDBClient is the subset of *dynamodb.Client used by the limiter.
//
//go:generate go tool mockgen -source=$GOFILE -destination=mock_ddb_client_test.go -package=$GOPACKAGE
type DDBClient interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// wrappedDDBClient classifies DynamoDB throttling errors and records them.
type wrappedDDBClient struct {
	c       DDBClient
	metrics *metrics
}

func (c *wrappedDDBClient) GetItem(ctx context.Context, logger *slog.Logger,
	input *dynamodb.GetItemInput, params ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	resp, err := c.c.GetItem(ctx, input, params...)
	if err != nil {
		return nil, c.classify(ctx, "GetItem", err)
	}
	debugResponse(ctx, logger, "get-item", resp.ResultMetadata, resp.ConsumedCapacity)
	return resp, nil
}

func (c *wrappedDDBClient) UpdateItem(ctx context.Context, logger *slog.Logger,
	input *dynamodb.UpdateItemInput, params ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	resp, err := c.c.UpdateItem(ctx, input, params...)
	if err != nil {
		return nil, c.classify(ctx, "UpdateItem", err)
	}
	debugResponse(ctx, logger, "update-item", resp.ResultMetadata, resp.ConsumedCapacity)
	return resp, nil
}

func (c *wrappedDDBClient) BatchWriteItem(ctx context.Context, logger *slog.Logger,
	input *dynamodb.BatchWriteItemInput, params ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	resp, err := c.c.BatchWriteItem(ctx, input, params...)
	if err != nil {
		return nil, c.classify(ctx, "BatchWriteItem", err)
	}
	debugResponse(ctx, logger, "batch-write-item", resp.ResultMetadata, resp.ConsumedCapacity)
	return resp, nil
}

func (c *wrappedDDBClient) DeleteItem(ctx context.Context, logger *slog.Logger,
	input *dynamodb.DeleteItemInput, params ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	resp, err := c.c.DeleteItem(ctx, input, params...)
	if err != nil {
		return nil, c.classify(ctx, "DeleteItem", err)
	}
	debugResponse(ctx, logger, "delete-item", resp.ResultMetadata, resp.ConsumedCapacity)
	return resp, nil
}

func (c *wrappedDDBClient) classify(ctx context.Context, operation string, err error) error {
	if !isErrLimitExceeded(err) {
		return err
	}
	c.metrics.recordStoreThrottle(ctx, operation)
	return fmt.Errorf("%w: %w", ErrStoreThrottled, err)
}

func debugResponse(ctx context.Context, logger *slog.Logger, op string, md smithymiddleware.Metadata, consumed any) {
	requestID, _ := middleware.GetRequestIDMetadata(md)
	logger.DebugContext(ctx, op+" response",
		slog.String("ddb_request_id", requestID),
		slog.Any("consumed_capacity", consumed))
}
