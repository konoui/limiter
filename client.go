package limiter

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

//go:generate mockgen -source=$GOFILE -destination=mock_$GOPACKAGE/$GOFILE -package=mock_$GOPACKAGE
type DDBClient interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

type wrappedDDBClient struct {
	c DDBClient
}

func newWrapDDBClient(client DDBClient) DDBClient {
	return &wrappedDDBClient{c: client}
}

func (c *wrappedDDBClient) UpdateItem(ctx context.Context,
	input *dynamodb.UpdateItemInput,
	params ...func(*dynamodb.Options)) (_ *dynamodb.UpdateItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
		}
	}()
	return c.c.UpdateItem(ctx, input, params...)
}

func (c *wrappedDDBClient) GetItem(ctx context.Context,
	input *dynamodb.GetItemInput,
	params ...func(*dynamodb.Options)) (_ *dynamodb.GetItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
		}
	}()
	return c.c.GetItem(ctx, input, params...)
}

func (c *wrappedDDBClient) BatchWriteItem(ctx context.Context,
	input *dynamodb.BatchWriteItemInput,
	params ...func(*dynamodb.Options)) (_ *dynamodb.BatchWriteItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
		}
	}()
	return c.c.BatchWriteItem(ctx, input, params...)
}
