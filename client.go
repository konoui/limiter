package limiter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//go:generate mockgen -source=$GOFILE -destination=mock_$GOPACKAGE/$GOFILE -package=mock_$GOPACKAGE
type DDBClient interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

type wrappedDDBClient struct {
	c         DDBClient
	metricOut io.Writer
}

func newWrapDDBClient(client DDBClient, w io.Writer) *wrappedDDBClient {
	return &wrappedDDBClient{c: client, metricOut: w}
}

func (c *wrappedDDBClient) UpdateItem(ctx context.Context,
	input *dynamodb.UpdateItemInput,
	params ...func(*dynamodb.Options)) (_ *dynamodb.UpdateItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
			bid, sid, lerr := getKeys(input.Key)
			if lerr != nil {
				err = errors.Join(err, lerr)
				return
			}
			outputLog(c.metricOut, buildDDBThrottleMetric(*input.TableName, "UpdateItem", bid, sid))
		}
	}()
	return c.c.UpdateItem(ctx, input, params...)
}

func (c *wrappedDDBClient) GetItem(ctx context.Context,
	input *dynamodb.GetItemInput,
	params ...func(*dynamodb.Options)) (out *dynamodb.GetItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
			bid, sid, lerr := getKeys(input.Key)
			if lerr != nil {
				err = errors.Join(err, lerr)
				return
			}
			outputLog(c.metricOut, buildDDBThrottleMetric(*input.TableName, "GetItem", bid, sid))
		}
	}()
	return c.c.GetItem(ctx, input, params...)
}

func (c *wrappedDDBClient) BatchWriteItem(ctx context.Context,
	input *dynamodb.BatchWriteItemInput,
	params ...func(*dynamodb.Options)) (out *dynamodb.BatchWriteItemOutput, err error) {
	defer func() {
		if isErrLimitExceeded(err) {
			err = errors.Join(err, ErrRateLimitExceeded)
			for tableName, v := range out.UnprocessedItems {
				for _, attr := range v {
					if attr.PutRequest == nil || attr.PutRequest.Item == nil {
						return
					}
					bid, sid, lerr := getKeys(attr.PutRequest.Item)
					if lerr != nil {
						err = errors.Join(err, lerr)
						return
					}
					outputLog(c.metricOut, buildDDBThrottleMetric(tableName, "BatchWriteItem", bid, sid))
				}
			}
		}
	}()
	return c.c.BatchWriteItem(ctx, input, params...)
}

func getKeys(attr map[string]types.AttributeValue) (bucketID string, shardID string, err error) {
	item := new(ddbItem)
	err = attributevalue.UnmarshalMap(attr, item)
	if err != nil {
		return "", "", fmt.Errorf("handle input: %w", err)
	}
	return item.BucketID, int64String(item.BucketShardID), nil
}

type EMF struct {
	AWS       AWS    `json:"_aws"`
	TableName string `json:"TableName"`
	BucketID  string `json:"BucketID"`
	ShardID   string `json:"ShardID"`
	Operation string `json:"Operation,omitempty"`
	Throttle  int    `json:"Throttle,omitempty"`
	RequestID string `json:"requestId"`
}

type AWS struct {
	CloudWatchMetrics []CloudWatchMetrics
	Timestamp         int64 `json:"Timestamp"`
}

type CloudWatchMetrics struct {
	Namespace  string     `json:"Namespace"`
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []Metric   `json:"Metrics"`
}

type Metric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

var (
	throttleMetrics = CloudWatchMetrics{
		Namespace:  "RateLimit",
		Dimensions: [][]string{{"TableName", "BucketID", "ShardID"}},
		Metrics: []Metric{
			{
				Name: "Throttle",
				Unit: "Count",
			},
		},
	}
	ddbThrottleMetrics = CloudWatchMetrics{
		Namespace:  "RateLimit",
		Dimensions: [][]string{{"TableName", "BucketID", "ShardID", "Operation"}},
		Metrics: []Metric{
			{
				Name: "Throttle",
				Unit: "Count",
			},
		},
	}
)

func buildDDBThrottleMetric(tableName string, operation string, bucketID string, shardID string) *EMF {
	emf := EMF{
		AWS: AWS{
			CloudWatchMetrics: []CloudWatchMetrics{
				ddbThrottleMetrics,
			},
		},
		TableName: tableName,
		BucketID:  bucketID,
		ShardID:   shardID,
		Operation: operation,
		Throttle:  1,
	}
	return &emf
}

func buildThrottleMetric(tableName string, bucketID string, shardID string) *EMF {
	emf := EMF{
		AWS: AWS{
			CloudWatchMetrics: []CloudWatchMetrics{
				throttleMetrics,
			},
		},
		TableName: tableName,
		BucketID:  bucketID,
		ShardID:   shardID,
		Throttle:  1,
	}
	return &emf
}

func outputLog(w io.Writer, emf *EMF) {
	if emf == nil {
		return
	}
	id, err := uuidNewRandom()
	if err != nil {
		return
	}
	emf.AWS.Timestamp = timeNow().UnixMilli()
	emf.RequestID = id.String()
	d, err := json.Marshal(emf)
	if err != nil {
		return
	}
	_, _ = w.Write(d)
}
