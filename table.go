package limiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	attrBucketIDShardID = "bucket_id_shard_id"
	attrTAT             = "tat"
	attrTTL             = "ttl"
)

const delimiter = "#"

// ddbItem is a GCRA state of a shard.
type ddbItem struct {
	BucketIDShardID string `dynamodbav:"bucket_id_shard_id"`
	// TAT is the theoretical arrival time in unix nanoseconds
	TAT int64 `dynamodbav:"tat"`
	// TTL is the expiry in unix seconds. Only set in anonymous mode.
	TTL int64 `dynamodbav:"ttl,omitempty"`
}

func buildPartitionKey(bucketID string, shardID int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrBucketIDShardID: &types.AttributeValueMemberS{
			Value: makePartitionKey(bucketID, shardID),
		},
	}
}

func makePartitionKey(bucketID string, shardID int) string {
	return bucketID + delimiter + strconv.Itoa(shardID)
}

// CreateTable creates a table for the limiter and enables TTL. It does nothing if the table exists.
func CreateTable(ctx context.Context, tableName string, client *dynamodb.Client) error {
	input := &dynamodb.CreateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: &attrBucketIDShardID,
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: &attrBucketIDShardID,
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	}

	_, err := client.CreateTable(ctx, input)
	var ae *types.TableAlreadyExistsException
	var ie *types.ResourceInUseException
	if errors.As(err, &ae) || errors.As(err, &ie) {
		return nil
	} else if err != nil {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 1*time.Minute)
	if err != nil {
		return fmt.Errorf("wait for table %s: %w", tableName, err)
	}

	ttlInput := &dynamodb.UpdateTimeToLiveInput{
		TableName: &tableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: &attrTTL,
		},
	}
	if _, err := client.UpdateTimeToLive(ctx, ttlInput); err != nil {
		return fmt.Errorf("enable ttl on %s: %w", tableName, err)
	}
	return nil
}
