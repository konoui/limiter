package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	// TimeNow is global variable for mock
	TimeNow               = time.Now
	_       LimitPreparer = &RateLimit{}
)

//go:generate mockgen -source=$GOFILE -destination=mock_$GOPACKAGE/$GOFILE -package=mock_$GOPACKAGE
type LimitPreparer interface {
	Limiter
	Preparer
}

type Limiter interface {
	ShouldThrottle(context.Context, string) (bool, error)
}

type Preparer interface {
	PrepareTokens(context.Context, string) error
}

type RateLimit struct {
	client    DDBClient
	bucket    *TokenBucket
	tableName string
}

type ddbItem struct {
	BucketID       string `dynamodbav:"bucket_id" json:"bucket_id"`
	BucketShardID  int64  `dynamodbav:"bucket_shard_id" json:"bucket_shard_id"`
	TokenCount     int64  `dynamodbav:"token_count" json:"token_count"`
	LastUpdated    int64  `dynamodbav:"last_updated" json:"last_updated"`
	ShardBurstSize int64  `dynamodbav:"shard_burst_size" json:"shard_burst_size"`
}

func New(table string, bucket *TokenBucket, client DDBClient) *RateLimit {
	l := &RateLimit{
		client:    newWrapDDBClient(client),
		bucket:    bucket,
		tableName: table,
	}
	return l
}

func pickIndex(min int) int {
	i := rand.Intn(min) //nolint:gosec,gocritic
	return i
}

// ShouldThrottle return throttle and error. If throttle is true, it means tokens run out.
// If an error is ErrRateLimitExceeded, DynamoDB API rate limit exceeded.
func (l *RateLimit) ShouldThrottle(ctx context.Context, bucketID string) (bool, error) {
	shardIDs := l.bucket.makeShards()
	// bucket size is zero
	if len(shardIDs) == 0 || l.bucket.numOfShards == 0 {
		return true, nil
	}

	i := pickIndex(len(shardIDs))
	shardID := shardIDs[i]
	return l.shouldThrottle(ctx, bucketID, shardID)
}

func (l *RateLimit) shouldThrottle(ctx context.Context, bucketID string, shardID int64) (bool, error) {
	token, err := l.getToken(ctx, bucketID, shardID)
	throttle := token <= 0
	// ignore ConditionalCheckFailed
	if isErrConditionalCheckFailed(err) {
		return throttle, nil
	}
	return throttle, err
}

// getToken return available tokens. it will tell dynamodb errors to caller.
func (l *RateLimit) getToken(ctx context.Context, bucketID string, shardID int64) (int64, error) {
	item, err := l.getItem(ctx, bucketID, shardID)
	if err != nil {
		return 0, err
	}
	now := TimeNow().Unix()
	token := item.TokenCount
	refillTokenCount := l.calculateRefillToken(item, now)
	switch {
	case refillTokenCount > 0:
		// store subtracted token as a token will be used for get-token
		err := l.refillToken(ctx, bucketID, shardID, item.ShardBurstSize, refillTokenCount-1, now)
		// available token are current token count + refill token count
		// TODO if error occurs, return token or token + refillTokenCount
		return token + refillTokenCount, err
	case token > 0:
		err := l.subtractToken(ctx, bucketID, shardID, now)
		return token, err
	default:
		return 0, nil
	}
}

func (l *RateLimit) getItem(ctx context.Context, bucketID string, shardID int64) (*ddbItem, error) {
	input := &dynamodb.GetItemInput{
		Key:       buildKey(bucketID, shardID),
		TableName: &l.tableName,
	}
	resp, err := l.client.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get-item: %w", err)
	}
	// check registered token or not
	if len(resp.Item) == 0 {
		return nil, fmt.Errorf("bucket ID: %s: %w", bucketID, ErrInvalidBucketID)
	}

	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Item, item); err != nil {
		return nil, err
	}

	return item, nil
}

func (l *RateLimit) calculateRefillToken(item *ddbItem, now int64) int64 {
	num := math.Floor(float64((now - item.LastUpdated)) / l.bucket.config.interval.Seconds())
	refill := l.bucket.baseTokens[item.BucketShardID] * int64(num)
	burstable := item.ShardBurstSize - item.TokenCount
	if refill > burstable {
		return burstable
	}
	return refill
}

func (l *RateLimit) refillToken(ctx context.Context,
	bucketID string, shardID, shardBurstSize, refillTokenCount, now int64) error {
	condNotExist := expression.Name("bucket_id").AttributeNotExists()
	condUpdated := expression.Name("last_updated").
		LessThan(
			expression.Value(now)).
		And(
			expression.ConditionBuilder(expression.Name("token_count").
				LessThan(
					expression.Value(shardBurstSize),
				)),
		)
	condExpr := condNotExist.Or(expression.ConditionBuilder(condUpdated))
	updateExpr := expression.
		Set(
			expression.Name("last_updated"), expression.Value(now)).
		Add(
			expression.Name("token_count"), expression.Value(refillTokenCount),
		)
	expr, err := expression.NewBuilder().WithCondition(condExpr).WithUpdate(updateExpr).Build()
	if err != nil {
		return fmt.Errorf("refill build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 &l.tableName,
		Key:                       buildKey(bucketID, shardID),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueNone,
	}

	if _, err := l.client.UpdateItem(ctx, input); err != nil {
		return fmt.Errorf("refill-token: %w", err)
	}
	return nil
}

func (l *RateLimit) subtractToken(ctx context.Context, bucketID string, shardID, now int64) error {
	// "token_count > :min_val"
	condExpr := expression.Name("token_count").GreaterThan(expression.Value(0))
	// "SET last_updated = :now ADD token_count :mod"
	updateExpr := expression.Set(
		expression.Name("last_updated"),
		expression.Value(now)).
		Add(
			expression.Name("token_count"),
			expression.Value(-1),
		)
	expr, err := expression.NewBuilder().WithCondition(condExpr).WithUpdate(updateExpr).Build()
	if err != nil {
		return fmt.Errorf("subtract build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		Key:                       buildKey(bucketID, shardID),
		TableName:                 &l.tableName,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueNone,
	}

	if _, err := l.client.UpdateItem(ctx, input); err != nil {
		// ConditionalCheckFailedException will occur when token_count equals to zero.
		// No handling the error here
		return fmt.Errorf("subtract-item: %w", err)
	}
	return nil
}

func (l *RateLimit) PrepareTokens(ctx context.Context, bucketID string) (err error) {
	shards := l.bucket.makeShards()
	now := TimeNow().Unix()
	batchSize := 25
	for i := 0; i < len(shards); i += batchSize {
		if len(shards) < batchSize+i {
			return l.prepareTokens(ctx, bucketID, now, shards[i:])
		}
		if err := l.prepareTokens(ctx, bucketID, now, shards[i:i+batchSize]); err != nil {
			return err
		}
	}
	return nil
}

func (l *RateLimit) prepareTokens(ctx context.Context, bucketID string, now int64, shards []int64) error {
	requests := make([]types.WriteRequest, 0, len(shards))
	for _, shardID := range shards {
		item := &ddbItem{
			BucketID:       bucketID,
			BucketShardID:  shardID,
			LastUpdated:    now,
			TokenCount:     l.bucket.burstTokens[shardID],
			ShardBurstSize: l.bucket.burstTokens[shardID],
		}
		attrs, err := attributevalue.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("prepare build: %w", err)
		}
		req := types.WriteRequest{
			PutRequest: &types.PutRequest{Item: attrs},
		}
		requests = append(requests, req)
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			l.tableName: requests,
		},
	}

	_, err := l.client.BatchWriteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("prepare-tokens: %w", err)
	}
	return nil
}

func int64String(v int64) string {
	return strconv.FormatInt(v, 10)
}

func buildKey(bucketID string, shardID int64) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"bucket_id": &types.AttributeValueMemberS{
			Value: bucketID,
		},
		"bucket_shard_id": &types.AttributeValueMemberN{
			Value: int64String(shardID),
		},
	}
}

func CreateTable(ctx context.Context, tableName string, client *dynamodb.Client) error {
	bucketKey := "bucket_id"
	bucketShardID := "bucket_shard_id"
	input := &dynamodb.CreateTableInput{
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: &bucketKey,
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: &bucketShardID,
				KeyType:       types.KeyTypeRange,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: &bucketKey,
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: &bucketShardID,
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
	}
	_, err := client.CreateTable(ctx, input)
	var ae *types.TableAlreadyExistsException
	var ie *types.ResourceInUseException
	if errors.As(err, &ae) || errors.As(err, &ie) {
		return nil
	}
	return err
}