package limitter

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

type TokenBucket struct {
	numofShards int64
	baseTokens  []int64
	burstTokens []int64
	config      *tokenBucketConfig
}

type tokenBucketConfig struct {
	interval time.Duration
}

type Option func(t *tokenBucketConfig)

func WithInterval(interval time.Duration) Option {
	return func(c *tokenBucketConfig) {
		c.interval = interval
	}
}

func NewTokenBucket(rateLimit, burstSize int64, opts ...Option) *TokenBucket {
	config := &tokenBucketConfig{
		interval: 60 * time.Second,
	}
	for _, opt := range opts {
		opt(config)
	}

	if rateLimit == 0 {
		return &TokenBucket{
			numofShards: 0,
			baseTokens:  []int64{},
			burstTokens: []int64{},
			config:      config,
		}
	}

	if rateLimit*2 > burstSize {
		burstSize = rateLimit * 2
	}

	maxRate := 500 * config.interval.Seconds()
	numofShards := int64(math.Ceil(float64(burstSize) / maxRate))
	baseTokens := distribute(rateLimit, numofShards)
	burstTokens := distribute(burstSize, numofShards)
	b := &TokenBucket{
		numofShards: numofShards,
		baseTokens:  baseTokens,
		burstTokens: burstTokens,
		config:      config,
	}
	return b
}

func (b *TokenBucket) makeShards() []int64 {
	shardIDs := make([]int64, b.numofShards)
	for i := int64(0); i < b.numofShards; i++ {
		shardIDs[i] = i
	}
	return shardIDs
}

func (b *TokenBucket) now() int64 {
	return time.Now().Unix() / int64(b.config.interval.Seconds())
}

func distribute(token, numofShard int64) []int64 {
	base := token / numofShard
	extra := token % numofShard
	shards := make([]int64, 0, numofShard)
	for i := int64(0); i < numofShard; i++ {
		add := int64(0)
		if i < extra {
			add = 1
		}
		shards = append(shards, base+add)
	}
	return shards
}

type RateLimit struct {
	client    *dynamodb.Client
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

func New(table string, bucket *TokenBucket, client *dynamodb.Client) *RateLimit {
	l := &RateLimit{
		client:    client,
		bucket:    bucket,
		tableName: table,
	}
	return l
}

func pickIndex(min int) int {
	rand.Seed(time.Now().UnixNano())
	i := rand.Intn(min) //nolint:gosec,gocritic
	return i
}

func (l *RateLimit) ShouldThrottle(ctx context.Context, bucketID string) (bool, error) {
	shardIDs := l.bucket.makeShards()
	// bucket size is zero
	if len(shardIDs) == 0 || l.bucket.numofShards == 0 {
		return true, nil
	}

	i := pickIndex(len(shardIDs))
	shardID := shardIDs[i]
	return l.shouldThrottle(ctx, bucketID, shardID)
}

func (l *RateLimit) shouldThrottle(ctx context.Context, bucketID string, shardID int64) (bool, error) {
	token, err := l.getToken(ctx, bucketID, shardID)
	if err != nil {
		var le *types.LimitExceededException
		if errors.As(err, &le) {
			return true, nil
		}
		return true, err
	}
	throttle := token <= 0
	return throttle, nil
}

func (l *RateLimit) getToken(ctx context.Context, bucketID string, shardID int64) (int64, error) {
	item, err := l.getItem(ctx, bucketID, shardID)
	if err != nil {
		return 0, nil
	}
	now := l.bucket.now()
	token := item.TokenCount
	var retErr error
	if now > item.LastUpdated {
		refilTokenCount := l.calculateRefilToken(item, now)
		// store subtracted token as a token will be used for get-token
		_, retErr = l.refilToken(ctx, bucketID, shardID, item.ShardBurstSize, refilTokenCount-1, now)
		if retErr == nil {
			// available token are current token count + refil token count
			return token + refilTokenCount, nil
		}
	} else if token > 0 {
		_, retErr = l.subtractToken(ctx, bucketID, shardID, now)
	}
	var chf *types.ConditionalCheckFailedException
	var le *types.LimitExceededException
	if errors.As(retErr, &chf) || errors.As(retErr, &le) {
		return token, nil
	}
	return token, err
}

func (l *RateLimit) getItem(ctx context.Context, bucketID string, shardID int64) (*ddbItem, error) {
	input := &dynamodb.GetItemInput{
		Key:       buildKey(bucketID, shardID),
		TableName: &l.tableName,
	}
	resp, err := l.client.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get-item: %w", err)
	}
	// check registered token or not
	if len(resp.Item) == 0 {
		return nil, fmt.Errorf("invalid token %s", bucketID)
	}

	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Item, item); err != nil {
		return nil, err
	}

	return item, nil
}

func (l *RateLimit) calculateRefilToken(item *ddbItem, now int64) int64 {
	refil := l.bucket.baseTokens[item.BucketShardID] * (now - item.LastUpdated)
	burstable := item.ShardBurstSize - item.TokenCount
	if refil > burstable {
		return burstable
	}
	return refil
}

func (l *RateLimit) refilToken(ctx context.Context, bucketID string, shardID, shardBurstSize, refilTokenCount, now int64) (int64, error) {
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
			expression.Name("token_count"), expression.Value(refilTokenCount),
		)
	expr, err := expression.NewBuilder().WithCondition(condExpr).WithUpdate(updateExpr).Build()
	if err != nil {
		return 0, fmt.Errorf("failed to build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 &l.tableName,
		Key:                       buildKey(bucketID, shardID),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	}
	resp, err := l.client.UpdateItem(ctx, input)
	if err != nil {
		return 0, err
	}
	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Attributes, item); err != nil {
		return 0, err
	}
	return item.TokenCount, nil
}

func (l *RateLimit) subtractToken(ctx context.Context, bucketID string, shardID, now int64) (int64, error) {
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
		return 0, fmt.Errorf("failed to build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		Key:                       buildKey(bucketID, shardID),
		TableName:                 &l.tableName,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	}
	resp, err := l.client.UpdateItem(ctx, input)
	if err != nil {
		// ConditionalCheckFailedException will occur when token_count equals zero
		// no handling the error
		return 0, err
	}
	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Attributes, item); err != nil {
		return 0, err
	}
	return item.TokenCount, nil
}

func (l *RateLimit) PrepareTokens(ctx context.Context, bucketID string) error {
	shards := l.bucket.makeShards()
	now := l.bucket.now()
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
			return err
		}
		req := types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: attrs,
			},
		}
		requests = append(requests, req)
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			l.tableName: requests,
		},
	}
	_, err := l.client.BatchWriteItem(ctx, input)
	return err
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
