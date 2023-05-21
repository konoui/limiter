package limiter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

var (
	// TimeNow is global variable for mock
	timeNow                           = time.Now
	uuidNewRandom                     = uuid.NewRandom
	_                   LimitPreparer = &RateLimit{}
	attrBucketIDShardID               = "bucket_id_shard_id"
	attrTTL                           = "ttl"
)

const (
	delimiter = "#"
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
	client         DDBClient
	bucket         *TokenBucket
	tableName      string
	metricOut      io.Writer
	throttleIfFail bool
	anonymous      bool
	ttl            time.Duration
}

type ddbItem struct {
	BucketIDShardID string `dynamodbav:"bucket_id_shard_id" json:"bucket_id_shard_id"`
	TokenCount      int64  `dynamodbav:"token_count" json:"token_count"`
	// milliseconds unix timestamp
	LastUpdated        int64 `dynamodbav:"last_updated" json:"last_updated"`
	BucketSizePerShard int64 `dynamodbav:"bucket_size_per_shard" json:"bucket_size_per_shard"`
	TTL                int64 `dynamodbav:"ttl" json:"ttl"`
	shardID            int64
	bucketID           string
}

type Opt func(rl *RateLimit)

func WithEMFMetrics(w io.Writer) Opt {
	return func(rl *RateLimit) {
		rl.metricOut = w
	}
}

// WithAnonymous allows bucket id not registered to check throttled or not.
// This is used for such as an IP address basis throttling.
func WithAnonymous(ttl time.Duration) Opt {
	return func(rl *RateLimit) {
		rl.anonymous = true
		rl.ttl = ttl
	}
}

// WithThrottleIfFail decides throttled or not when DDB API return an unexpected error.
// If specified, DDB API receives an internal error, ShouldThrottle() return true.
func WithThrottleIfFail() Opt {
	return func(rl *RateLimit) {
		rl.throttleIfFail = true
	}
}

func New(cfg *Config, client DDBClient, opts ...Opt) (*RateLimit, error) {
	interval := DefaultInterval
	if cfg.Interval != 0 {
		interval = cfg.Interval
	}
	bucket, err := newTokenBucket(
		cfg.TokenPerInterval,
		cfg.BucketSize,
		interval)
	if err != nil {
		return nil, err
	}
	if cfg.TableName == "" {
		return nil, errors.New("table_name in config is empty")
	}
	l := newLimiter(cfg.TableName, bucket, client, opts...)
	return l, nil
}

func newLimiter(table string, bucket *TokenBucket, client DDBClient, opts ...Opt) *RateLimit {
	l := &RateLimit{
		bucket:    bucket,
		tableName: table,
		metricOut: io.Discard,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(l)
		}
	}

	l.client = newWrapDDBClient(client, l.metricOut)
	return l
}

func pickIndex(min int) int {
	i := rand.Intn(min) //nolint:gosec,gocritic
	return i
}

// ShouldThrottle return throttle and error. If throttle is true, it means tokens run out.
// If an error is ErrRateLimitExceeded, DynamoDB API rate limit exceeded.
func (l *RateLimit) ShouldThrottle(ctx context.Context, bucketID string) (bool, error) {
	if bucketID == "" {
		return true, ErrInvalidBucketID
	}

	// bucket size is zero
	shardIDs := l.bucket.makeShards()
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
	// Note ignore the invalid bucket id error
	// other throttle will be caught here
	if throttle && !errors.Is(err, ErrInvalidBucketID) {
		outputLog(l.metricOut, buildThrottleMetric(l.tableName, bucketID, int64String(shardID)))
	}

	// ignore ConditionalCheckFailed
	if isErrConditionalCheckFailed(err) {
		return throttle, nil
	}

	return throttle, err
}

// getToken return available tokens. it will tell dynamodb errors to the caller.
func (l *RateLimit) getToken(ctx context.Context, bucketID string, shardID int64) (count int64, err error) {
	item, err := l.getItem(ctx, bucketID, shardID)
	if err != nil {
		return 0, err
	}

	defer func() {
		if isErrLimitExceeded(err) || errors.Is(err, ErrRateLimitExceeded) {
			count = 0
		}
	}()

	now := timeNow().UnixMilli()
	token := item.TokenCount
	refillTokenCount := l.calculateRefillToken(item, now)
	switch {
	case refillTokenCount > 0:
		// store subtracted token as a token will be used for get-token
		err := l.refillToken(ctx, bucketID, shardID, item.BucketSizePerShard, refillTokenCount-1, item.LastUpdated, now)
		if err != nil {
			if isErrConditionalCheckFailed(err) {
				// fallback to subtract, when succeeded, available one token at least.
				if err := l.subtractToken(ctx, bucketID, shardID, now); err != nil {
					if isErrConditionalCheckFailed(err) {
						// if ConditionalCheckFailedException, it means tokens run out by other request.
						return 0, err
					}
					// an internal error at subtractToken
					return l.internalThrottle(token, err)
				}
				// subtractToken successfully
				return 1, err
			}
			// an internal error at refillToken
			return l.internalThrottle(token, err)
		}
		// available token are current token count + refill token count
		return token + refillTokenCount, nil
	case token > 0:
		err := l.subtractToken(ctx, bucketID, shardID, now)
		if isErrConditionalCheckFailed(err) {
			// if ConditionalCheckFailedException, it means tokens run out by other request.
			return 0, err
		}
		// an internal error at subtractToken
		return l.internalThrottle(token, err)
	default:
		return 0, nil
	}
}

func (l RateLimit) internalThrottle(cur int64, err error) (int64, error) {
	if l.throttleIfFail {
		return 0, err
	}
	return cur, err
}

func (l *RateLimit) getItem(ctx context.Context, bucketID string, shardID int64) (*ddbItem, error) {
	input := &dynamodb.GetItemInput{
		Key:       buildPartitionKey(bucketID, shardID),
		TableName: &l.tableName,
	}
	resp, err := l.client.GetItem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get-item: %w", err)
	}
	// check registered token or not
	if len(resp.Item) == 0 {
		if l.anonymous {
			// return temporary token as create operation is executed by refillToken().
			// avoid to call prepareTokens() API.
			i := &ddbItem{
				BucketIDShardID: makePartitionKey(bucketID, shardID),
				TokenCount:      0,
				LastUpdated: timeNow().
					Add(-l.bucket.interval * time.Duration(l.bucket.bucketSizePerShard[shardID])).
					UnixMilli(),
				BucketSizePerShard: l.bucket.bucketSizePerShard[shardID],
				bucketID:           bucketID,
				shardID:            shardID,
			}
			return i, nil
		}
		return nil, fmt.Errorf("bucket ID: %s: %w", bucketID, ErrInvalidBucketID)
	}

	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Item, item); err != nil {
		return nil, err
	}

	// add internal value
	item.bucketID, item.shardID = bucketID, shardID
	return item, nil
}

func (l *RateLimit) calculateRefillToken(item *ddbItem, now int64) int64 {
	num := math.Floor(float64((now - item.LastUpdated)) / float64(l.bucket.interval.Milliseconds()))
	if num < 0 {
		return 0
	}
	refill := l.bucket.tokensPerShardPerInterval[item.shardID] * int64(num)
	burstable := item.BucketSizePerShard - item.TokenCount
	if refill > burstable {
		return burstable
	}
	return refill
}

// refillToken if return an error of ConditionalCheckFailedException, this means other request has been accepted before this request
func (l *RateLimit) refillToken(ctx context.Context,
	bucketID string, shardID, shardBurstSize, refillTokenCount, lastUpdated, now int64) error {
	condNotExist := expression.Name("bucket_id").AttributeNotExists()
	// Check last_updated is equal to last_updated of got item to achieve strong write consistency.
	// Other refillToken request is accepted before this, it will cause ConditionalCheckFailedException.
	// It avoid to refill tokens unexpectedly.
	// last_update == lastUpdated of got item
	condUpdated := expression.Name("last_updated").
		Equal(expression.Value(lastUpdated)).
		And(
			// token_count < burst size
			expression.ConditionBuilder(expression.Name("token_count").
				LessThan(expression.Value(shardBurstSize))),
		)
	condExpr := condNotExist.Or(expression.ConditionBuilder(condUpdated))
	updateExpr := expression.
		Set(
			expression.Name("last_updated"), expression.Value(now)).
		Add(
			expression.Name("token_count"), expression.Value(refillTokenCount),
		)
	updateExpr = l.updateTTLExpr(updateExpr, now)

	expr, err := expression.NewBuilder().WithCondition(condExpr).WithUpdate(updateExpr).Build()
	if err != nil {
		return fmt.Errorf("refill build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 &l.tableName,
		Key:                       buildPartitionKey(bucketID, shardID),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueNone,
	}

	if _, err := l.client.UpdateItem(ctx, input); err != nil {
		// TODO return custom error if CCF
		return fmt.Errorf("refill-token: %w", err)
	}
	return nil
}

// subtractToken if return an error of ConditionalCheckFailedException, this means token has been zero by other request.
func (l *RateLimit) subtractToken(ctx context.Context, bucketID string, shardID, now int64) error {
	// if other request subtract token before this and token run out, ConditionalCheckFailedException will occur.
	// No handling the error here
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
	updateExpr = l.updateTTLExpr(updateExpr, now)

	expr, err := expression.NewBuilder().WithCondition(condExpr).WithUpdate(updateExpr).Build()
	if err != nil {
		return fmt.Errorf("subtract build: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		Key:                       buildPartitionKey(bucketID, shardID),
		TableName:                 &l.tableName,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueNone,
	}

	if _, err := l.client.UpdateItem(ctx, input); err != nil {
		// TODO return custom error if CCF
		return fmt.Errorf("subtract-item: %w", err)
	}
	return nil
}

func (l *RateLimit) updateTTLExpr(expr expression.UpdateBuilder, now int64) expression.UpdateBuilder {
	if l.ttl > 0 {
		expr.Set(
			expression.Name(attrTTL),
			// second unix timestamp
			expression.Value(time.UnixMilli(now).Unix()+int64(l.ttl.Seconds())),
		)
	}
	return expr
}

func (l *RateLimit) PrepareTokens(ctx context.Context, bucketID string) (err error) {
	shards := l.bucket.makeShards()
	now := timeNow().UnixMilli()
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
			BucketIDShardID:    makePartitionKey(bucketID, shardID),
			LastUpdated:        now,
			TokenCount:         l.bucket.bucketSizePerShard[shardID],
			BucketSizePerShard: l.bucket.bucketSizePerShard[shardID],
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

func makePartitionKey(bucketID string, shardID int64) string {
	return fmt.Sprintf("%s%s%d", bucketID, delimiter, shardID)
}

func buildPartitionKey(bucketID string, shardID int64) map[string]types.AttributeValue {
	bucketIDShardID := makePartitionKey(bucketID, shardID)
	return map[string]types.AttributeValue{
		attrBucketIDShardID: &types.AttributeValueMemberS{
			Value: bucketIDShardID,
		},
	}
}

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
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 1*time.Minute)
	if err != nil {
		return err
	}

	ttlInput := &dynamodb.UpdateTimeToLiveInput{
		TableName: &tableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: &attrTTL,
		},
	}

	_, err = client.UpdateTimeToLive(ctx, ttlInput)
	return err
}
