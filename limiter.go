package limiter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/exp/slog"
)

var (
	// TimeNow is global variable for mock
	timeNow                           = time.Now
	uuidNewRandom                     = uuid.NewRandom
	_                   LimitPreparer = &RateLimit{}
	attrBucketIDShardID               = "bucket_id_shard_id"
	attrTTL                           = "ttl"
	attrBucketPerShard                = "bucket_size_per_shard"
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
	client     DDBClient
	bucket     *TokenBucket
	tableName  string
	metricOut  io.Writer
	anonymous  bool
	ttl        time.Duration
	ncache     *lru.Cache[string, interface{}]
	maxLength  int
	baseLogger *slog.Logger
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

func WithLogger(logger *slog.Logger) Opt {
	return func(rl *RateLimit) {
		rl.baseLogger = logger
	}
}

func withAnonymous(ttl time.Duration) Opt {
	return func(rl *RateLimit) {
		rl.anonymous = true
		rl.ttl = ttl
	}
}

func withNegativeCache(size int) Opt {
	return func(rl *RateLimit) {
		rl.ncache.Resize(size)
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

	addOpts, err := cfgToOpts(cfg.AdditionalConfig)
	if err != nil {
		return nil, err
	}

	addOpts = append(addOpts, opts...)
	l := newLimiter(cfg.TableName, bucket, client, addOpts...)
	return l, nil
}

func newLimiter(table string, bucket *TokenBucket, client DDBClient, opts ...Opt) *RateLimit {
	l := &RateLimit{
		bucket:    bucket,
		tableName: table,
		metricOut: io.Discard,
		ncache:    defaultCache,
		maxLength: 512, // a WCU is 1kib basis
		// disabled by default
		baseLogger: slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(-1)})),
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

// ShouldThrottle return throttled and an error. If throttle is true, it means tokens run out.
// If an error is ErrRateLimitExceeded, DynamoDB API rate limit exceeded.
func (l *RateLimit) ShouldThrottle(ctx context.Context, bucketID string) (bool, error) {
	if bucketID == "" {
		return true, fmt.Errorf("empty bucket id: %w", ErrInvalidBucketID)
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

// TODO handle bucket id and shard id for structured logging
func (l *RateLimit) shouldThrottle(ctx context.Context, bucketID string, shardID int64) (throttled bool, err error) {
	logger := l.baseLogger.With(
		slog.String("table_name", l.tableName),
		slog.String("bucket_id", bucketID),
		slog.Int64("shard_id", shardID),
		slog.Int("negative_cache_entries", l.ncache.Len()),
	)
	defer func() { logger.DebugCtx(ctx, fmt.Sprintf("result throttled: %v", throttled)) }()

	// first, validate bucket id
	if l.maxLength < len(bucketID) {
		return true, fmt.Errorf("exceeded maximum bucket id length %d: %w", l.maxLength, ErrInvalidBucketID)
	}

	// second search from negative cache
	if bucketID != "" && l.ncache.Contains(bucketID) {
		return true, fmt.Errorf("found %s in negative cache: %w", bucketID, ErrInvalidBucketID)
	}

	token, err := l.getToken(ctx, logger, bucketID, shardID)
	throttle := token <= 0
	defer func() {
		if err != nil {
			// add context to an error
			err = fmt.Errorf("bucketID %s, shardID : %d: %w", bucketID, shardID, err)
		}
	}()

	// add negative cache and early return
	if errors.Is(err, ErrInvalidBucketID) {
		logger.DebugCtx(ctx, "adding bucket_id into the negative cache")
		l.ncache.Add(bucketID, nil)
		return true, err
	}

	// other throttle will be caught here to record metrics
	if throttle {
		outputLog(l.metricOut, buildThrottleMetric(l.tableName, bucketID, int64String(shardID)))
	}

	// ignore ConditionalCheckFailed
	if isErrConditionalCheckFailed(err) {
		return throttle, nil
	}

	return throttle, err
}

// getToken return available tokens. it will tell dynamodb errors to the caller.
func (l *RateLimit) getToken(ctx context.Context, logger *slog.Logger, bucketID string, shardID int64) (count int64, err error) {
	item, err := l.getItem(ctx, logger, bucketID, shardID)
	if err != nil {
		return 0, err
	}

	defer func() {
		if isErrLimitExceeded(err) || errors.Is(err, ErrRateLimitExceeded) {
			logger.WarnCtx(ctx, fmt.Sprintf("encountered DynamoDB API limits: %v", err))
			count = 0
		}
	}()

	now := timeNow().UnixMilli()
	token := item.TokenCount
	refillTokenCount := l.calculateRefillToken(item, now)
	logger.DebugCtx(ctx, fmt.Sprintf("calculated refilled token: %d", refillTokenCount))
	switch {
	case refillTokenCount > 0:
		// store subtracted token as a token will be used for get-token
		err := l.refillToken(ctx, logger, bucketID, shardID, item.BucketSizePerShard, refillTokenCount-1, item.LastUpdated, now)
		if err != nil {
			if isErrConditionalCheckFailed(err) {
				// fallback to subtract, when succeeded, available one token at least.
				logger.DebugCtx(ctx, "refill-token failed due to conditional check failed. fallback to subtract-token")
				if err := l.subtractToken(ctx, logger, bucketID, shardID, now); err != nil {
					if isErrConditionalCheckFailed(err) {
						// if ConditionalCheckFailedException, it means tokens run out by other request.
						logger.DebugCtx(ctx, "available token is zero as subtract-token for fallback failed due to conditional check failed.")
						return 0, err
					}
					// an internal error at subtractToken
					logger.ErrorCtx(ctx, fmt.Sprintf("subtract-token encountered unexpected error: %v", err))
					return 0, errors.Join(err, ErrInternal)
				}
				// subtractToken successfully
				logger.DebugCtx(ctx, "subtract-token for fallback succeeded")
				return 1, err
			}
			// an internal error at refillToken
			logger.ErrorCtx(ctx, fmt.Sprintf("refill-token encountered unexpected error: %v", err))
			return 0, errors.Join(err, ErrInternal)
		}
		// available token are current token count + refill token count
		logger.DebugCtx(ctx, "refill-token for fallback succeeded")
		return token + refillTokenCount, nil
	case token > 0:
		err := l.subtractToken(ctx, logger, bucketID, shardID, now)
		if err != nil {
			if isErrConditionalCheckFailed(err) {
				logger.DebugCtx(ctx, "tokens run out. subtract-token failed due to conditional check failed.")
				// if ConditionalCheckFailedException, it means tokens run out by other request.
				return 0, err
			}
			// an internal error at subtractToken
			logger.ErrorCtx(ctx, fmt.Sprintf("subtract-token encountered unexpected error: %v", err))
			return 0, errors.Join(err, ErrInternal)
		}
		logger.DebugCtx(ctx, "subtract-token succeeded")
		return 1, nil
	default:
		logger.DebugCtx(ctx, "tokens run out")
		return 0, nil
	}
}

func (l *RateLimit) getItem(ctx context.Context, logger *slog.Logger, bucketID string, shardID int64) (_ *ddbItem, retErr error) {
	input := &dynamodb.GetItemInput{
		Key:       buildPartitionKey(bucketID, shardID),
		TableName: &l.tableName,
	}
	resp, err := l.client.GetItem(ctx, input)
	if err != nil {
		logger.ErrorCtx(ctx, fmt.Sprintf("get-item failed: %v", err))
		return nil, fmt.Errorf("get-item: %w", err)
	}

	// TODO
	// add dynamodb context
	requestID, ok := middleware.GetRequestIDMetadata(resp.ResultMetadata)
	if ok {
		*logger = *logger.With(slog.String("ddb_get_item_request_id", requestID))
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
			logger.DebugCtx(ctx, "created ddb item for anonymous mode", slog.Any("ddb_item", i))
			return i, nil
		}
		logger.DebugCtx(ctx, "unregistered bucket id")
		return nil, fmt.Errorf("unregistered bucket id: %w", ErrInvalidBucketID)
	}

	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(resp.Item, item); err != nil {
		return nil, err
	}

	// add internal value
	item.bucketID, item.shardID = bucketID, shardID
	logger.DebugCtx(ctx, "got item", slog.Any("ddb_item", item))
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
func (l *RateLimit) refillToken(ctx context.Context, _ *slog.Logger,
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
	updateExpr = l.updateTTLExpr(updateExpr, shardID, now)

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

	_, err = l.client.UpdateItem(ctx, input)
	if err != nil {
		// TODO return custom error if CCF
		return fmt.Errorf("refill-token: %w", err)
	}

	return nil
}

// subtractToken if return an error of ConditionalCheckFailedException, this means token has been zero by other request.
func (l *RateLimit) subtractToken(ctx context.Context, _ *slog.Logger, bucketID string, shardID, now int64) error {
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
	updateExpr = l.updateTTLExpr(updateExpr, shardID, now)

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

	_, err = l.client.UpdateItem(ctx, input)
	if err != nil {
		// TODO return custom error if CCF
		return fmt.Errorf("subtract-item: %w", err)
	}

	return nil
}

func (l *RateLimit) updateTTLExpr(expr expression.UpdateBuilder, shardID int64, now int64) expression.UpdateBuilder {
	if l.ttl > 0 {
		expr = expr.Set(
			expression.Name(attrTTL),
			// second unix timestamp
			expression.Value(time.UnixMilli(now).Unix()+int64(l.ttl.Seconds())),
		).Set(
			expression.Name(attrBucketPerShard),
			expression.Value(l.bucket.bucketSizePerShard[shardID]),
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

func buildPartitionKey(bucketID string, shardID int64) map[string]types.AttributeValue {
	bucketIDShardID := makePartitionKey(bucketID, shardID)
	return map[string]types.AttributeValue{
		attrBucketIDShardID: &types.AttributeValueMemberS{
			Value: bucketIDShardID,
		},
	}
}

func makePartitionKey(bucketID string, shardID int64) string {
	return fmt.Sprintf("%s%s%d", bucketID, delimiter, shardID)
}

func splitPartitionKey(bucketIDShardID string) (bucketID, shardID string, _ error) {
	idx := strings.LastIndex(bucketIDShardID, delimiter)
	invalidErr := fmt.Errorf("unexpected partition key: %s", bucketIDShardID)
	if idx <= 0 {
		return "", "", invalidErr

	}

	bucketID = bucketIDShardID[:idx]
	shardID = bucketIDShardID[idx+1:]
	if bucketID == "" || shardID == "" {
		return "", "", invalidErr
	}

	return bucketIDShardID[:idx], bucketIDShardID[idx+1:], nil
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
