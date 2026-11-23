package limiter

import (
	"context"
	"errors"
	"fmt"

	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.opentelemetry.io/otel/metric"
)

var (
	// for tests
	timeNow = time.Now
	// shardOrder returns shard ids in the order to try
	shardOrder = rand.Perm //nolint:gosec // shard selection is not security sensitive

	_ LimitPreparer = &RateLimit{}
	_ Revoker       = &RateLimit{}
)

const (
	// maxAttempts bounds retries caused by concurrent updates of the same shard
	maxAttempts = 3
	// maxShardTries bounds DynamoDB writes per request when shards are denied
	maxShardTries = 2
	batchSize     = 25
)

// Decision is the result of Allow.
type Decision struct {
	// Allowed reports whether the request is within the rate limit.
	Allowed bool
	// Remaining is the estimated number of requests that can be allowed immediately.
	// It is the value of the selected shard.
	Remaining int64
	// RetryAfter is how long to wait until a request is allowed. Zero when Allowed.
	RetryAfter time.Duration
}

//go:generate go tool mockgen -source=$GOFILE -destination=mock_$GOPACKAGE/$GOFILE -package=mock_$GOPACKAGE
type LimitPreparer interface {
	Limiter
	Preparer
}

type Limiter interface {
	// Allow consumes a token of the bucket.
	// An error is returned with a Decision. Decision.Allowed is false on errors unless fail-open is enabled.
	Allow(ctx context.Context, bucketID string) (Decision, error)
}

type Preparer interface {
	// PrepareTokens registers a bucket id with a full bucket.
	PrepareTokens(ctx context.Context, bucketID string) error
}

type Revoker interface {
	// RevokeTokens deletes the registration of a bucket id.
	RevokeTokens(ctx context.Context, bucketID string) error
}

type RateLimit struct {
	client    *wrappedDDBClient
	gcra      *gcra
	tableName string
	anonymous bool
	ttl       time.Duration
	failOpen  bool
	// ncache remembers unregistered bucket ids
	ncache    *expiringCache
	ncacheTTL time.Duration
	// rcache remembers registered bucket ids
	rcache    *expiringCache
	rcacheTTL time.Duration
	// dcache remembers denied shards until they are allowed again, to skip DynamoDB calls
	dcache    *expiringCache
	maxLength int
	logger    *slog.Logger
	// meterProvider is the global MeterProvider unless WithMeterProvider is given
	meterProvider metric.MeterProvider
	metrics       *metrics
}

// Opt configures RateLimit.
type Opt func(rl *RateLimit)

// WithMeterProvider sets the OpenTelemetry MeterProvider. The global MeterProvider is used by default,
// which is a no-op until an OpenTelemetry SDK is configured.
func WithMeterProvider(mp metric.MeterProvider) Opt {
	return func(rl *RateLimit) {
		rl.meterProvider = mp
	}
}

// WithLogger sets a logger. Logs are discarded by default.
func WithLogger(logger *slog.Logger) Opt {
	return func(rl *RateLimit) {
		rl.logger = logger
	}
}

// WithFailOpen allows requests when DynamoDB fails. The error is still returned.
func WithFailOpen() Opt {
	return func(rl *RateLimit) {
		rl.failOpen = true
	}
}

func withAnonymous(ttl time.Duration) Opt {
	return func(rl *RateLimit) {
		rl.anonymous = true
		rl.ttl = ttl
	}
}

func withNegativeCache(size int, ttl time.Duration) Opt {
	return func(rl *RateLimit) {
		rl.ncache = newExpiringCache(size)
		rl.ncacheTTL = ttl
	}
}

// New returns a RateLimit for cfg. client is usually a *dynamodb.Client.
func New(cfg *Config, client DDBClient, opts ...Opt) (*RateLimit, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = DefaultInterval
	}
	g, err := newGCRA(cfg.TokenPerInterval, cfg.BucketSize, interval, cfg.Shards)
	if err != nil {
		return nil, err
	}

	addOpts, err := cfgToOpts(cfg)
	if err != nil {
		return nil, err
	}

	l := &RateLimit{
		gcra:      g,
		tableName: cfg.TableName,
		ncache:    newExpiringCache(defaultNegativeCacheSize),
		ncacheTTL: defaultNegativeCacheTTL,
		rcache:    newExpiringCache(defaultRegistrationCacheSize),
		rcacheTTL: defaultRegistrationCacheTTL,
		dcache:    newExpiringCache(defaultDenyCacheSize),
		maxLength: 512, // a WCU is 1KiB basis
		logger:    slog.New(slog.DiscardHandler),
	}
	for _, opt := range append(addOpts, opts...) {
		if opt != nil {
			opt(l)
		}
	}

	m, err := newMetrics(l.meterProvider, l.tableName)
	if err != nil {
		return nil, err
	}
	l.metrics = m
	l.client = &wrappedDDBClient{c: client, metrics: m}
	return l, nil
}

// Allow consumes a token of the bucket.
//
// A sharded bucket tries shards in random order. Shards known to be exhausted (the local deny cache) are skipped,
// and a shard denied by DynamoDB is cached and the next shard is tried, up to maxShardTries writes.
// Without this fallback a request could be denied while other shards still have tokens.
func (l *RateLimit) Allow(ctx context.Context, bucketID string) (Decision, error) {
	if bucketID == "" {
		l.metrics.recordDecision(ctx, resultInvalid, sourceCache)
		return Decision{}, fmt.Errorf("empty bucket id: %w", ErrInvalidBucketID)
	}
	if len(bucketID) > l.maxLength {
		l.metrics.recordDecision(ctx, resultInvalid, sourceCache)
		return Decision{}, fmt.Errorf("bucket id is longer than %d: %w", l.maxLength, ErrInvalidBucketID)
	}

	// Logs are for troubleshooting only (debug level). Errors are returned and counted by metrics,
	// so they are not logged here to avoid logging them twice with the caller.
	debug := l.logger.Enabled(ctx, slog.LevelDebug)
	baseLogger := l.logger
	if debug {
		// skip building attributes (and hashing the id) on the hot path unless debug logs are enabled
		baseLogger = l.logger.With(
			slog.String("table_name", l.tableName),
			slog.String("bucket_id", redactID(bucketID)),
		)
	}
	if source, err := l.checkRegistered(ctx, baseLogger, bucketID); err != nil {
		err = fmt.Errorf("bucket %s: %w", redactID(bucketID), err)
		if errors.Is(err, ErrInvalidBucketID) {
			l.metrics.recordDecision(ctx, resultInvalid, source)
			return Decision{}, err
		}
		baseLogger.DebugContext(ctx, "registration check failed", slog.String("error", err.Error()), slog.Bool("fail_open", l.failOpen))
		l.metrics.recordDecision(ctx, resultError, source)
		return Decision{Allowed: l.failOpen}, err
	}
	now := timeNow()
	tries, contended := 0, 0
	var denied Decision // the earliest retry among denied shards
	for _, shardID := range shardOrder(len(l.gcra.shards)) {
		logger := baseLogger
		if debug {
			logger = baseLogger.With(slog.Int("shard_id", shardID))
		}
		pk := makePartitionKey(bucketID, shardID)
		if until, ok := l.dcache.Get(pk); ok {
			denied = earlier(denied, until.Sub(now))
			logger.DebugContext(ctx, "skip a shard denied by the local cache")
			continue
		}
		if tries == maxShardTries {
			break
		}
		tries++

		d, isContended, err := l.allow(ctx, logger, bucketID, shardID, now.UnixNano())
		if err != nil {
			err = fmt.Errorf("bucket %s, shard %d: %w", redactID(bucketID), shardID, err)
			logger.DebugContext(ctx, "dynamodb failed", slog.String("error", err.Error()), slog.Bool("fail_open", l.failOpen))
			l.metrics.recordDecision(ctx, resultError, sourceDynamoDB)
			return Decision{Allowed: l.failOpen}, err
		}
		if d.Allowed {
			l.metrics.recordDecision(ctx, resultAllowed, sourceDynamoDB)
			logger.DebugContext(ctx, "allowed", slog.Int64("remaining", d.Remaining))
			return d, nil
		}
		if isContended {
			contended++
		}
		l.dcache.AddUntil(pk, now.Add(d.RetryAfter))
		denied = earlier(denied, d.RetryAfter)
		logger.DebugContext(ctx, "shard denied", slog.Bool("contended", isContended), slog.Duration("retry_after", d.RetryAfter))
	}

	source, result := sourceDynamoDB, resultDenied
	switch {
	case tries == 0:
		source = sourceCache
	case contended == tries:
		// every shard tried was busy with concurrent updates, not over the rate limit
		result = resultContention
	}
	l.metrics.recordDecision(ctx, result, source)
	baseLogger.DebugContext(ctx, "denied", slog.String("source", source), slog.String("result", result),
		slog.Duration("retry_after", denied.RetryAfter))
	return denied, nil
}

// earlier returns a denied Decision with the earlier RetryAfter.
func earlier(d Decision, retryAfter time.Duration) Decision {
	if d.RetryAfter == 0 || retryAfter < d.RetryAfter {
		d.RetryAfter = retryAfter
	}
	return d
}

type writePath int

const (
	// pathIdle is for TAT < now. TAT becomes now + emission.
	pathIdle writePath = iota
	// pathBusy is for now <= TAT <= now + tolerance. TAT becomes TAT + emission.
	pathBusy
)

// allow runs GCRA with conditional writes. No read is needed:
// a failed condition returns the current item, which tells the next action.
// contended reports a denial because of too many concurrent updates rather than the rate limit.
func (l *RateLimit) allow(ctx context.Context, logger *slog.Logger, bucketID string, shardID int, now int64) (_ Decision, contended bool, _ error) {
	s := l.gcra.shards[shardID]
	path := pathIdle
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var tat int64
		var err error
		switch path {
		case pathIdle:
			tat, err = l.writeIdle(ctx, logger, bucketID, shardID, now, s)
		case pathBusy:
			tat, err = l.writeBusy(ctx, logger, bucketID, shardID, now, s)
		}
		if err == nil {
			return Decision{Allowed: true, Remaining: s.remaining(now, tat)}, false, nil
		}

		var ccf *types.ConditionalCheckFailedException
		if !errors.As(err, &ccf) {
			if errors.Is(err, ErrStoreThrottled) {
				return Decision{}, false, err
			}
			return Decision{}, false, fmt.Errorf("%w: %w", ErrInternal, err)
		}

		if len(ccf.Item) == 0 {
			// not created yet, or deleted concurrently (e.g. by TTL). create it.
			path = pathIdle
			continue
		}

		item := new(ddbItem)
		if err := attributevalue.UnmarshalMap(ccf.Item, item); err != nil {
			return Decision{}, false, fmt.Errorf("%w: unmarshal item: %w", ErrInternal, err)
		}
		switch {
		case item.TAT < now:
			path = pathIdle
		case item.TAT > now+int64(s.tolerance):
			return Decision{Allowed: false, RetryAfter: s.retryAfter(now, item.TAT)}, false, nil
		default:
			path = pathBusy
		}
		logger.DebugContext(ctx, "condition check failed, retrying", slog.Int("attempt", attempt), slog.Int("next_path", int(path)))
	}

	// the shard is updated by many concurrent requests. treat it as exhausted for an emission interval.
	// it is counted as limiter.result=contention.
	return Decision{Allowed: false, RetryAfter: s.emission}, true, nil
}

// writeIdle sets TAT = now + emission when TAT < now, creating the shard item if needed.
// Registration is checked separately by checkRegistered.
func (l *RateLimit) writeIdle(ctx context.Context, logger *slog.Logger, bucketID string, shardID int, now int64, s shardParams) (int64, error) {
	tat := now + int64(s.emission)
	cond := expression.Or(
		expression.Name(attrBucketIDShardID).AttributeNotExists(),
		// items written by the token bucket version have no TAT
		expression.Name(attrTAT).AttributeNotExists(),
		expression.Name(attrTAT).LessThan(expression.Value(now)),
	)
	update := l.withTTL(expression.Set(expression.Name(attrTAT), expression.Value(tat)), now, s)
	if _, err := l.update(ctx, logger, bucketID, shardID, cond, update, types.ReturnValueNone); err != nil {
		return 0, err
	}
	return tat, nil
}

// writeBusy sets TAT = TAT + emission when now <= TAT <= now + tolerance.
func (l *RateLimit) writeBusy(ctx context.Context, logger *slog.Logger, bucketID string, shardID int, now int64, s shardParams) (int64, error) {
	cond := expression.Name(attrTAT).Between(expression.Value(now), expression.Value(now+int64(s.tolerance)))
	update := l.withTTL(expression.Set(expression.Name(attrTAT), expression.Name(attrTAT).Plus(expression.Value(int64(s.emission)))), now, s)
	out, err := l.update(ctx, logger, bucketID, shardID, cond, update, types.ReturnValueUpdatedNew)
	if err != nil {
		return 0, err
	}
	item := new(ddbItem)
	if err := attributevalue.UnmarshalMap(out.Attributes, item); err != nil {
		return 0, fmt.Errorf("unmarshal updated item: %w", err)
	}
	return item.TAT, nil
}

func (l *RateLimit) update(ctx context.Context, logger *slog.Logger, bucketID string, shardID int,
	cond expression.ConditionBuilder, update expression.UpdateBuilder, rv types.ReturnValue) (*dynamodb.UpdateItemOutput, error) {
	expr, err := expression.NewBuilder().WithCondition(cond).WithUpdate(update).Build()
	if err != nil {
		return nil, fmt.Errorf("build expression: %w", err)
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                           &l.tableName,
		Key:                                 buildPartitionKey(bucketID, shardID),
		ExpressionAttributeNames:            expr.Names(),
		ExpressionAttributeValues:           expr.Values(),
		UpdateExpression:                    expr.Update(),
		ConditionExpression:                 expr.Condition(),
		ReturnValues:                        rv,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	out, err := l.client.UpdateItem(ctx, logger, input)
	if err != nil {
		return nil, fmt.Errorf("update-item: %w", err)
	}
	return out, nil
}

// shardTTLMargin keeps a shard item for a while after its bucket became full again.
const shardTTLMargin = time.Hour

// withTTL sets the expiry of a shard item.
//
// After TAT has passed, a shard item is equivalent to a missing item (a full bucket), so it can be deleted
// safely and is created again on demand. The expiry is after the latest possible TAT (now + tolerance + emission),
// which keeps the table from growing with idle, revoked or orphan (after reducing shards) items,
// and never deletes an item while its bucket is still being refilled, even with a short anonymous record_ttl.
func (l *RateLimit) withTTL(update expression.UpdateBuilder, now int64, s shardParams) expression.UpdateBuilder {
	return update.Set(expression.Name(attrTTL), expression.Value(l.shardExpiry(now, s)))
}

func (l *RateLimit) shardExpiry(now int64, s shardParams) int64 {
	base := time.Unix(0, now)
	expiry := base.Add(s.tolerance + s.emission + shardTTLMargin)
	if l.anonymous && base.Add(l.ttl).After(expiry) {
		expiry = base.Add(l.ttl)
	}
	return expiry.Unix()
}

// PrepareTokens registers the bucket id with a full bucket. An existing bucket is reset.
func (l *RateLimit) PrepareTokens(ctx context.Context, bucketID string) error {
	if bucketID == "" || len(bucketID) > l.maxLength {
		return fmt.Errorf("prepare-tokens: bucket id must be 1 to %d bytes: %w", l.maxLength, ErrInvalidBucketID)
	}
	logger := l.logger.With(slog.String("bucket_id", redactID(bucketID)))
	now := timeNow()

	// shard items are reset here, but they are also created on demand
	requests := make([]types.WriteRequest, 0, len(l.gcra.shards)+1)
	for shardID, s := range l.gcra.shards {
		attrs, err := attributevalue.MarshalMap(&ddbItem{
			BucketIDShardID: makePartitionKey(bucketID, shardID),
			// zero means TAT < now, i.e. the bucket is full
			TAT: 0,
			TTL: l.shardExpiry(now.UnixNano(), s),
		})
		if err != nil {
			return fmt.Errorf("prepare-tokens: marshal: %w", err)
		}
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: attrs}})
	}
	// the registration item is written last, so that a registered id always has reset shards
	reg, err := registrationRequest(bucketID, now)
	if err != nil {
		return fmt.Errorf("prepare-tokens: %w", err)
	}
	requests = append(requests, reg)

	for start := 0; start < len(requests); start += batchSize {
		end := min(start+batchSize, len(requests))
		if err := l.batchWrite(ctx, logger, requests[start:end]); err != nil {
			return fmt.Errorf("prepare-tokens: items %d-%d: %w", start, end-1, err)
		}
	}
	// the bucket id may have been cached as unregistered by this instance
	l.ncache.Remove(bucketID)
	l.rcache.AddUntil(bucketID, now.Add(l.rcacheTTL))
	return nil
}

func (l *RateLimit) batchWrite(ctx context.Context, logger *slog.Logger, requests []types.WriteRequest) error {
	b := retry.NewExponentialJitterBackoff(retry.DefaultMaxBackoff)
	items := map[string][]types.WriteRequest{l.tableName: requests}
	for attempt := 0; attempt < retry.DefaultMaxAttempts; attempt++ {
		resp, err := l.client.BatchWriteItem(ctx, logger, &dynamodb.BatchWriteItemInput{RequestItems: items})
		if err != nil {
			return err
		}
		items = resp.UnprocessedItems
		if len(items) == 0 {
			return nil
		}

		delay, err := b.BackoffDelay(attempt, nil)
		if err != nil {
			return fmt.Errorf("backoff delay: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	// unprocessed items are caused by throttling
	return fmt.Errorf("%d items are still unprocessed after %d attempts: %w",
		len(items[l.tableName]), retry.DefaultMaxAttempts, ErrStoreThrottled)
}
