package limiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// A registered bucket id has a registration item "<bucket id>#meta", separate from its shard items "<bucket id>#<n>".
// Shard items are created on demand, so the number of shards can be changed at any time.
const metaSuffix = delimiter + "meta"

// attrLegacyTokenCount exists only in shard items registered by the token bucket version, which has no registration item.
const attrLegacyTokenCount = "token_count"

type registrationItem struct {
	BucketIDShardID string `dynamodbav:"bucket_id_shard_id"`
	// RegisteredAt is a unix seconds timestamp
	RegisteredAt int64 `dynamodbav:"registered_at"`
}

func metaPartitionKey(bucketID string) string {
	return bucketID + metaSuffix
}

func withRegistrationCache(size int, ttl time.Duration) Opt {
	return func(rl *RateLimit) {
		rl.rcache = newExpiringCache(size)
		rl.rcacheTTL = ttl
	}
}

// checkRegistered returns ErrInvalidBucketID if the bucket id is not registered.
// Results are cached per instance: registered ids by the registration cache and unregistered ids by the negative cache.
// source reports whether DynamoDB was read.
func (l *RateLimit) checkRegistered(ctx context.Context, logger *slog.Logger, bucketID string) (source string, _ error) {
	if l.anonymous {
		return sourceCache, nil
	}
	if l.ncache.Contains(bucketID) {
		return sourceCache, fmt.Errorf("unregistered bucket id (cached): %w", ErrInvalidBucketID)
	}
	if l.rcache.Contains(bucketID) {
		return sourceCache, nil
	}

	now := timeNow()
	item, err := l.getItem(ctx, logger, metaPartitionKey(bucketID))
	if err != nil {
		return sourceDynamoDB, err
	}
	if item == nil {
		// registered by the token bucket version
		item, err = l.getItem(ctx, logger, makePartitionKey(bucketID, 0))
		if err != nil {
			return sourceDynamoDB, err
		}
		if _, ok := item[attrLegacyTokenCount]; !ok {
			item = nil
		}
	}
	if item == nil {
		l.ncache.AddUntil(bucketID, now.Add(l.ncacheTTL))
		return sourceDynamoDB, fmt.Errorf("unregistered bucket id: %w", ErrInvalidBucketID)
	}
	l.rcache.AddUntil(bucketID, now.Add(l.rcacheTTL))
	return sourceDynamoDB, nil
}

// getItem returns nil when the item does not exist. It reads strongly consistently
// so that an id registered just before is not cached as unregistered.
func (l *RateLimit) getItem(ctx context.Context, logger *slog.Logger, pk string) (map[string]types.AttributeValue, error) {
	out, err := l.client.GetItem(ctx, logger, &dynamodb.GetItemInput{
		TableName:      &l.tableName,
		Key:            map[string]types.AttributeValue{attrBucketIDShardID: &types.AttributeValueMemberS{Value: pk}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		if errors.Is(err, ErrStoreThrottled) {
			return nil, fmt.Errorf("get-item: %w", err)
		}
		return nil, fmt.Errorf("%w: get-item: %w", ErrInternal, err)
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	return out.Item, nil
}

func registrationRequest(bucketID string, now time.Time) (types.WriteRequest, error) {
	attrs, err := attributevalue.MarshalMap(&registrationItem{
		BucketIDShardID: metaPartitionKey(bucketID),
		RegisteredAt:    now.Unix(),
	})
	if err != nil {
		return types.WriteRequest{}, fmt.Errorf("marshal registration: %w", err)
	}
	return types.WriteRequest{PutRequest: &types.PutRequest{Item: attrs}}, nil
}
