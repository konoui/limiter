package limiter

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errInvalidRateLimitArg        = errors.New("token_per_interval must be greater than zero")
	errInvalidRateLimitBucketSize = errors.New("bucket_size must be larger than or equal to token_per_interval")
	errInvalidInterval            = errors.New("interval must be greater than or equal 1 sec")
	// ErrStoreThrottled means DynamoDB throttled the request. It is a server side capacity problem,
	// not that the caller exceeded its rate limit. The caller's rate limit is reported by Decision.Allowed.
	ErrStoreThrottled = errors.New("dynamodb throttled the request")
	// ErrInternal means DynamoDB or the limiter failed unexpectedly.
	ErrInternal = errors.New("internal error")
	// ErrInvalidBucketID means the bucket id is empty, too long or not registered.
	ErrInvalidBucketID = errors.New("invalid bucket id")
)

// redactID returns a short, stable and non-reversible identifier for a bucket id.
// Bucket ids may be secrets such as API keys, so they must not appear in errors, logs or metrics as is.
func redactID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "sha256:" + hex.EncodeToString(sum[:6])
}

func isErrConditionalCheckFailed(err error) bool {
	var e *types.ConditionalCheckFailedException
	return errors.As(err, &e)
}

// see https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-conflict-handling
func isErrLimitExceeded(err error) bool {
	// https://github.com/aws/aws-sdk-go-v2/blob/v1.18.0/aws/retry/throttle_error.go#L47
	// https://github.com/aws/aws-sdk-go-v2/blob/v1.18.0/aws/retry/standard.go#L60
	codes := retry.ThrottleErrorCode{Codes: retry.DefaultThrottleErrorCodes}
	if codes.IsErrorThrottle(err) == aws.TrueTernary {
		return true
	}

	var le *types.LimitExceededException
	var re *types.RequestLimitExceeded
	var pte *types.ProvisionedThroughputExceededException
	return errors.As(err, &le) || errors.As(err, &re) || errors.As(err, &pte)
}
