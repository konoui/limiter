package limiter

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errInvalidRateLimitArg        = errors.New("rate_limit must greater than zero")
	errInvalidRateLimitBucketSize = errors.New("bucket_size must be twice larger than rate_limit at least")
	ErrRateLimitExceeded          = errors.New("API rate limit exceeded")
	ErrInvalidBucketID            = errors.New("invalid bucket id")
)

func isErrConditionalCheckFailed(err error) bool {
	var e *types.ConditionalCheckFailedException
	return errors.As(err, &e)
}

// see https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-conflict-handling
func isErrLimitExceeded(err error) bool {
	var le *types.LimitExceededException
	var re *types.RequestLimitExceeded
	return errors.As(err, &le) || errors.As(err, &re)
}
