package limiter

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	errInvalidRateLimitArg        = errors.New("rate_limit must be greater than zero")
	errInvalidRateLimitBucketSize = errors.New("bucket_size must be larger than or equal rate_limit")
	errInvalidInterval            = errors.New("interval must be greater than or equal 1 sec")
	ErrRateLimitExceeded          = errors.New("API rate limit exceeded")
	ErrInternal                   = errors.New("internal API error")
	ErrInvalidBucketID            = errors.New("invalid bucket id")
)

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
