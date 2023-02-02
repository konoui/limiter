package limiter

import "errors"

var (
	errInvalidRateLimitArg        = errors.New("rate_limit must greater than zero")
	errInvalidRateLimitBucketSize = errors.New("bucket_size must be twice larger than rate_limit at least")
)
