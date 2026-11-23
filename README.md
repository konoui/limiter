### API Rate Limit with DynamoDB

A distributed rate limiter backed by DynamoDB, using GCRA (Generic Cell Rate Algorithm).

#### How it works

Each bucket keeps a single value, the theoretical arrival time (TAT), and a request is decided only with
conditional `UpdateItem`. No read is needed: on a failed condition DynamoDB returns the current item
(`ReturnValuesOnConditionCheckFailure`), which tells whether the bucket is idle, busy or exhausted.

| case | DynamoDB calls |
|---|---|
| allowed, idle bucket | 1 write |
| allowed, busy bucket | 2 writes |
| denied | 1 failed write, then served by a local cache until `RetryAfter` (0 calls) |

GCRA is equivalent to a token bucket that refills `token_per_interval` tokens per `interval` up to `bucket_size`.
Large buckets are split into shards so that a single partition stays below its write capacity;
a request picks a random shard.

#### Usage

```go
rl, err := limiter.New(cfg, dynamodbClient, limiter.WithLogger(logger))

d, err := rl.Allow(ctx, apiKey)
// d.Allowed, d.Remaining, d.RetryAfter
// errors: limiter.ErrInvalidBucketID, limiter.ErrStoreThrottled (DynamoDB capacity), limiter.ErrInternal
```

net/http middleware (sets `Retry-After` and `X-RateLimit-Remaining`, maps DynamoDB throttling to 503):

```go
h := handlerfunc.Middleware(rl, keyFunc)(next)
```

#### Config

```yaml
table_name: rate_limit
token_per_interval: 20
bucket_size: 40
interval: 60s
additional:
  fail_open: false        # allow requests when DynamoDB fails
  anonymous:              # accept unregistered ids, e.g. IP addresses
    record_ttl: 24h
  negative_cache:         # cache unregistered ids
    size: 1000
    ttl: 5m
```

Unknown keys are rejected.

#### Metrics

The library records OpenTelemetry metrics through the global `MeterProvider` (or `limiter.WithMeterProvider`).
It depends only on the OpenTelemetry API; configure an SDK and exporter in your application.

| metric | attributes |
|---|---|
| `limiter.decisions` | `limiter.table`, `limiter.result` (allowed / denied / invalid / error), `limiter.source` (dynamodb / cache) |
| `limiter.dynamodb.throttles` | `limiter.table`, `aws.dynamodb.operation` |

Bucket ids are never recorded in metrics, and appear in errors and logs only as a hash.

#### Test

```
$ make start-local
$ make test
```
