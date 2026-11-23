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

#### Shards

A bucket is split into shards so that a single partition key stays below DynamoDB's 1000 WCU per second.
A token bucket allows at most `bucket_size + token_per_interval * 1s / interval` requests in any second,
no matter how many instances (e.g. Lambda) send them, and an allowed request takes up to 2 writes.
The default number of shards keeps each shard under 500 allowed requests per second.
Set `shards` explicitly when the expected traffic per bucket id is known.

Requests try shards in random order and skip shards known to be exhausted, up to 2 writes per request.
`Decision.Remaining` is the value of the selected shard.

#### Registration

`PrepareTokens` writes a registration item `<id>#meta`. Shard items `<id>#<n>` are created on demand,
so `shards` (and the bucket settings) can be changed at any time without re-registering ids.
Registered ids are cached per instance (`registration_cache`, 1 minute by default);
deleting the registration item revokes an id after the cache expires.
Ids registered by the token bucket version are still accepted.

#### Usage

```go
rl, err := limiter.New(cfg, dynamodbClient, limiter.WithLogger(logger))

d, err := rl.Allow(ctx, apiKey)
// d.Allowed, d.Remaining, d.RetryAfter
// errors: limiter.ErrInvalidBucketID, limiter.ErrStoreThrottled (DynamoDB capacity), limiter.ErrInternal
```

net/http middleware (sets `Retry-After` and `X-RateLimit-Remaining`, maps DynamoDB throttling to 503):

```go
h := limiterhttp.Middleware(rl, keyFunc)(next)
```

#### Config

```yaml
table_name: rate_limit
token_per_interval: 20
bucket_size: 40
interval: 60s
shards: 0                 # computed when zero
additional:
  fail_open: false        # allow requests when DynamoDB fails
  anonymous:              # accept unregistered ids, e.g. IP addresses
    record_ttl: 24h
  negative_cache:         # cache unregistered ids
    size: 1000
    ttl: 5m
  registration_cache:     # cache registered ids (revocation delay)
    size: 10000
    ttl: 1m
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
