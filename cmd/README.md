### Example server

| command | purpose |
|---|---|
| `limiter` | serve the example API on AWS Lambda with the config baked into the image (`LIMITER_CONFIG_FILEPATH`) |
| `limiter start-server --config-file <path>` | serve the example API on `:8080` |
| `limiter create-token --config-file <path> --config-name default` | issue an API key (the only way; keys are not issued over HTTP) |
| `limiter revoke-token --config-file <path> --config-name default --token <key>` | revoke an API key. Running instances allow it until their registration cache expires (1 minute by default) |
| `limiter create-table --table-name <name>` | create a table with TTL enabled on the `ttl` attribute |

With DynamoDB Local, point the SDK to it with the standard environment variable:

```
export AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000
limiter create-table --table-name test_table
limiter create-token --config-file cmd/example-config.yaml --config-name default
limiter start-server --config-file cmd/example-config.yaml
```

### HTTP API

```
$ curl -i -H "x-api-key: aaaaaaaa-aaaa-aaaa-aaaa-c95d780512c7" https://xxxx.execute-api.ap-northeast-1.amazonaws.com/hello
HTTP/2 200
x-ratelimit-remaining: 18
ok

$ curl -i https://xxxx.execute-api.ap-northeast-1.amazonaws.com/anon
HTTP/2 429
retry-after: 42
```

### Metrics

Set the standard `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) to export
OpenTelemetry metrics with OTLP/HTTP, e.g. to the ADOT collector Lambda layer. Metrics are flushed after
each request. Without the variable, metrics are not exported.

`LIMITER_LOG_LEVEL=debug` enables debug logs.
