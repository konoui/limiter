### Example server

```
limiter create-table --table-name test_table
limiter start-server --config-file cmd/example-config.yaml
limiter create-token --config-file cmd/example-config.yaml --config-name default
```

On AWS Lambda (see `template.yaml`) the config baked into the image (`LIMITER_CONFIG_FILEPATH`) is used.

### HTTP API

```
$ curl -X POST https://xxxx.execute-api.ap-northeast-1.amazonaws.com/create
{"token":"aaaaaaaa-aaaa-aaaa-aaaa-c95d780512c7"}

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
