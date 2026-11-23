package limiter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/konoui/limiter"

// metric attribute values. Bucket ids are never used as attributes (unbounded cardinality, may be secrets).
const (
	resultAllowed = "allowed"
	resultDenied  = "denied"
	resultInvalid = "invalid"
	resultError   = "error"
	// resultContention is a denial because every shard tried was busy with concurrent updates
	resultContention = "contention"

	sourceDynamoDB = "dynamodb"
	sourceCache    = "cache"
)

var (
	attrKeyTable     = attribute.Key("limiter.table")
	attrKeyName      = attribute.Key("limiter.name")
	attrKeyResult    = attribute.Key("limiter.result")
	attrKeySource    = attribute.Key("limiter.source")
	attrKeyOperation = attribute.Key("aws.dynamodb.operation")
)

// metrics records OpenTelemetry metrics. The library depends only on the OpenTelemetry API.
// Users choose exporters by configuring a MeterProvider (the global one by default).
type metrics struct {
	decisions metric.Int64Counter
	throttles metric.Int64Counter
	// base attributes of every data point: limiter.table, and limiter.name when Config.Name is set
	base []attribute.KeyValue
}

func newMetrics(mp metric.MeterProvider, table, name string) (*metrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(instrumentationName)
	decisions, err := m.Int64Counter("limiter.decisions",
		metric.WithDescription("Number of rate limit decisions"),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("create limiter.decisions counter: %w", err)
	}
	throttles, err := m.Int64Counter("limiter.dynamodb.throttles",
		metric.WithDescription("Number of DynamoDB requests throttled by DynamoDB"),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("create limiter.dynamodb.throttles counter: %w", err)
	}
	base := []attribute.KeyValue{attrKeyTable.String(table)}
	if name != "" {
		base = append(base, attrKeyName.String(name))
	}
	return &metrics{decisions: decisions, throttles: throttles, base: base}, nil
}

func (m *metrics) recordDecision(ctx context.Context, result, source string) {
	m.decisions.Add(ctx, 1, metric.WithAttributes(append(m.base[:len(m.base):len(m.base)],
		attrKeyResult.String(result), attrKeySource.String(source))...))
}

func (m *metrics) recordStoreThrottle(ctx context.Context, operation string) {
	m.throttles.Add(ctx, 1, metric.WithAttributes(append(m.base[:len(m.base):len(m.base)],
		attrKeyOperation.String(operation))...))
}
