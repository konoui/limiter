package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func newDDBClient(ctx context.Context) (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return dynamodb.NewFromConfig(cfg), nil
}

// setupMetrics exports metrics with OTLP/HTTP when an OTLP endpoint is configured by the standard
// OTEL_EXPORTER_OTLP_* environment variables (e.g. to the ADOT collector Lambda layer).
// Otherwise metrics are not exported. It returns a function to flush buffered metrics.
func setupMetrics(ctx context.Context) (flush func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") == "" {
		return noop, nil
	}
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return noop, fmt.Errorf("create otlp metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
	otel.SetMeterProvider(mp)
	return mp.ForceFlush, nil
}

// flushAfter flushes metrics after each request.
// A Lambda execution environment may be frozen before a periodic export runs.
func flushAfter(flush func(context.Context) error, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), time.Second)
		defer cancel()
		if err := flush(ctx); err != nil {
			logger.WarnContext(ctx, "failed to flush metrics", "error", err.Error())
		}
	})
}
