package tracer

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// SetupSpan add span into ctx values.
// Also setup logger in context with tracerID field.
func SetupSpan(ctx context.Context, span trace.Span) context.Context {
	ctx = trace.ContextWithSpan(ctx, span)
	ctx = log.WithFields(ctx, log.Stringer("traceID", span.SpanContext().TraceID()))
	return ctx
}

// Propagate passes span context into a new ctx with different lifetime.
// Also setup logger in new context with traceID field.
func Propagate(ctx, newRoot context.Context) context.Context {
	spanCtx := trace.SpanContextFromContext(ctx)

	newCtx := trace.ContextWithSpanContext(newRoot, spanCtx)
	return log.WithFields(newCtx, log.Stringer("traceID", spanCtx.TraceID()))
}
