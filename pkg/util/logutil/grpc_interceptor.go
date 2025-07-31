package logutil

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
)

const (
	logLevelRPCMetaKeyLegacy = "log_level"
	logLevelRPCMetaKey       = "log-level"
	clientRequestIDKeyLegacy = "client-request-id"
	clientRequestIDKey       = "client_request_id"
)

// UnaryTraceLoggerInterceptor adds a traced logger in unary rpc call ctx
func UnaryTraceLoggerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newctx := withLevelAndTrace(ctx)
	return handler(newctx, req)
}

// StreamTraceLoggerInterceptor add a traced logger in stream rpc call ctx
func StreamTraceLoggerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	newctx := withLevelAndTrace(ctx)
	wrappedStream := grpc_middleware.WrapServerStream(ss)
	wrappedStream.WrappedContext = newctx
	return handler(srv, wrappedStream)
}

func withLevelAndTrace(ctx context.Context) context.Context {
	newctx := ctx
	mctx := mcontext.FromContext(ctx)

	switch mctx.LogLevel {
	case zapcore.DebugLevel:
		newctx = log.WithDebugLevel(ctx)
	case zapcore.InfoLevel:
		newctx = log.WithInfoLevel(ctx)
	case zapcore.WarnLevel:
		newctx = log.WithWarnLevel(ctx)
	case zapcore.ErrorLevel:
		newctx = log.WithErrorLevel(ctx)
	case zapcore.FatalLevel:
		newctx = log.WithFatalLevel(ctx)
	default:
		newctx = ctx
	}
	// traceID not valid, generate a new one
	traceID := trace.SpanContextFromContext(newctx).TraceID()
	return log.WithTraceID(newctx, traceID.String())
}

func GetMetadata(md metadata.MD, keys ...string) []string {
	var result []string
	for _, key := range keys {
		if values := md.Get(key); len(values) > 0 {
			result = append(result, values...)
		}
	}
	return result
}
