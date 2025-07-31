package interceptor

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	traceIDKey = "traceID"
)

// InterceptorLogger adapts zap logger to interceptor logger.
// This code is simple enough to be copied and not imported.
// from https://github.com/grpc-ecosystem/go-grpc-middleware/blob/v2.3.2/interceptors/logging/examples/zap/example_test.go
func InterceptorLogger() logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		f := make([]zap.Field, 0, len(fields)/2)

		for i := 0; i < len(fields); i += 2 {
			key := fields[i]
			value := fields[i+1]

			switch v := value.(type) {
			case string:
				f = append(f, zap.String(key.(string), v))
			case int:
				f = append(f, zap.Int(key.(string), v))
			case bool:
				f = append(f, zap.Bool(key.(string), v))
			default:
				f = append(f, zap.Any(key.(string), v))
			}
		}
		logger := log.L().WithOptions(zap.AddCallerSkip(1)).WithLazy(f...)

		switch lvl {
		case logging.LevelDebug:
			logger.Debug(msg)
		case logging.LevelInfo:
			logger.Info(msg)
		case logging.LevelWarn:
			logger.Warn(msg)
		case logging.LevelError:
			logger.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}

// NewLogUnaryServerInterceptor is a grpc interceptor that adds traceID to the log fields.
func NewLogUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return logging.UnaryServerInterceptor(InterceptorLogger(),
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			mctx := mcontext.FromContext(ctx)
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", mctx.SourceNodeID,
				"dstNodeID", mctx.DestinationNodeID,
			}
		}),
	)
}

// NewLogStreamServerInterceptor is a grpc interceptor that adds traceID to the log fields.
func NewLogStreamServerInterceptor() grpc.StreamServerInterceptor {
	return logging.StreamServerInterceptor(InterceptorLogger(),
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			mctx := mcontext.FromContext(ctx)
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", mctx.SourceNodeID,
				"dstNodeID", mctx.DestinationNodeID,
			}
		}),
	)
}

// NewLogClientUnaryInterceptor is a grpc interceptor that adds traceID to the log fields.
func NewLogClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return logging.UnaryClientInterceptor(InterceptorLogger(),
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			md, _ := metadata.FromOutgoingContext(ctx)
			dstNodeID := md.Get(mcontext.DestinationServerIDKey)
			var dstNodeIDString string
			if len(dstNodeID) > 0 {
				dstNodeIDString = dstNodeID[0]
			}
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", paramtable.GetStringNodeID(),
				"dstNodeID", dstNodeIDString,
			}
		}),
	)
}

// NewLogClientStreamInterceptor is a grpc interceptor that adds traceID to the log fields.
func NewLogClientStreamInterceptor() grpc.StreamClientInterceptor {
	return logging.StreamClientInterceptor(InterceptorLogger(),
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			md, _ := metadata.FromOutgoingContext(ctx)
			dstNodeID := md.Get(mcontext.DestinationServerIDKey)
			var dstNodeIDString string
			if len(dstNodeID) > 0 {
				dstNodeIDString = dstNodeID[0]
			}
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", paramtable.GetStringNodeID(),
				"dstNodeID", dstNodeIDString,
			}
		}),
	)
}
