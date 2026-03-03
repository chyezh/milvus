package logutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/mlog"
)

func TestCtxWithLevelAndTrace(t *testing.T) {
	t.Run("debug level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), mlog.DebugLevel)
		newctx := withLevelAndTrace(ctx)
		assert.NotNil(t, newctx)
	})

	t.Run("info level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), mlog.InfoLevel)
		newctx := withLevelAndTrace(ctx)
		assert.NotNil(t, newctx)
	})

	t.Run("warn level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), mlog.WarnLevel)
		newctx := withLevelAndTrace(ctx)
		assert.NotNil(t, newctx)
	})

	t.Run("error level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), mlog.ErrorLevel)
		newctx := withLevelAndTrace(ctx)
		assert.NotNil(t, newctx)
	})

	t.Run("fatal level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), mlog.FatalLevel)
		newctx := withLevelAndTrace(ctx)
		assert.NotNil(t, newctx)
	})

	t.Run("pass through variables", func(t *testing.T) {
		md := metadata.New(map[string]string{
			logLevelRPCMetaKey: mlog.ErrorLevel.String(),
			clientRequestIDKey: "cb1ef460136611f0b3352a4f4aa7d7fd",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)
		newctx := withLevelAndTrace(ctx)
		md, ok := metadata.FromOutgoingContext(newctx)
		assert.True(t, ok)
		assert.Equal(t, "cb1ef460136611f0b3352a4f4aa7d7fd", md.Get(clientRequestIDKey)[0])
		assert.Equal(t, mlog.ErrorLevel.String(), md.Get(logLevelRPCMetaKey)[0])
	})
}

func withMetaData(ctx context.Context, level mlog.Level) context.Context {
	md := metadata.New(map[string]string{
		logLevelRPCMetaKey: level.String(),
	})
	return metadata.NewIncomingContext(ctx, md)
}
