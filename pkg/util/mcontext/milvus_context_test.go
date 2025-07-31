package mcontext

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/metadata"
)

func TestNewMilvusContext(t *testing.T) {
	ctx := context.Background()
	mctx := FromContext(ctx)
	assert.Equal(t, defaultMilvusContext, *mctx)

	ctx, _ = NewMilvusContext(ctx)
	assert.Equal(t, defaultMilvusContext, *mctx)
	ctx = AppendOutgoingContext(ctx)

	ctx = context.Background()
	md := metadata.New(map[string]string{
		sourceNodeIDKey:        "123",
		DestinationServerIDKey: "456",
		DestinationClusterKey:  "test-cluster",
		logLevelRPCMetaKey:     "debug",
	})
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx, _ = NewMilvusContext(ctx)
	mctx = FromContext(ctx)

	assert.Equal(t, int64(123), mctx.SourceNodeID)
	assert.Equal(t, int64(456), mctx.DestinationNodeID)
	assert.Equal(t, "test-cluster", mctx.DestinationCluster)
	assert.Equal(t, zapcore.DebugLevel, mctx.LogLevel)

	paramtable.SetNodeID(345)
	ctx = AppendOutgoingContext(ctx)
	md, _ = metadata.FromOutgoingContext(ctx)
	assert.Equal(t, paramtable.GetStringNodeID(), md.Get(sourceNodeIDKey)[0])
	assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), md.Get(DestinationClusterKey)[0])
	assert.Equal(t, "debug", md.Get(logLevelRPCMetaKey)[0])
}
