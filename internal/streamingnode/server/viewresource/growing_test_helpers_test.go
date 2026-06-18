package viewresource

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/growingruntime"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type noopGrowingRuntimeApplier = growingruntime.NoopApplier

func newGrowingRuntimeForTest(applier GrowingRuntimeApplier) *GrowingRuntime {
	return growingruntime.NewRuntime(applier)
}

func deleteTimestampsFromRequest(timeTick uint64, request *msgpb.DeleteRequest) []typeutil.Timestamp {
	return growingruntime.DeleteTimestampsFromRequest(timeTick, request)
}

func deleteTimestampsFromTransformLogBlock(timeTick uint64, block *streamingpb.TransformDeleteBlock) []typeutil.Timestamp {
	return growingruntime.DeleteTimestampsFromTransformLogBlock(timeTick, block)
}
