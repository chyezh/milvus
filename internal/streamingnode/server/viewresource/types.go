package viewresource

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/growingruntime"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type GrowingRuntime = growingruntime.Runtime
type GrowingSegmentRuntimeBuilder = growingruntime.Builder
type NoopGrowingSegmentRuntimeBuilder = growingruntime.NoopBuilder
type SnapshotGrowingSegmentRuntimeBuilder = growingruntime.SnapshotBuilder

// QueryRuntimeModule is a concrete vchannel resource module managed by
// QueryRuntime. Modules do not observe WAL directly.
type QueryRuntimeModule interface {
	Prepare(context.Context, walview.VChannelWALView) error
	ApplyLiveEvent(context.Context, walview.VChannelResourceEvent)
	Advance(qviews.DataVersion)
	Close()
}

// IDFOracleRuntime is the vchannel BM25 / IDF module.
type IDFOracleRuntime interface {
	QueryRuntimeModule
	BM25Oracle
}

// IDFOracleRuntimeBuilder creates an unprepared IDF oracle module.
type IDFOracleRuntimeBuilder interface {
	NewRuntime() (IDFOracleRuntime, error)
}

// BM25Oracle is the query-facing IDF oracle surface.
type BM25Oracle interface {
	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)
}

func QueryViewSettingsFromWALView(view walview.VChannelWALView) *viewpb.QueryViewSettings {
	header := view.LoadConfig.GetHeader()
	if header == nil {
		return &viewpb.QueryViewSettings{}
	}
	fields := make([]int64, 0, len(header.GetLoadFields()))
	for _, field := range header.GetLoadFields() {
		fields = append(fields, field.GetFieldId())
	}
	return &viewpb.QueryViewSettings{
		RequiredPartitions: append([]int64{}, header.GetPartitionIds()...),
		RequiredFields:     fields,
	}
}
