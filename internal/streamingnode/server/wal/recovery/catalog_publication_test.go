package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCatalogPublicationPreservesEveryCapturedOperation(t *testing.T) {
	meta := &streamingpb.VChannelMeta{Vchannel: "v1"}
	segment := &streamingpb.SegmentAssignmentMeta{Vchannel: "v1", SegmentId: 1}
	var snapshots []moduleapi.DirtySnapshot
	for _, op := range []moduleapi.SnapshotOp{moduleapi.SnapshotOpUpsert, moduleapi.SnapshotOpUpsertBase, moduleapi.SnapshotOpDelete, moduleapi.SnapshotOpDeleteSchemas} {
		snapshots = append(snapshots, newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v1"}, op, meta))
	}
	for _, op := range []moduleapi.SnapshotOp{moduleapi.SnapshotOpUpsert, moduleapi.SnapshotOpDelete} {
		snapshots = append(snapshots, newOrderedDirtySnapshot(moduleapi.ModuleNameSegment, moduleapi.SnapshotKey{SegmentID: 1}, op, segment))
	}
	publication := catalogPublication(snapshots)
	require.Equal(t, &streamingpb.WALReplicaCatalogPublication{
		Vchannels: []*streamingpb.VChannelMeta{meta}, VchannelBases: []*streamingpb.VChannelMeta{meta},
		RemovedVchannels: []*streamingpb.VChannelMeta{meta}, RemovedSchemas: []*streamingpb.VChannelMeta{meta},
		Segments: []*streamingpb.SegmentAssignmentMeta{segment}, RemovedSegments: []*streamingpb.SegmentAssignmentMeta{segment},
	}, publication)
}
