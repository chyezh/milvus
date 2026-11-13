package recovery

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// catalogPublication is called only after buildRecoverySnapshot has validated
// the captured batch. Preserve deletion descriptors, not just catalog keys, so
// every replica can resolve the same observed owner without scanning modules.
func catalogPublication(snapshots []moduleapi.DirtySnapshot) *streamingpb.WALReplicaCatalogPublication {
	publication := &streamingpb.WALReplicaCatalogPublication{}
	for _, snapshot := range snapshots {
		switch snapshot.ModuleName() {
		case moduleapi.ModuleNameVChannel:
			meta := snapshot.Payload().(*streamingpb.VChannelMeta)
			switch snapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				publication.Vchannels = append(publication.Vchannels, meta)
			case moduleapi.SnapshotOpUpsertBase:
				publication.VchannelBases = append(publication.VchannelBases, meta)
			case moduleapi.SnapshotOpDelete:
				publication.RemovedVchannels = append(publication.RemovedVchannels, meta)
			case moduleapi.SnapshotOpDeleteSchemas:
				publication.RemovedSchemas = append(publication.RemovedSchemas, meta)
			}
		case moduleapi.ModuleNameSegment:
			meta := snapshot.Payload().(*streamingpb.SegmentAssignmentMeta)
			if snapshot.Op() == moduleapi.SnapshotOpDelete {
				publication.RemovedSegments = append(publication.RemovedSegments, meta)
			} else {
				publication.Segments = append(publication.Segments, meta)
			}
		}
	}
	return publication
}
