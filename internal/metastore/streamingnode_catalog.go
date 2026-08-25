package metastore

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// StreamingNodeCataLog is the interface for streamingnode catalog
type StreamingNodeCataLog interface {
	// WAL select the wal related recovery infos.
	// Which must give the pchannel name.

	// ListVChannel list all vchannels on current pchannel.
	ListVChannel(ctx context.Context, pchannelName string) ([]*streamingpb.VChannelMeta, error)

	// ListSegmentAssignment list all segment assignments for the wal.
	ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error)

	// GetConsumeCheckpoint gets the consuming checkpoint of the wal.
	// Return nil, nil if the checkpoint is not exist.
	GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error)

	// GetPChannelRecoveryControlMeta gets the pchannel-scoped recovery control
	// state. Return nil, nil if the state does not exist.
	GetPChannelRecoveryControlMeta(ctx context.Context, pChannelName string) (*streamingpb.PChannelRecoveryControlMeta, error)

	// GetSalvageCheckpoint gets all salvage checkpoints for a channel.
	// Returns an empty slice if none exist. One checkpoint per source cluster.
	GetSalvageCheckpoint(ctx context.Context, pChannelName string) ([]*commonpb.ReplicateCheckpoint, error)

	// SaveRecoverySnapshot applies a WAL recovery DELTA in one compound
	// operation. Despite the name it is not a full-state replacement: only the
	// entries present in the payload are touched, missing keys are left
	// unchanged, and deletion is expressed by the explicit Removed* sections,
	// never by omission. It therefore cannot express
	// "replace the persisted recovery state with this set" (in particular an
	// empty payload is a no-op, not a wipe); pruning stale keys is the caller's
	// responsibility.
	//
	// The etcd-based implementation stages the delta as a single composite
	// write via the shared txn.Builder/txn.Commit primitive - atomically when
	// the op count fits the etcd txn limit, else via an ordered chunked
	// fallback - with the consume checkpoint always the last/commit-marker op.
	// On the atomic path the whole delta becomes visible together. On the
	// fallback path non-commit parts may become visible before the checkpoint;
	// their component checkpoints make replay from the old global checkpoint
	// idempotent. A CAS
	// single-point commit (see the recovery background-task TODO) is the
	// durable fix.
	SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *WALRecoverySnapshot) error

	// SavePendingL0Segment records a pending L0 segment registration of the
	// pchannel: the segment's binlog objects are durable and its DataCoord
	// registration is still in flight. The payload is the exact
	// SaveBinlogPathsRequest that registers the segment; re-sending it after a
	// crash is idempotent because DataCoord applies it as a full replacement
	// (WithFullBinlogs). Recovery replays every pending record before it can
	// advance the materialization frontier, and the record is removed only
	// after the vchannel meta carrying that frontier is persisted.
	SavePendingL0Segment(ctx context.Context, pChannelName string, pending *datapb.SaveBinlogPathsRequest) error

	// ListPendingL0Segments lists the pending L0 segment registrations of the
	// pchannel. Returns an empty slice when none exist.
	ListPendingL0Segments(ctx context.Context, pChannelName string) ([]*datapb.SaveBinlogPathsRequest, error)

	// RemovePendingL0Segments removes the pending L0 segment registrations of
	// the given segment IDs. Removing an absent record is a no-op.
	RemovePendingL0Segments(ctx context.Context, pChannelName string, segmentIDs []int64) error
}

// WALRecoverySnapshot is the compound payload of
// StreamingNodeCataLog.SaveRecoverySnapshot. It is a delta, not a full
// snapshot: absent sections mean "unchanged", and deletion is carried by
// explicit Removed* sections, not by omission. See SaveRecoverySnapshot.
type WALRecoverySnapshot struct {
	// PChannelControlMeta is the pchannel-scoped control state to save; skipped if nil.
	PChannelControlMeta *streamingpb.PChannelRecoveryControlMeta
	// SegmentAssignments are the segment assignments to save; skipped if empty.
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	// RemovedSegmentIDs are the segment assignments to remove; skipped if empty.
	RemovedSegmentIDs []int64
	// VChannels are the complete vchannel metas, including schemas, to save;
	// skipped if empty.
	VChannels map[string]*streamingpb.VChannelMeta
	// VChannelBaseMetas update only the vchannel base record without rewriting
	// separately stored schemas; skipped if empty.
	VChannelBaseMetas map[string]*streamingpb.VChannelMeta
	// RemovedVChannels are the vchannel base and schema records to remove;
	// skipped if empty.
	RemovedVChannels map[string]*streamingpb.VChannelMeta
	// SalvageCheckpoint is the salvage checkpoint to save; skipped if nil.
	// It must be persisted before the consume checkpoint to guarantee ordering.
	SalvageCheckpoint *commonpb.ReplicateCheckpoint
	// ConsumeCheckpoint is the consume checkpoint to save; skipped if nil.
	// It is always written last as the commit point of the snapshot.
	ConsumeCheckpoint *streamingpb.WALCheckpoint
}
