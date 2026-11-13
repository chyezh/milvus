package segment

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *SegmentView) publishStorage(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	if s.runtime.PublishStorage != nil {
		return s.runtime.PublishStorage(ctx, walevent.StoragePublished{Segments: []*streamingpb.SegmentAssignmentMeta{meta}})
	}
	// Standalone modules use the same Apply operation without a WAL sequencer.
	return s.ApplyStoragePublished(ctx, meta)
}

// ApplyStoragePublished installs durable query-visible state. Workers keep
// uploaded objects and registration results private until this operation runs.
// Live observation may be ahead of the durable descriptor: never overwrite its
// row counters, closing state or observation watermark with an older prefix.
func (s *SegmentView) ApplyStoragePublished(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	s.mu.Lock()
	if meta.GetSegmentId() != s.segmentID || meta.GetVchannel() != s.vchannel {
		s.mu.Unlock()
		return merr.WrapErrServiceInternalMsg("published segment identity does not match local segment %d", s.segmentID)
	}
	if err := s.unrecoverableErr(); err != nil {
		s.mu.Unlock()
		return err
	}
	if meta.GetCheckpointTimeTick() < s.durableMeta.GetCheckpointTimeTick() || proto.Equal(meta, s.durableMeta) {
		s.mu.Unlock()
		return nil
	}
	handles := s.markPendingDataDurableLocked(meta.GetCheckpointTimeTick())
	if s.readOnly {
		through := 0
		for through < len(s.readOnlyInserts) && s.readOnlyInserts[through].TimeTick() <= meta.GetCheckpointTimeTick() {
			through++
		}
		s.readOnlyInserts = slices.Delete(s.readOnlyInserts, 0, through)
	}
	s.durableMeta = proto.CloneOf(meta)
	s.meta.PersistedStorage = cloneSegmentMetaState(s.durableMeta).PersistedStorage
	s.dirty = true
	var abandoned []message.RetainedImmutableMessage
	if finalCommitDoneFromMeta(meta) {
		s.finalCommitDone.Store(true)
		s.meta.State = meta.State
		s.meta.SealedAtDataVersion = s.durableMeta.SealedAtDataVersion
		s.meta.CheckpointTimeTick = max(s.meta.CheckpointTimeTick, meta.CheckpointTimeTick)
		if s.meta.Stat != nil && meta.Stat != nil {
			s.meta.Stat.LastModifiedTimestamp = meta.Stat.LastModifiedTimestamp
		}
		// Preserve the existing poison contract for a tail that no task can
		// persist after the final commit. Never ACK these handles as durable.
		if len(s.pending.entries) > 0 {
			mlog.Warn(ctx, "final commit leaves pending insert data unpersisted",
				mlog.Int64("segmentID", s.segmentID), mlog.String("vchannel", s.vchannel),
				mlog.Int("pendingEntries", len(s.pending.entries)),
				mlog.Uint64("pendingFromTimeTick", s.pending.fromTimeTick),
				mlog.Uint64("commitTimeTick", meta.CheckpointTimeTick))
			abandoned = s.pending.entries
			s.pending.reset()
		}
	}
	s.mu.Unlock()
	s.NotifyDataUpdated()
	releaseMessages(handles)
	for _, handle := range abandoned {
		handle.PoisonedRelease()
	}
	return nil
}
