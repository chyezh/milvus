package recovery

import (
	"context"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// replicaMetadata is a projection of published facts, not a copy of mutable
// recovery modules. In particular, assignment statistics in a live module may
// include inserts that have not reached storage yet. All access holds r.mu.
type replicaMetadata struct {
	checkpoint   *streamingpb.WALCheckpoint
	observed     *streamingpb.WALCheckpoint
	vchannels    map[string]*streamingpb.VChannelMeta
	segments     map[int64]*streamingpb.SegmentAssignmentMeta
	storage      map[int64]*streamingpb.SegmentAssignmentMeta
	materialized map[string]*streamingpb.VChannelMeta
	summary      *streamingpb.PChannelSummaryManifest
	views        map[qviews.QueryViewKey]*viewpb.QueryViewOfShard
	barriers     map[string]uint64
}

func newReplicaMetadata() *replicaMetadata {
	return &replicaMetadata{
		vchannels:    make(map[string]*streamingpb.VChannelMeta),
		segments:     make(map[int64]*streamingpb.SegmentAssignmentMeta),
		storage:      make(map[int64]*streamingpb.SegmentAssignmentMeta),
		materialized: make(map[string]*streamingpb.VChannelMeta),
		views:        make(map[qviews.QueryViewKey]*viewpb.QueryViewOfShard),
		barriers:     make(map[string]uint64),
	}
}

// WithReplicaQueryViews seeds intent recovered before the live stream starts.
// Readiness is deliberately not sampled from the primary's local state machine.
func WithReplicaQueryViews(views []*viewpb.QueryViewOfShard) RecoveryStorageOption {
	return func(r *recoveryStorageImpl) { r.replicaMetadata.applyViews(views) }
}

// seedReplicaMetadata runs before recovery modules can mutate the loaded metas.
// Summary restoration may already publish, so retain that independently ordered
// manifest and protect the projection with the same lock as startup observation.
func (r *recoveryStorageImpl) seedReplicaMetadata(vchannels map[string]*streamingpb.VChannelMeta, segments map[int64]*streamingpb.SegmentAssignmentMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.replicaMetadata
	m.checkpoint = proto.CloneOf(r.checkpoint.IntoProto())
	m.observed = proto.CloneOf(m.checkpoint)
	if r.summaryManager != nil && m.summary == nil {
		m.summary = r.summaryManager.PublishedManifest()
	}
	for name, meta := range vchannels {
		m.vchannels[name] = proto.CloneOf(meta)
	}
	for id, meta := range segments {
		m.segments[id] = proto.CloneOf(meta)
	}
}

// CutReplicaSnapshot atomically freezes metadata and subscribes to S+1. It does
// no catalog or object I/O. The bootstrap owner must protect the returned replay
// range and object references before allowing a remote replica to consume it.
func (r *recoveryStorageImpl) CutReplicaSnapshot(ctx context.Context) (*streamingpb.WALReplicaSnapshot, *walevent.Reader, error) {
	if r.events == nil {
		return nil, nil, merr.WrapErrServiceUnavailable("read-only WAL has no event source")
	}
	var snapshot *streamingpb.WALReplicaSnapshot
	reader, err := r.events.Cut(ctx, func(cursor walevent.Cursor) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		var err error
		snapshot, err = r.replicaMetadata.snapshot(cursor, walevent.MaxSnapshotBytes)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return snapshot, reader, nil
}

func (m *replicaMetadata) observe(msg message.ImmutableMessage) {
	if msg.IsPersisted() {
		m.observed = &streamingpb.WALCheckpoint{
			MessageId: message.MustMarshalMessageID(msg.LastConfirmedMessageID()), TimeTick: msg.TimeTick(),
			LogGeneration: m.checkpoint.GetLogGeneration(),
		}
	}
	if msg.MessageType() == message.MessageTypeMVCCBarrier {
		for _, name := range message.MustAsImmutableMVCCBarrierMessageV2(msg).Header().GetVchannels() {
			m.barriers[name] = max(m.barriers[name], msg.TimeTick())
		}
	}
}

func (m *replicaMetadata) applyViews(views []*viewpb.QueryViewOfShard) {
	for _, view := range views {
		key := qviews.QueryViewKey{
			ShardID:          qviews.NewShardIDFromQVMeta(view.GetMeta()),
			QueryViewVersion: qviews.FromProtoQueryViewVersion(view.GetMeta().GetVersion()),
		}
		if view.GetMeta().GetState() == viewpb.QueryViewState_QueryViewStateDropped {
			delete(m.views, key)
		} else {
			m.views[key] = proto.CloneOf(view)
		}
	}
}

func (m *replicaMetadata) applyStorage(publication walevent.StoragePublished) {
	if publication.Catalog != nil {
		m.applyCatalog(publication.Catalog)
	}
	if publication.Summary != nil {
		m.summary = proto.CloneOf(publication.Summary)
	}
	for _, meta := range publication.Segments {
		if previous := m.storage[meta.GetSegmentId()]; previous == nil || previous.GetCheckpointTimeTick() <= meta.GetCheckpointTimeTick() {
			m.storage[meta.GetSegmentId()] = proto.CloneOf(meta)
		}
	}
	for _, meta := range publication.VChannels {
		if previous := m.materialized[meta.GetVchannel()]; previous == nil || previous.GetTransformMaterializedTimeTick() < meta.GetTransformMaterializedTimeTick() {
			m.materialized[meta.GetVchannel()] = proto.CloneOf(meta)
		}
	}
}

func (m *replicaMetadata) applyCatalog(publication *streamingpb.WALReplicaCatalogPublication) {
	for _, meta := range publication.Vchannels {
		m.applyVChannel(meta, false)
	}
	for _, meta := range publication.VchannelBases {
		m.applyVChannel(meta, true)
	}
	for _, meta := range publication.Segments {
		m.segments[meta.GetSegmentId()] = proto.CloneOf(meta)
		// A catalog transaction can finish after a newer upload. Keep that
		// upload as a replay overlay instead of regressing physical coverage.
		if stored := m.storage[meta.GetSegmentId()]; stored != nil {
			covered := stored.GetCheckpointTimeTick() < meta.GetCheckpointTimeTick()
			if stored.GetCheckpointTimeTick() == meta.GetCheckpointTimeTick() {
				// Final registration can complete without another insert. Do not
				// overlay its catalog descriptor with an older growing descriptor
				// at the same physical frontier.
				covered = proto.Equal(stored, meta) || meta.GetSealedAtDataVersion() != nil || meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
			}
			if covered {
				delete(m.storage, meta.GetSegmentId())
			}
		}
	}
	for _, meta := range publication.RemovedSchemas {
		current := m.vchannels[meta.GetVchannel()]
		if current == nil || current.GetCreateCollectionTimeTick() > meta.GetCreateCollectionTimeTick() {
			continue
		}
		removed := make(map[uint64]struct{})
		for _, schema := range meta.GetCollectionInfo().GetSchemas() {
			removed[schema.GetCheckpointTimeTick()] = struct{}{}
		}
		current.CollectionInfo.Schemas = slices.DeleteFunc(current.CollectionInfo.Schemas, func(schema *streamingpb.CollectionSchemaOfVChannel) bool {
			_, ok := removed[schema.GetCheckpointTimeTick()]
			return ok
		})
	}
	for _, meta := range publication.RemovedSegments {
		delete(m.segments, meta.GetSegmentId())
		delete(m.storage, meta.GetSegmentId())
	}
	for _, meta := range publication.RemovedVchannels {
		if current := m.vchannels[meta.GetVchannel()]; current != nil && current.GetCreateCollectionTimeTick() > meta.GetCreateCollectionTimeTick() {
			continue
		}
		delete(m.vchannels, meta.GetVchannel())
		delete(m.materialized, meta.GetVchannel())
		delete(m.barriers, meta.GetVchannel())
	}
}

func (m *replicaMetadata) applyVChannel(meta *streamingpb.VChannelMeta, baseOnly bool) {
	current := m.vchannels[meta.GetVchannel()]
	if current != nil && current.GetCreateCollectionTimeTick() > meta.GetCreateCollectionTimeTick() {
		return
	}
	clone := proto.CloneOf(meta)
	if baseOnly && current != nil && current.GetCreateCollectionTimeTick() == meta.GetCreateCollectionTimeTick() {
		clone.CollectionInfo.Schemas = current.GetCollectionInfo().GetSchemas()
	}
	m.vchannels[meta.GetVchannel()] = clone
}

func (m *replicaMetadata) snapshot(cursor walevent.Cursor, maxBytes int) (*streamingpb.WALReplicaSnapshot, error) {
	// Check cardinality before allocating the repeated-field slices, and wire
	// size before cloning any nested schemas, segment metadata or View payloads.
	// A baseline is all-or-nothing; never truncate metadata to fit the budget.
	if len(m.vchannels)+len(m.segments)+len(m.materialized)+len(m.storage)+len(m.views)+len(m.barriers) > walevent.MaxSnapshotRecords {
		return nil, merr.WrapErrServiceUnavailable("WAL replica snapshot record budget exhausted")
	}
	result := &streamingpb.WALReplicaSnapshot{
		Cursor: cursor.IntoProto(), Checkpoint: m.checkpoint, Observed: m.observed,
		Vchannels: slices.Collect(maps.Values(m.vchannels)), Segments: slices.Collect(maps.Values(m.segments)),
		Storage: &streamingpb.WALReplicaStoragePublication{
			Vchannels: slices.Collect(maps.Values(m.materialized)), Segments: slices.Collect(maps.Values(m.storage)), Summary: m.summary,
		},
		QueryViews: slices.Collect(maps.Values(m.views)), QueryBarriers: m.barriers,
	}
	if proto.Size(result) > maxBytes {
		return nil, merr.WrapErrServiceUnavailable("WAL replica snapshot byte budget exhausted")
	}
	// The source gate orders the cut; the clone gives the transport ownership
	// without retaining a lock while it encodes or sends the image.
	return proto.CloneOf(result), nil
}
