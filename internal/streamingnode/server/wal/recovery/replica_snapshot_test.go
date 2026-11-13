package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReplicaMetadataCutHasNoTailAndOwnsItsSuffix(t *testing.T) {
	r := newTestRecoveryStorage(t, readOnlyTestConfig().Checkpoint)
	defer r.closeRecoveryResources()
	views := []*viewpb.QueryViewOfShard{{Meta: &viewpb.QueryViewMeta{Vchannel: "v", ReplicaId: 1, Version: &viewpb.QueryViewVersion{}, State: viewpb.QueryViewState_QueryViewStateUp}}}
	WithReplicaQueryViews(views)(r)
	WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { return nil })(r)
	metas := map[string]*streamingpb.VChannelMeta{"v": {Vchannel: "v", CheckpointTimeTick: 1}}
	r.seedReplicaMetadata(metas, nil)
	metas["v"].CheckpointTimeTick = 999
	views[0].Meta.ReplicaId = 999
	for i := uint64(2); i <= 100; i++ {
		_, err := r.Events().Publish(t.Context(), walevent.WALEntry{Message: newAckTestTimeTickMessage(t, i, int64(i))})
		require.NoError(t, err)
	}
	snapshot, suffix, err := r.CutReplicaSnapshot(t.Context())
	require.NoError(t, err)
	defer suffix.Close()
	require.EqualValues(t, 99, snapshot.Cursor.Seq)
	require.EqualValues(t, 1, snapshot.Checkpoint.TimeTick)
	require.EqualValues(t, 100, snapshot.Observed.TimeTick)
	require.True(t, proto.Equal(walimplstest.NewTestMessageID(100).IntoProto(), snapshot.Observed.MessageId))
	require.EqualValues(t, 1, snapshot.Vchannels[0].CheckpointTimeTick)
	require.EqualValues(t, 1, snapshot.QueryViews[0].Meta.ReplicaId)
	// Traffic grows the cursor and frontier, not the snapshot's payload count.
	require.Less(t, proto.Size(snapshot), 256)
	snapshot.Vchannels[0].CheckpointTimeTick = 888
	_, err = r.Events().Publish(t.Context(), walevent.QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{{Meta: &viewpb.QueryViewMeta{Vchannel: "v", ReplicaId: 1, Version: &viewpb.QueryViewVersion{}, State: viewpb.QueryViewState_QueryViewStateDropped}}}})
	require.NoError(t, err)
	event, err := suffix.Next(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 100, event.Cursor.Seq)
	next, nextSuffix, err := r.CutReplicaSnapshot(t.Context())
	require.NoError(t, err)
	nextSuffix.Close()
	require.Empty(t, next.QueryViews)
	require.EqualValues(t, 1, next.Vchannels[0].CheckpointTimeTick)
	r.Events().Close()
	_, _, err = r.CutReplicaSnapshot(t.Context())
	require.Error(t, err)
	ro, err := NewReadOnlyRecoveryStorage(readOnlyTestConfig())
	require.NoError(t, err)
	defer ro.Close()
	_, _, err = ro.CutReplicaSnapshot(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestReplicaMetadataSeparatesCatalogFromPendingStorage(t *testing.T) {
	m := newReplicaMetadata()
	channel := &streamingpb.VChannelMeta{Vchannel: "v", CreateCollectionTimeTick: 10, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
		CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{{CheckpointTimeTick: 10}, {CheckpointTimeTick: 20}},
	}}
	segment := &streamingpb.SegmentAssignmentMeta{Vchannel: "v", SegmentId: 1, CheckpointTimeTick: 30}
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{Vchannels: []*streamingpb.VChannelMeta{channel}, Segments: []*streamingpb.SegmentAssignmentMeta{segment}})
	newStorage := proto.CloneOf(segment)
	newStorage.CheckpointTimeTick = 50
	materialized := &streamingpb.VChannelMeta{Vchannel: "v", TransformMaterializedTimeTick: 50}
	m.applyStorage(walevent.StoragePublished{Segments: []*streamingpb.SegmentAssignmentMeta{newStorage}, VChannels: []*streamingpb.VChannelMeta{materialized}, Summary: &streamingpb.PChannelSummaryManifest{}})
	segment.CheckpointTimeTick = 40
	m.applyStorage(walevent.StoragePublished{Catalog: &streamingpb.WALReplicaCatalogPublication{Segments: []*streamingpb.SegmentAssignmentMeta{segment}}})
	materialized.TransformMaterializedTimeTick = 40
	m.applyStorage(walevent.StoragePublished{Segments: []*streamingpb.SegmentAssignmentMeta{segment}, VChannels: []*streamingpb.VChannelMeta{materialized}})
	snap, err := m.snapshot(walevent.Cursor{Term: 7, Seq: 8}, walevent.MaxSnapshotBytes)
	require.NoError(t, err)
	require.EqualValues(t, 40, snap.Segments[0].CheckpointTimeTick)
	require.EqualValues(t, 50, snap.Storage.Segments[0].CheckpointTimeTick)
	require.EqualValues(t, 50, snap.Storage.Vchannels[0].TransformMaterializedTimeTick)
	require.NotNil(t, snap.Storage.Summary)
	base := proto.CloneOf(channel)
	base.CollectionInfo.Schemas = nil
	base.CheckpointTimeTick = 60
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{base}})
	require.Len(t, m.vchannels["v"].CollectionInfo.Schemas, 2)
	oldOwner := proto.CloneOf(channel)
	oldOwner.CreateCollectionTimeTick = 5
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{Vchannels: []*streamingpb.VChannelMeta{oldOwner}, RemovedVchannels: []*streamingpb.VChannelMeta{oldOwner}, RemovedSchemas: []*streamingpb.VChannelMeta{oldOwner}})
	require.EqualValues(t, 60, m.vchannels["v"].CheckpointTimeTick)
	removed := proto.CloneOf(channel)
	removed.CollectionInfo.Schemas = removed.CollectionInfo.Schemas[:1]
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{RemovedSchemas: []*streamingpb.VChannelMeta{removed, {Vchannel: "missing"}}})
	require.Len(t, m.vchannels["v"].CollectionInfo.Schemas, 1)
	require.EqualValues(t, 20, m.vchannels["v"].CollectionInfo.Schemas[0].CheckpointTimeTick)
	segment.CheckpointTimeTick = 60
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{Segments: []*streamingpb.SegmentAssignmentMeta{segment}})
	require.Empty(t, m.storage)
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{RemovedSegments: []*streamingpb.SegmentAssignmentMeta{segment}, RemovedVchannels: []*streamingpb.VChannelMeta{channel}})
	require.Empty(t, m.segments)
	require.Empty(t, m.vchannels)
	require.Empty(t, m.materialized)
	// Final catalog registration at the same TimeTick supersedes a prior
	// growing upload; applying that upload after replay must not resurrect it.
	m.applyStorage(walevent.StoragePublished{Segments: []*streamingpb.SegmentAssignmentMeta{newStorage}})
	final := proto.CloneOf(newStorage)
	final.SealedAtDataVersion = &viewpb.DataVersion{}
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{Segments: []*streamingpb.SegmentAssignmentMeta{final}})
	require.Empty(t, m.storage)
	m.applyStorage(walevent.StoragePublished{Segments: []*streamingpb.SegmentAssignmentMeta{newStorage}})
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{Segments: []*streamingpb.SegmentAssignmentMeta{newStorage}})
	require.Empty(t, m.storage)
}

func TestReplicaSnapshotBudgetsRejectCompleteImage(t *testing.T) {
	m := newReplicaMetadata()
	m.vchannels["v"] = &streamingpb.VChannelMeta{Vchannel: "v"}
	image, err := m.snapshot(walevent.Cursor{Term: 7}, walevent.MaxSnapshotBytes)
	require.NoError(t, err)
	size := proto.Size(image)
	_, err = m.snapshot(walevent.Cursor{Term: 7}, size)
	require.NoError(t, err)
	rejected, err := m.snapshot(walevent.Cursor{Term: 7}, size-1)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, rejected)
	for i := 0; i < walevent.MaxSnapshotRecords; i++ {
		m.segments[int64(i)] = &streamingpb.SegmentAssignmentMeta{SegmentId: int64(i)}
	}
	rejected, err = m.snapshot(walevent.Cursor{Term: 7}, walevent.MaxSnapshotBytes)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, rejected)
	require.Len(t, m.segments, walevent.MaxSnapshotRecords, "failed export must preserve every metadata record")
}

func TestReplicaMetadataKeepsVolatileScopedFrontiers(t *testing.T) {
	m := newReplicaMetadata()
	m.observe(newAckTestTimeTickMessage(t, 20, 2))
	barrier := message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{Vchannels: []string{"a", "b"}}).
		WithBody(&message.MVCCBarrierMessageBody{}).WithAllVChannel().WithNotPersisted().MustBuildMutable().
		WithTimeTick(30).WithLastConfirmed(walimplstest.NewTestMessageID(2)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	m.observe(barrier)
	require.EqualValues(t, 20, m.observed.TimeTick, "volatile progress never advances the retained-log upper bound")
	require.Equal(t, map[string]uint64{"a": 30, "b": 30}, m.barriers)
	m.applyCatalog(&streamingpb.WALReplicaCatalogPublication{RemovedVchannels: []*streamingpb.VChannelMeta{{Vchannel: "a"}}})
	require.Equal(t, map[string]uint64{"b": 30}, m.barriers)
}
