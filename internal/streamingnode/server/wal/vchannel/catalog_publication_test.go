package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalogPublicationRetiresEachReplicaAfterItsOwnViews(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		mockey.PatchConvey("catalog completion does not write or inherit primary references", t, func() {
			meta := &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, CheckpointTimeTick: 10, TransformMaterializedTimeTick: 10}
			segment := &streamingpb.SegmentAssignmentMeta{Vchannel: "v1", SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, CheckpointTimeTick: 10}
			summary := walsummary.NewManager(walsummary.ManagerConfig{ReadOnly: true})
			manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
				PChannel: "p1", ReadOnly: readOnly, SummaryManager: summary,
				VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": meta}, Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1: segment},
			})
			require.NoError(t, err)
			defer manager.Close()
			module := manager.Module("v1")
			require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{meta}, Segments: []*streamingpb.SegmentAssignmentMeta{segment}}))
			if !readOnly {
				require.Len(t, manager.ConsumeCleanupSnapshots(moduleapi.CleanupContext{PhysicalTimeTick: 11}), 1)
			}
			referenced := readOnly
			mockey.Mock((*queryresource.Manager).OldestDataVersion).To(func(*queryresource.Manager) (qviews.DataVersion, bool) { return qviews.DataVersion{}, referenced }).Build()
			require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{RemovedSegments: []*streamingpb.SegmentAssignmentMeta{segment}}))
			if readOnly {
				require.Len(t, module.segments, 1, "the primary's retirement cannot remove locally referenced input")
			} else {
				require.Empty(t, module.segments)
			}
			require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{RemovedVchannels: []*streamingpb.VChannelMeta{meta}}))
			if readOnly {
				require.Same(t, module, manager.Module("v1"))
				mockey.Mock((*queryresource.Manager).Release).To(func(_ *queryresource.Manager, req snview.ReleaseResource) { referenced = false; req.OnDropped() }).Build()
				var released bool
				module.ReleaseQueryResource(snview.ReleaseResource{OnDropped: func() { released = true }})
				require.True(t, released)
				module.reclaimReadOnlyCatalog() // late callbacks do not repeat retirement.
			}
			require.Nil(t, manager.Module("v1"))
		})
	}
}

func TestReadOnlyCatalogCompletesObservedDropWithoutReplacingNewerMetadata(t *testing.T) {
	meta := &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 1, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}, {PartitionId: 2, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{CheckpointTimeTick: 1, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, Schema: &schemapb.CollectionSchema{Version: 1}},
				{CheckpointTimeTick: 5, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, Schema: &schemapb.CollectionSchema{Version: 2}},
			},
		},
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", ReadOnly: true, VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": meta}})
	require.NoError(t, err)
	defer manager.Close()
	module := manager.Module("v1")
	drop := message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 1}).WithBody(&message.DropPartitionRequest{}).MustBuildMutable().WithTimeTick(20).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	observe := func(msg message.ImmutableMessage) {
		owner := message.NewOwnedImmutableMessage(msg, nil)
		dispatch := owner.Clone()
		require.True(t, module.ObserveMessage(context.Background(), dispatch))
		dispatch.Release()
		owner.Release()
	}
	observe(drop)
	// A snapshot captured before Drop cannot complete it.
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{Vchannels: []*streamingpb.VChannelMeta{meta}}))
	require.Len(t, module.vchannelView.pendingDrops, 1)
	published := proto.CloneOf(meta)
	published.CheckpointTimeTick = 20
	published.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
	published.CollectionInfo.Partitions[0].CheckpointTimeTick = 20
	published.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
	published.SegmentDataVersionSummary = &viewpb.DataVersion{StreamingVersion: 10}
	// A newer schema is observed while the older catalog transaction is saving.
	module.vchannelView.meta.CollectionInfo.Schemas = append(module.vchannelView.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{CheckpointTimeTick: 25, Schema: &schemapb.CollectionSchema{Version: 3}})
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{Vchannels: []*streamingpb.VChannelMeta{published}}))
	require.Empty(t, module.vchannelView.pendingDrops)
	require.Len(t, module.vchannelView.AssignmentMeta().CollectionInfo.Schemas, 3)
	removedSchema := &streamingpb.VChannelMeta{Vchannel: "v1", CollectionInfo: &streamingpb.CollectionInfoOfVChannel{Schemas: []*streamingpb.CollectionSchemaOfVChannel{{CheckpointTimeTick: 1}}}}
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{RemovedSchemas: []*streamingpb.VChannelMeta{removedSchema}}))
	require.Len(t, module.vchannelView.AssignmentMeta().CollectionInfo.Schemas, 2)
	published.CollectionInfo.Partitions = published.CollectionInfo.Partitions[1:]
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{published}}))
	require.Len(t, module.vchannelView.AssignmentMeta().CollectionInfo.Partitions, 1)
	collectionDrop := message.NewDropCollectionMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).MustBuildMutable().WithTimeTick(30).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
	observe(collectionDrop)
	published.CheckpointTimeTick = 30
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{published}}))
	require.True(t, module.vchannelView.IsClosing())
	published.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{published}}))
	require.False(t, module.vchannelView.IsClosing())
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, module.vchannelView.AssignmentMeta().State)
}

func TestCatalogPublicationRejectsMissingObservedOwners(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", ReadOnly: true, VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1"}}})
	require.NoError(t, err)
	defer manager.Close()
	for _, publication := range []*streamingpb.WALReplicaCatalogPublication{
		{Vchannels: []*streamingpb.VChannelMeta{{Vchannel: "missing"}}},
		{Segments: []*streamingpb.SegmentAssignmentMeta{{Vchannel: "missing"}}},
		{Segments: []*streamingpb.SegmentAssignmentMeta{{Vchannel: "v1", SegmentId: 1}}},
		{RemovedSchemas: []*streamingpb.VChannelMeta{{Vchannel: "missing"}}},
		{RemovedSegments: []*streamingpb.SegmentAssignmentMeta{{Vchannel: "missing"}}},
		{RemovedVchannels: []*streamingpb.VChannelMeta{{Vchannel: "missing"}}},
	} {
		require.ErrorIs(t, manager.ApplyCatalogPublication(publication), merr.ErrServiceInternal)
	}
	require.Nil(t, manager.Module("missing"))
}

func TestCatalogPublicationCannotRetireReplacementCollection(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", ReadOnly: true,
		VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CreateCollectionTimeTick: 30}},
	})
	require.NoError(t, err)
	defer manager.Close()
	module := manager.Module("v1")
	old := &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, CreateCollectionTimeTick: 10, CheckpointTimeTick: 20}
	require.NoError(t, manager.ApplyCatalogPublication(&streamingpb.WALReplicaCatalogPublication{VchannelBases: []*streamingpb.VChannelMeta{old}, RemovedVchannels: []*streamingpb.VChannelMeta{old}}))
	require.Same(t, module, manager.Module("v1"))
	require.True(t, module.IsActive())
	require.Zero(t, module.catalogRemovedAt)
}
