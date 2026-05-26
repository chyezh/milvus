package growing

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestNewVChannelViewFromPersistedMeta(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "vchannel-1",
		CheckpointTimeTick:     11,
		DataCheckpointTimeTick: 22,
	})

	assert.False(t, info.dirty)
	assert.Equal(t, uint64(11), info.MetaTimeTick())
	assert.Equal(t, uint64(22), info.DataTimeTick())

	snapshot := info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
}

func TestVChannelViewDirtySnapshotIncludesCurrentDataState(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick:     5,
		DataCheckpointTimeTick: 5,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "lagged-data-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})
	info.MarkDataCheckpoint(20)

	create := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 100, PartitionId: 102}).
		WithBody(&msgpb.CreatePartitionRequest{CollectionID: 100, PartitionID: 102}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(10)
	immutableCreate := create.WithTimeTick(10).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableCreate))

	snapshot := info.ConsumeDirtyAndGetSnapshot()

	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), snapshot.GetDataCheckpointTimeTick())
	assert.Len(t, snapshot.GetCollectionInfo().GetPartitions(), 2)
}

func TestVChannelViewTombstonedCleanupPlanReturnsCleanupIDs(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "vchannel-1",
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 10},
			},
		},
	})
	info.MarkMetaPersisted(10)
	info.MarkDataPersisted(10)

	dropSnapshot, cleanupPartitions := info.TombstonedCleanupPlan(11, 11)

	assert.Nil(t, dropSnapshot)
	assert.Equal(t, map[int64]uint64{101: 10}, cleanupPartitions)
}

func TestVChannelViewDataUpdatedCallbackRunsAfterOwnerLockReleased(t *testing.T) {
	callbackSawUnlocked := false
	var info *VChannelView
	info = NewVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: &schemapb.CollectionSchema{Name: "schema"}, CheckpointTimeTick: 1},
				},
			},
		},
		runtimeConfig{
			onDataUpdated: func() {
				if info.mu.TryLock() {
					callbackSawUnlocked = true
					info.mu.Unlock()
				}
			},
		},
	)

	info.notifyDataUpdated()

	assert.True(t, callbackSawUnlocked)
}

func TestVChannelViewStaleDataSnapshotDoesNotClearDirtyTombstone(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "dirty-vchannel-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})

	info.TryFinalizeTombstone()
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, info.AssignmentMeta().GetState())
	require.True(t, info.HasDirty())

	info.MarkSnapshotPersisted(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "dirty-vchannel-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})

	require.True(t, info.HasDirty())
	snapshot := info.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetTombstoneTimeTick())
}

func TestVChannelViewStaleDataSnapshotDoesNotClearDirtyPartitionTombstone(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "dirty-partition-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})

	info.TryFinalizeTombstone()
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(info.AssignmentMeta(), 101))
	require.True(t, info.HasDirty())

	info.MarkSnapshotPersisted(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "dirty-partition-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})

	require.True(t, info.HasDirty())
	snapshot := info.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(snapshot, 101))
	assert.Equal(t, uint64(10), partitionTombstoneTimeTick(snapshot, 101))
}

func TestVChannelViewIgnoresDuplicateSchemaChangeReplay(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:           "vchannel-1",
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{Name: "schema-1"}, CheckpointTimeTick: 1},
			},
		},
	})
	msg := message.NewSchemaChangeMessageBuilderV2().
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 100}).
		WithBody(&message.SchemaChangeMessageBody{Schema: &schemapb.CollectionSchema{Name: "schema-2"}}).
		WithVChannel("vchannel-1").
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))
	schemaChange := message.MustAsImmutableSchemaChangeMessageV2(msg)

	info.ObserveSchemaChangeMessageV2(schemaChange)
	info.ObserveSchemaChangeMessageV2(schemaChange)

	assert.Len(t, info.meta.GetCollectionInfo().GetSchemas(), 2)
	assert.Equal(t, uint64(10), info.meta.GetCheckpointTimeTick())
}

func TestVChannelViewIgnoresDuplicateAlterCollectionSchemaReplay(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:           "vchannel-1",
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{Name: "schema-1"}, CheckpointTimeTick: 1},
			},
		},
	})
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 100,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Name: "schema-2"}},
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))
	alter := message.MustAsImmutableAlterCollectionMessageV2(msg)

	info.ObserveAlterCollectionMessageV2(alter)
	info.ObserveAlterCollectionMessageV2(alter)

	assert.Len(t, info.meta.GetCollectionInfo().GetSchemas(), 2)
	assert.Equal(t, uint64(10), info.meta.GetCheckpointTimeTick())
}

func TestVChannelViewIgnoresDuplicateMetadataOnlyAlterCollectionReplay(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:           "vchannel-1",
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{Name: "schema-1"}, CheckpointTimeTick: 1},
			},
		},
	})
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 100,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionProperties}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			},
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))

	result := info.ObserveAlterCollectionMessageV2(message.MustAsImmutableAlterCollectionMessageV2(msg))

	require.NotNil(t, result.Meta)
	assert.False(t, info.dirty)
	assert.Equal(t, uint64(10), info.meta.GetCheckpointTimeTick())
}

func TestVChannelViewConsumeDirtyDoesNotPruneSchemaMeta(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:           "vchannel-1",
		CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "schema-1"},
					CheckpointTimeTick: 1,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
				{
					Schema:             &schemapb.CollectionSchema{Name: "schema-2"},
					CheckpointTimeTick: 5,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})
	info.persistedMetaTimeTick = 4
	info.dirty = true

	snapshot := info.ConsumeDirtyAndGetSnapshot()

	require.NotNil(t, snapshot)
	assert.Len(t, snapshot.GetCollectionInfo().GetSchemas(), 2)
	assert.Len(t, info.meta.GetCollectionInfo().GetSchemas(), 2)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)
}

func TestVChannelViewFromCreateCollectionMessage(t *testing.T) {
	paramtable.Init()
	paramtable.Get().StreamingCfg.WALRecoverySchemaExpirationTolerance.SwapTempValue("0")

	schema1 := &schemapb.CollectionSchema{
		Name: "test-collection-1",
	}
	schema1Bytes, _ := proto.Marshal(schema1)

	// CreateCollection
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{101, 102},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionIDs:   []int64{101, 102},
			Schema:         schema1Bytes,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(1)
	ts := uint64(12345)
	immutableMsg := msg.WithTimeTick(ts).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	manager := newTestManager(nil, nil, nil)
	result := manager.observeMessage(context.Background(), message.MustAsImmutableCreateCollectionMessageV1(immutableMsg))
	assert.NotNil(t, result.Meta)
	info := manager.VChannel("vchannel-1")

	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Equal(t, streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY, info.meta.GrowingSegmentMode)
	assert.NotNil(t, info.meta.LatestDataVersion)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 2)
	assert.Equal(t, uint64(0), info.MetaTimeTick())
	idx, schema1Saved := info.GetSchema(0)
	assert.Equal(t, 0, idx)
	assert.True(t, proto.Equal(schema1, schema1Saved))
	assert.True(t, info.dirty)

	snapshot := info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, ts, info.MetaTimeTick())
	assert.Equal(t, 1, len(info.meta.CollectionInfo.Schemas))
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)

	// CreatePartition
	msg3 := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 100,
			PartitionId:  103,
		}).
		WithBody(&msgpb.CreatePartitionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionID:    103,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID3 := rmq.NewRmqID(3)
	ts += 1
	immutableMsg3 := msg3.WithTimeTick(ts).WithLastConfirmed(msgID3).IntoImmutableMessage(msgID3)

	info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	// idempotent
	info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	assert.True(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	ts += 1
	immutableMsg3 = msg3.WithTimeTick(ts).WithLastConfirmed(msgID3).IntoImmutableMessage(msgID3)
	// idempotent
	info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	// DropPartition
	msg4 := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: 100,
			PartitionId:  101,
		}).
		WithBody(&msgpb.DropPartitionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionID:    101,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID4 := rmq.NewRmqID(4)
	ts += 1
	immutableMsg4 := msg4.WithTimeTick(ts).WithLastConfirmed(msgID4).IntoImmutableMessage(msgID4)

	info.ObserveDropPartitionMessageV1(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	// idempotent
	info.ObserveDropPartitionMessageV1(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partitionState(info.meta, 101))
	assert.True(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	ts += 1
	immutableMsg4 = msg4.WithTimeTick(ts).WithLastConfirmed(msgID4).IntoImmutableMessage(msgID4)
	// idempotent
	info.ObserveDropPartitionMessageV1(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, info.dirty)

	// SchemaChange
	schema2 := &schemapb.CollectionSchema{
		Name: "test-collection-2",
	}
	msg5 := message.NewSchemaChangeMessageBuilderV2().
		WithHeader(&message.SchemaChangeMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.SchemaChangeMessageBody{
			Schema: schema2,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID5 := rmq.NewRmqID(5)
	ts += 1
	immutableMsg5 := msg5.WithTimeTick(ts).WithLastConfirmed(msgID5).IntoImmutableMessage(msgID5)
	info.ObserveSchemaChangeMessageV2(message.MustAsImmutableSchemaChangeMessageV2(immutableMsg5))

	idx, schema2Saved := info.GetSchema(0)
	assert.Equal(t, 1, idx)
	assert.True(t, proto.Equal(schema2, schema2Saved))
	idx, schema2Saved = info.GetSchema(ts)
	assert.Equal(t, 1, idx)
	assert.True(t, proto.Equal(schema2, schema2Saved))
	idx, schema2Saved = info.GetSchema(ts - 1)
	assert.Equal(t, 0, idx)
	assert.True(t, proto.Equal(schema1, schema2Saved))
	assert.True(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)
	assert.Len(t, snapshot.CollectionInfo.Schemas, 2)

	// DropCollection
	msg2 := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID2 := rmq.NewRmqID(2)
	ts += 1
	immutableMsg2 := msg2.WithTimeTick(ts).WithLastConfirmed(msgID2).IntoImmutableMessage(msgID2)

	info.ObserveDropCollectionMessageV1(message.MustAsImmutableDropCollectionMessageV1(immutableMsg2))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partitionState(info.meta, 101))
	assert.True(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)

	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
}

func TestVChannelViewTransitionsDroppedMetaToTombstonedAfterDataCatchesUp(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel: "v1",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "vchannel-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
		CheckpointTimeTick:     4,
		DataCheckpointTimeTick: 4,
	})

	drop := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 100}).
		WithBody(&msgpb.DropCollectionRequest{CollectionID: 100}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(10)
	immutableDrop := drop.WithTimeTick(10).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	info.ObserveDropCollectionMessageV1(message.MustAsImmutableDropCollectionMessageV1(immutableDrop))
	snapshot := info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, snapshot.GetState())

	info.MarkDataCheckpoint(10)
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetDataCheckpointTimeTick())

	info.TryFinalizeTombstone()
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetTombstoneTimeTick())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(snapshot, 101))
}

func TestVChannelViewDoesNotTombstoneBeforeDataCheckpointCatchesUp(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 4,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "vchannel-tombstone-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})

	info.TryFinalizeTombstone()
	meta := info.AssignmentMeta()

	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, meta.GetState())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partitionState(meta, 101))
	assert.Equal(t, uint64(0), meta.GetTombstoneTimeTick())
	assert.False(t, info.HasDirty())
}

func TestVChannelViewDoesNotTombstoneWhenCoveredSegmentTombstoneIsDirty(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "dirty-covered-segment-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})
	segment := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		CollectionId:           100,
		PartitionId:            101,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		TombstoneTimeTick:      10,
		Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
	}, &schemapb.CollectionSchema{Name: "dirty-covered-segment-schema"})
	segment.dirty = true
	segment.persistedMetaTimeTick = 4
	segment.persistedDataTimeTick = 10
	info.addSegment(segment)

	info.TryFinalizeTombstone()

	snapshot := info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, info.AssignmentMeta().GetState())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partitionState(info.AssignmentMeta(), 101))
}

func TestVChannelViewCreatePartitionReactivatesRetainedClosedPartition(t *testing.T) {
	for _, state := range []streamingpb.PartitionState{
		streamingpb.PartitionState_PARTITION_STATE_DROPPED,
		streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED,
	} {
		info := NewVChannelView(&streamingpb.VChannelMeta{
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 101, State: state, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "reactivate-partition-schema"},
						CheckpointTimeTick: 4,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
		})

		create := message.NewCreatePartitionMessageBuilderV1().
			WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 100, PartitionId: 101}).
			WithBody(&msgpb.CreatePartitionRequest{CollectionID: 100, PartitionID: 101}).
			WithVChannel("v1").
			MustBuildMutable()
		msgID := rmq.NewRmqID(20)
		immutableCreate := create.WithTimeTick(20).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

		result := info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableCreate))

		require.NotNil(t, result.Meta)
		assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(info.meta, 101))
		assert.Equal(t, uint64(0), partitionTombstoneTimeTick(info.meta, 101))
		assert.Equal(t, uint64(20), info.meta.GetCheckpointTimeTick())
		assert.True(t, info.dirty)
	}
}

func TestVChannelViewCreatePartitionClearsDirtyPartitionTombstone(t *testing.T) {
	info := NewVChannelView(&streamingpb.VChannelMeta{
		Vchannel:               "v1",
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "reactivate-pending-partition-schema"},
					CheckpointTimeTick: 4,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	})
	info.TryFinalizeTombstone()
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(info.meta, 101))
	require.Equal(t, uint64(9), info.DurableFrontierTimeTick())

	create := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 100, PartitionId: 101}).
		WithBody(&msgpb.CreatePartitionRequest{CollectionID: 100, PartitionID: 101}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(20)
	immutableCreate := create.WithTimeTick(20).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	result := info.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(immutableCreate))

	require.NotNil(t, result.Meta)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(info.meta, 101))
	assert.Equal(t, uint64(0), partitionTombstoneTimeTick(info.meta, 101))
	assert.Equal(t, uint64(20), info.meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(math.MaxUint64), info.DurableFrontierTimeTick())
}

func partitionState(meta *streamingpb.VChannelMeta, partitionID int64) streamingpb.PartitionState {
	for _, partition := range meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition.GetState()
		}
	}
	return streamingpb.PartitionState_PARTITION_STATE_UNKNOWN
}

func partitionTombstoneTimeTick(meta *streamingpb.VChannelMeta, partitionID int64) uint64 {
	for _, partition := range meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition.GetTombstoneTimeTick()
		}
	}
	return 0
}
