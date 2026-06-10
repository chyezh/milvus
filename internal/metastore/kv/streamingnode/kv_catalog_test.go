package streamingnode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatalogConsumeCheckpoint(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	v := streamingpb.WALCheckpoint{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Return(string(vs), nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.NotNil(t, checkpoint)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", errors.New("err"))
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Error(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", merr.ErrIoKeyNotFound)
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Nil(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.NoError(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("err"))
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.Error(t, err)
}

func TestCatalogSegmentAssignments(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	k := buildSegmentAssignmentKey("p1", 10)
	v := streamingpb.SegmentAssignmentMeta{SegmentId: 10}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return([]string{k}, []string{string(vs)}, nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	metas, err := catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, metas, 1)
	assert.NoError(t, err)

	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil)

	err = catalog.SaveSegmentAssignments(ctx, "p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			SegmentId: 1,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		},
		2: {
			SegmentId: 2,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	})
	assert.NoError(t, err)
}

func TestCatalogTransformLogMeta(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	meta := &streamingpb.VChannelTransformLogMeta{
		CheckpointTimeTick: 50,
		FirstChunkId:       3,
		NextChunkId:        4,
	}
	value, err := proto.Marshal(meta)
	require.NoError(t, err)

	kv.EXPECT().LoadWithPrefix(mock.Anything, buildTransformLogPrefix("p1")).
		Return([]string{buildTransformLogKey("p1", "v1")}, []string{string(value)}, nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	metas, err := catalog.ListTransformLogMeta(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, metas, 1)
	assert.True(t, proto.Equal(meta, metas["v1"]))

	kv.EXPECT().MultiSave(mock.Anything, mock.MatchedBy(func(kvs map[string]string) bool {
		saved, ok := kvs[buildTransformLogKey("p1", "v1")]
		if !ok {
			return false
		}
		loaded := &streamingpb.VChannelTransformLogMeta{}
		return proto.Unmarshal([]byte(saved), loaded) == nil && proto.Equal(meta, loaded)
	})).Return(nil)
	require.NoError(t, catalog.SaveTransformLogMeta(ctx, "p1", map[string]*streamingpb.VChannelTransformLogMeta{"v1": meta}))

	kv.EXPECT().MultiRemove(mock.Anything, []string{buildTransformLogKey("p1", "v1")}).
		Return(nil)
	require.NoError(t, catalog.DropTransformLogMeta(ctx, "p1", []string{"v1"}))
}

func TestCatalogListSegmentAssignmentRejectsMismatchedOwner(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	segment := &streamingpb.SegmentAssignmentMeta{
		SegmentId: 20,
		State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
	}
	value, err := proto.Marshal(segment)
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildSegmentAssignmentPrefix("p1")).Return(
		[]string{buildSegmentAssignmentKey("p1", 10)},
		[]string{string(value)},
		nil,
	)

	catalog := NewCataLog(kv)
	segments, err := catalog.ListSegmentAssignment(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, segments)
	assert.ErrorContains(t, err, "mismatched segment assignment")
}

func TestCatalogRetainsClosedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogRetainsClosedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 200}},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
		},
	}
	require.NoError(t, catalog.SaveVChannels(ctx, "p1", vchannels))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, loadedVChannels[0].GetState())
	assert.Equal(t, uint64(100), loadedVChannels[0].GetCheckpointTimeTick())

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			CollectionId:           100,
			PartitionId:            200,
			SegmentId:              300,
			Vchannel:               "vchannel-1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     120,
			DataCheckpointTimeTick: 80,
		},
	}
	require.NoError(t, catalog.SaveSegmentAssignments(ctx, "p1", segments))

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, loadedSegments[0].GetState())
	assert.Equal(t, uint64(120), loadedSegments[0].GetCheckpointTimeTick())
	assert.Equal(t, uint64(80), loadedSegments[0].GetDataCheckpointTimeTick())
}

func TestCatalogListVChannelRejectsMissingSchema(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "v1",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
		},
	}
	value, err := proto.Marshal(vchannel)
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelPrefix("p1")).
		Return([]string{buildVChannelKey("p1", "v1")}, []string{string(value)}, nil)

	catalog := NewCataLog(kv)
	vchannels, err := catalog.ListVChannel(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, vchannels)
	assert.ErrorContains(t, err, "missing schemas")
}

func TestCatalogListVChannelRejectsMismatchedOwner(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "other",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
		},
	}
	vchannelValue, err := proto.Marshal(vchannel)
	require.NoError(t, err)
	schemaValue, err := proto.Marshal(&streamingpb.CollectionSchemaOfVChannel{
		Schema:             &schemapb.CollectionSchema{Name: "schema"},
		CheckpointTimeTick: 10,
	})
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelPrefix("p1")).Return(
		[]string{
			buildVChannelKey("p1", "v1"),
			buildVChannelSchemaKey("p1", "v1", 10),
		},
		[]string{string(vchannelValue), string(schemaValue)},
		nil,
	)

	catalog := NewCataLog(kv)
	vchannels, err := catalog.ListVChannel(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, vchannels)
	assert.ErrorContains(t, err, "mismatched vchannel")
}

func TestCatalogRetainsTombstonedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogRetainsTombstonedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       200,
						State:             streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED,
						TombstoneTimeTick: 120,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
			TombstoneTimeTick:  100,
		},
	}
	require.NoError(t, catalog.SaveVChannels(ctx, "p1", vchannels))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, loadedVChannels[0].GetState())
	assert.Equal(t, uint64(100), loadedVChannels[0].GetTombstoneTimeTick())
	require.Len(t, loadedVChannels[0].GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, loadedVChannels[0].GetCollectionInfo().GetPartitions()[0].GetState())
	assert.Equal(t, uint64(120), loadedVChannels[0].GetCollectionInfo().GetPartitions()[0].GetTombstoneTimeTick())

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			CollectionId:           100,
			PartitionId:            200,
			SegmentId:              300,
			Vchannel:               "vchannel-1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     120,
			DataCheckpointTimeTick: 120,
			TombstoneTimeTick:      120,
		},
	}
	require.NoError(t, catalog.SaveSegmentAssignments(ctx, "p1", segments))

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, loadedSegments[0].GetState())
	assert.Equal(t, uint64(120), loadedSegments[0].GetTombstoneTimeTick())
}

func TestCatalogDropsTombstonedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogDropsTombstonedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-2"},
						CheckpointTimeTick: 20,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
			TombstoneTimeTick:  100,
		},
		"vchannel-2": {
			Vchannel: "vchannel-2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 101,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-3"},
						CheckpointTimeTick: 30,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
	}
	require.NoError(t, catalog.SaveVChannels(ctx, "p1", vchannels))

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			SegmentId:              300,
			Vchannel:               "vchannel-1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     120,
			DataCheckpointTimeTick: 120,
			TombstoneTimeTick:      120,
		},
		301: {
			SegmentId: 301,
			Vchannel:  "vchannel-2",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}
	require.NoError(t, catalog.SaveSegmentAssignments(ctx, "p1", segments))

	require.NoError(t, catalog.DropVChannels(ctx, "p1", vchannelsByName(vchannels, "vchannel-1")))
	require.NoError(t, catalog.DropSegmentAssignments(ctx, "p1", []int64{300}))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, "vchannel-2", loadedVChannels[0].GetVchannel())

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, int64(301), loadedSegments[0].GetSegmentId())
}

func vchannelsByName(vchannels map[string]*streamingpb.VChannelMeta, names ...string) map[string]*streamingpb.VChannelMeta {
	selected := make(map[string]*streamingpb.VChannelMeta, len(names))
	for _, name := range names {
		selected[name] = vchannels[name]
	}
	return selected
}

func TestCatalogRejectsDroppedVChannelSchemaOnSave(t *testing.T) {
	catalog := &catalog{}
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "vchannel-1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
					CheckpointTimeTick: 10,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
				},
			},
		},
	}

	removes, kvs, err := catalog.getRemovalAndSaveForVChannel("p1", vchannel)
	require.Error(t, err)
	assert.Nil(t, removes)
	assert.Nil(t, kvs)
	assert.ErrorContains(t, err, "unknown vchannel schema state")
}

func TestCatalogVChannel(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogVChannel-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	channel1 := "p1"
	vchannels, err := catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 0)
	assert.NoError(t, err)

	vchannelMetas := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 100,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-1",
						},
						CheckpointTimeTick: 0,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-2",
						},
						CheckpointTimeTick: 8,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-3",
						},
						CheckpointTimeTick: 101,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
		"vchannel-2": {
			Vchannel: "vchannel-2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 100,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-1",
						},
						CheckpointTimeTick: 0,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
	}

	err = catalog.SaveVChannels(ctx, channel1, vchannelMetas)
	assert.NoError(t, err)

	vchannels, err = catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 2)
	assert.NoError(t, err)
	for _, vchannel := range vchannels {
		switch vchannel.Vchannel {
		case "vchannel-1":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 3)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].Schema.Name, "collection-2")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].CheckpointTimeTick, uint64(8))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].Schema.Name, "collection-3")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].CheckpointTimeTick, uint64(101))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		case "vchannel-2":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		}
	}

	vchannelMetas["vchannel-2"].State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	err = catalog.SaveVChannels(ctx, channel1, vchannelMetas)
	assert.NoError(t, err)

	vchannels, err = catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 2)
	assert.NoError(t, err)
	for _, vchannel := range vchannels {
		switch vchannel.Vchannel {
		case "vchannel-1":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 3)
		case "vchannel-2":
			assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.GetState())
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		}
	}
}

func TestCatalogSalvageCheckpoint(t *testing.T) {
	ctx := context.Background()

	t.Run("save_and_get_success", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		cp := &commonpb.ReplicateCheckpoint{
			ClusterId: "source-cluster",
			Pchannel:  "source-cluster-rootcoord-dml_0",
		}
		cpBytes, err := proto.Marshal(cp)
		assert.NoError(t, err)

		kv.EXPECT().Save(mock.Anything, mock.Anything, string(cpBytes)).Return(nil)
		err = catalog.SaveSalvageCheckpoint(ctx, "p1", cp)
		assert.NoError(t, err)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"streamingnode-meta/wal/p1/salvage-checkpoint/source-cluster"},
			[]string{string(cpBytes)},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Len(t, checkpoints, 1)
		assert.Equal(t, "source-cluster", checkpoints[0].ClusterId)
		assert.Equal(t, "source-cluster-rootcoord-dml_0", checkpoints[0].Pchannel)
	})

	t.Run("save_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("etcd error"))
		err := catalog.SaveSalvageCheckpoint(ctx, "p1", &commonpb.ReplicateCheckpoint{ClusterId: "c1"})
		assert.Error(t, err)
	})

	t.Run("get_load_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, errors.New("etcd error"))
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.Error(t, err)
		assert.Nil(t, checkpoints)
	})

	t.Run("get_unmarshal_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"key"},
			[]string{"invalid-proto-bytes"},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.Error(t, err)
		assert.Nil(t, checkpoints)
	})

	t.Run("get_empty", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Empty(t, checkpoints)
	})

	t.Run("get_multiple_clusters", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		cp1 := &commonpb.ReplicateCheckpoint{ClusterId: "cluster-a"}
		cp2 := &commonpb.ReplicateCheckpoint{ClusterId: "cluster-b"}
		bytes1, _ := proto.Marshal(cp1)
		bytes2, _ := proto.Marshal(cp2)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"key1", "key2"},
			[]string{string(bytes1), string(bytes2)},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Len(t, checkpoints, 2)
	})
}

func TestBuildPrefixAndKey(t *testing.T) {
	// Prefix functions
	assert.Equal(t, "streamingnode-meta/wal/p1/", buildWALPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/", buildWALPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/", buildSegmentAssignmentPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/", buildSegmentAssignmentPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/", buildVChannelPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/", buildVChannelPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/salvage-checkpoint/", buildSalvageCheckpointPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/salvage-checkpoint/", buildSalvageCheckpointPrefix("p2"))

	// Key functions
	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/1", buildSegmentAssignmentKey("p1", 1))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/2", buildSegmentAssignmentKey("p2", 2))

	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/v1", buildVChannelKey("p1", "v1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/v2", buildVChannelKey("p2", "v2"))
	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/v1/schema/100", buildVChannelSchemaKey("p1", "v1", 100))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/v2/schema/200", buildVChannelSchemaKey("p2", "v2", 200))

	assert.Equal(t, "streamingnode-meta/wal/p1/consume-checkpoint", buildConsumeCheckpointKey("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/consume-checkpoint", buildConsumeCheckpointKey("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/salvage-checkpoint/cluster-a", buildSalvageCheckpointPath("p1", "cluster-a"))
	assert.Equal(t, "streamingnode-meta/wal/p2/salvage-checkpoint/cluster-b", buildSalvageCheckpointPath("p2", "cluster-b"))
}
