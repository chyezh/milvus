package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestPChannelRecoveryManagerCreatesAndRoutesVChannelModules(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()

	result := manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Nil(t, manager.Module("v2"))

	result = manager.ObserveMessage(ctx, newTestCreateCollectionMessage(t, "v2", 20))
	require.NotNil(t, result.Meta)
	require.NotNil(t, manager.Module("v2"))
	assert.True(t, manager.Module("v2").metaAndData)

	snapshots := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, snapshots)
	assert.Contains(t, dirtySnapshotVChannels(snapshots), "v2")
}

func TestPChannelRecoveryManagerBroadcastsPChannelMessages(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10))
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 11))

	result := manager.ObserveMessage(ctx, newTestRecoveryBarrierMessage(t, 20))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestPChannelRecoveryManagerKeepsInFlightDirtyVChannelIndexed(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")

	manager.ObserveMessage(ctx, newTestCreateCollectionMessage(t, "v3", 20))
	first := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, first)
	assert.Contains(t, dirtySnapshotVChannels(first), "v3")

	second := manager.ConsumeDirtySnapshots()
	require.NotEmpty(t, second)
	assert.Contains(t, dirtySnapshotVChannels(second), "v3")

	for _, snapshot := range second {
		snapshot.MarkPersisted()
	}
	assert.Empty(t, manager.ConsumeDirtySnapshots())
}

func TestPChannelRecoveryManagerAggregatesDataFrontier(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1", "v2")
	manager.SwitchIntoMetaAndData()
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 10))

	v1Frontier := manager.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})
	require.NotNil(t, v1Frontier)
	assert.Equal(t, uint64(0), v1Frontier.TimeTick())

	v2Frontier := manager.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v2",
	})
	require.NotNil(t, v2Frontier)
	assert.NotZero(t, v2Frontier.TimeTick())

	allFrontier := manager.DataFrontier(moduleapi.Scope{
		Type: moduleapi.ScopeAll,
		Kind: moduleapi.DataProgressDurable,
	})
	require.NotNil(t, allFrontier)
	assert.Equal(t, uint64(0), allFrontier.TimeTick())
}

func TestPChannelRecoveryManagerBuildsWALViewForIndexedVChannel(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")
	manager.SwitchIntoMetaAndData()
	manager.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	view, ok := manager.BuildWALView(ctx, "v1", nil, nil)

	require.True(t, ok)
	assert.Equal(t, "p1", view.PChannel)
	assert.Equal(t, "v1", view.VChannel)
	assert.Equal(t, int64(100), view.CollectionID)
	assert.Equal(t, uint64(20), view.BaseTransformTimeTick)
	require.NotNil(t, view.DeleteReplay)
	defer view.DeleteReplay.Close()
}

func TestPChannelRecoveryManagerProvidesTransformLogStream(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t, "p1", "v1")

	stream, err := manager.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	require.NotNil(t, stream)
	assert.NoError(t, stream.Close())

	_, err = manager.AcquireStream(ctx, "other")
	assert.Error(t, err)
}

func newTestManager(t *testing.T, pchannel string, vchannels ...string) *PChannelRecoveryManager {
	t.Helper()
	metas := make(map[string]*streamingpb.VChannelMeta, len(vchannels))
	for _, vchannel := range vchannels {
		metas[vchannel] = newTestVChannelMeta(vchannel)
	}
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:          pchannel,
		VChannelMetas:     metas,
		TransformLogMetas: map[string]*streamingpb.VChannelTransformLogMeta{},
		Runtime:           moduleapi.Runtime{},
	})
	require.NoError(t, err)
	return manager
}

func newTestVChannelMeta(vchannel string) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:           vchannel,
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "c100"},
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				},
			},
		},
		LoadConfig: &streamingpb.VChannelLoadConfig{},
	}
}

func newTestCreateCollectionMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{
				10,
			},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{Name: "c100"},
		}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func dirtySnapshotVChannels(snapshots []moduleapi.DirtySnapshot) []string {
	vchannels := make([]string, 0)
	for _, snapshot := range snapshots {
		if snapshot.ModuleName() != moduleapi.ModuleNameVChannel {
			continue
		}
		meta, ok := snapshot.Payload().(*streamingpb.VChannelMeta)
		if !ok {
			continue
		}
		vchannels = append(vchannels, proto.Clone(meta).(*streamingpb.VChannelMeta).GetVchannel())
	}
	return vchannels
}
