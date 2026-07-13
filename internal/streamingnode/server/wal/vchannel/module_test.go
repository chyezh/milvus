package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestVChannelRecoveryModuleObservesOnlyItsVChannel(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	require.NotNil(t, module.vchannelView)
	require.NotNil(t, module.transformLog)
	assert.Empty(t, module.segments)
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, module.ConsumeDirtySnapshots())

	result = module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleBuildsWALViewFromOwnedState(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	view, ok := module.BuildWALView(context.Background(), nil, nil)

	require.True(t, ok)
	assert.Equal(t, "p1", view.PChannel)
	assert.Equal(t, "v1", view.VChannel)
	assert.Equal(t, int64(100), view.CollectionID)
	assert.NotNil(t, view.LoadConfig)
	assert.Equal(t, uint64(20), view.BaseTransformTimeTick)
	assert.Equal(t, uint64(20), view.BaseGrowingTimeTick)
	require.NotNil(t, view.DeleteReplay)
	defer view.DeleteReplay.Close()
}

func TestVChannelRecoveryModuleRecoveryBarrierFlushesOwnedTransformLog(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	result := module.ObserveMessage(ctx, newTestRecoveryBarrierMessage(t, 30))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleReturnsOwnedDataFrontier(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	frontier := module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})

	require.NotNil(t, frontier)
	assert.Equal(t, uint64(0), frontier.TimeTick())
	assert.Nil(t, module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v2",
	}))
}

func newTestModule(t *testing.T, pchannel string, vchannel string) *VChannelRecoveryModule {
	t.Helper()
	module, err := NewModule(ModuleConfig{
		PChannel: pchannel,
		VChannel: vchannel,
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel:           vchannel,
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
			},
			LoadConfig: &streamingpb.VChannelLoadConfig{},
		},
		TransformLogMeta: &streamingpb.VChannelTransformLogMeta{},
		Runtime:          moduleapi.Runtime{},
	})
	require.NoError(t, err)
	return module
}

func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.DeleteRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
