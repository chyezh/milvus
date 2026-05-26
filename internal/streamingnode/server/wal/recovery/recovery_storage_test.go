package recovery

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	mock_walimpls "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestRecoveryStorageCheckpointManagerDirtySnapshot(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.modules = []moduleapi.Module{
		&recordingModule{result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return 2 }),
		}},
	}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(2).
		WithLastConfirmed(rmq.NewRmqID(2)).
		IntoImmutableMessage(rmq.NewRmqID(2))

	rs.observeMessage(context.Background(), msg)

	assert.True(t, rs.checkpointManager.HasDirty())
	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.True(t, snapshot.CheckpointDirty)
	assert.True(t, rmq.NewRmqID(2).EQ(snapshot.Checkpoint.MessageID))
	assert.Equal(t, uint64(2), snapshot.Checkpoint.TimeTick)
	assert.False(t, rs.checkpointManager.HasDirty())
}

func TestRecoveryStorageCheckpointPersistDoesNotUseBusinessScheduler(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, "test_channel", mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
		return rmq.NewRmqID(10).EQ(message.MustUnmarshalMessageID(checkpoint.GetMessageId())) &&
			checkpoint.GetTimeTick() == 10 &&
			checkpoint.GetDataCheckpoint() != nil &&
			rmq.NewRmqID(5).EQ(message.MustUnmarshalMessageID(checkpoint.GetDataCheckpoint().GetMessageId())) &&
			checkpoint.GetDataCheckpoint().GetTimeTick() == 5
	})).Return(nil).Once()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, mock.MatchedBy(func(id message.MessageID) bool {
		return rmq.NewRmqID(5).EQ(id)
	})).Return(nil).Once()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	rs.truncator = truncator
	rs.taskScheduler.Close()

	snapshot := &RecoverySnapshot{
		Checkpoint: &WALCheckpoint{
			MessageID: rmq.NewRmqID(10),
			TimeTick:  10,
			DataCheckpoint: &utility.WALConsumeCheckpoint{
				MessageID: rmq.NewRmqID(5),
				TimeTick:  5,
			},
		},
		CheckpointDirty: true,
	}

	require.NoError(t, rs.persistCheckpointSnapshot(context.Background(), snapshot, false))
}

func TestRecoveryStorageCheckpointPersistNotifiesModules(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, "test_channel", mock.Anything).Return(nil).Once()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	module := &recordingModule{}
	rs.modules = []moduleapi.Module{module}
	snapshot := &RecoverySnapshot{
		Checkpoint: &WALCheckpoint{
			MessageID: rmq.NewRmqID(10),
			TimeTick:  10,
			DataCheckpoint: &utility.WALConsumeCheckpoint{
				MessageID: rmq.NewRmqID(5),
				TimeTick:  5,
			},
		},
		CheckpointDirty: true,
	}

	require.NoError(t, rs.persistCheckpointSnapshot(context.Background(), snapshot, false))

	require.Equal(t, [][2]uint64{{10, 5}}, module.persistedCheckpoints)
}

func TestRecoveryStorageObservesNonPersistedTimeTick(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	rs.modules = []moduleapi.Module{
		&recordingModule{result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
			Data: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
		}},
	}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithNotPersisted().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))
	require.False(t, msg.IsPersisted())

	rs.observeMessage(context.Background(), msg)
	rs.checkpointManager.TryAdvanceMetaCheckpoint()
	rs.checkpointManager.TryAdvanceDataCheckpoint()

	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.TimeTick)
	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.DataCheckpoint.TimeTick)
}

func TestRecoveryStorageMetaCheckpointWaitsForPersistedMetaBarrier(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	metaFrontier := newTestAtomicUint64(1)
	module := &recordingModule{
		result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return metaFrontier.Load() }),
		},
	}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeMessage(context.Background(), msg)
	assert.True(t, rmq.NewRmqID(1).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(1), rs.checkpoint.TimeTick)

	snapshot := rs.consumeDirtySnapshot()
	assert.Nil(t, snapshot)
	assert.Equal(t, 1, module.persistRequests)

	metaFrontier.Store(5)
	rs.NotifyBarrierUpdated()

	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.TimeTick)
}

func updateCheckpointWithImmediateMetaBarrier(rs *recoveryStorageImpl, msg message.ImmutableMessage) {
	timetick := msg.TimeTick()
	rs.updateCheckpoint(msg, walcheckpoint.BarrierFunc(func() uint64 { return timetick }))
}

type recordingModule struct {
	observed             []uint64
	persistRequests      int
	persistedCheckpoints [][2]uint64
	result               moduleapi.ObserveResult
}

func newTestAtomicUint64(value uint64) *atomic.Uint64 {
	frontier := &atomic.Uint64{}
	frontier.Store(value)
	return frontier
}

func (m *recordingModule) Name() string {
	return "recording"
}

func (m *recordingModule) ObserveMessage(_ context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	m.observed = append(m.observed, msg.TimeTick())
	return m.result
}

func (m *recordingModule) SwitchIntoMetaAndData() moduleapi.Snapshot {
	return nil
}

func (m *recordingModule) RequirePersist() {
	m.persistRequests++
}

func (m *recordingModule) NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64) {
	m.persistedCheckpoints = append(m.persistedCheckpoints, [2]uint64{metaTimeTick, dataTimeTick})
}

func TestRecoveryStorageCheckpointDirtyTriggersModulePersist(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	module := &recordingModule{}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))
	rs.updateCheckpoint(msg, walcheckpoint.BarrierFunc(func() uint64 { return 5 }))

	require.True(t, rs.checkpointManager.HasDirty())
	require.Equal(t, 0, rs.dirtyCounter)

	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, 1, module.persistRequests)
}

func TestRecoveryStorageDataCheckpointDirtyTriggersDataPersist(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(5),
		TimeTick:  5,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	module := &recordingModule{}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))
	rs.updateDataCheckpoint(msg, walcheckpoint.BarrierFunc(func() uint64 { return 5 }))

	require.True(t, rs.checkpointManager.HasDirty())
	require.Equal(t, 0, rs.dirtyCounter)

	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, 1, module.persistRequests)
}

func TestRecoveryStorageDoesNotFilterPersistedMessageByTimeTick(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	})
	module := &recordingModule{}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeMessage(context.Background(), msg)

	assert.Equal(t, []uint64{5}, module.observed)
	assert.True(t, rmq.NewRmqID(10).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(10), rs.checkpoint.TimeTick)
}

func TestRecoveryStorageRegistersReturnedDataBarrierWithoutModeCheck(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	module := &recordingModule{
		result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
			Data: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
		},
	}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeMessage(context.Background(), msg)

	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.DataCheckpoint.TimeTick)
}

func TestRecoveryStorageStartsDataScannerFromDataCheckpoint(t *testing.T) {
	channel := types.PChannelInfo{Name: "test_channel"}
	rs := newRecoveryStorage(channel, &WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(3),
			TimeTick:  3,
		},
	})
	builder := &recordingRecoveryStreamBuilder{
		channel: channel,
		stream:  newBlockingRecoveryStream(),
	}

	rs.startDataLiveScanner(builder)
	defer rs.backgroundTaskNotifier.Cancel()
	defer builder.stream.Close()
	defer rs.taskScheduler.Close()

	require.Len(t, builder.builds, 1)
	assert.True(t, rmq.NewRmqID(3).EQ(builder.builds[0].StartCheckpoint))
	assert.Equal(t, uint64(0), builder.builds[0].EndTimeTick)
}

func TestRecoveryStorageMetaAndDataObserveRegistersDataBarrier(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	dataFrontier := newTestAtomicUint64(1)
	module := &recordingModule{
		result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
			Data: walcheckpoint.BarrierFunc(func() uint64 { return dataFrontier.Load() }),
		},
	}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeMessage(context.Background(), msg)
	assert.Nil(t, rs.checkpoint.DataCheckpoint)

	dataFrontier.Store(5)
	rs.checkpointManager.TryAdvanceMetaCheckpoint()
	rs.checkpointManager.TryAdvanceDataCheckpoint()
	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.DataCheckpoint.TimeTick)
}

func TestRecoveryStorageDataScannerObserveIgnoresMetaCheckpoint(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	dataFrontier := newTestAtomicUint64(1)
	module := &recordingModule{
		result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return 5 }),
			Data: walcheckpoint.BarrierFunc(func() uint64 { return dataFrontier.Load() }),
		},
	}
	rs.modules = []moduleapi.Module{module}
	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeDataScannerMessage(context.Background(), msg)
	assert.True(t, rmq.NewRmqID(1).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(1), rs.checkpoint.DataCheckpoint.TimeTick)

	dataFrontier.Store(5)
	rs.checkpointManager.TryAdvanceDataCheckpoint()

	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.DataCheckpoint.TimeTick)
}

func TestRecoveryStorageDataScannerObservesMetaAfterMetaCheckpoint(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel"}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
	})
	metaFrontier := newTestAtomicUint64(1)
	dataFrontier := newTestAtomicUint64(1)
	module := &recordingModule{
		result: moduleapi.ObserveResult{
			Meta: walcheckpoint.BarrierFunc(func() uint64 { return metaFrontier.Load() }),
			Data: walcheckpoint.BarrierFunc(func() uint64 { return dataFrontier.Load() }),
		},
	}
	rs.modules = []moduleapi.Module{module}

	msg := message.NewTimeTickMessageBuilderV1().
		WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	rs.observeDataScannerMessage(context.Background(), msg)
	assert.Equal(t, []uint64{5}, module.observed)
	assert.True(t, rmq.NewRmqID(1).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(1), rs.checkpoint.TimeTick)
	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(1).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(1), rs.checkpoint.DataCheckpoint.TimeTick)
	assert.Equal(t, 1, rs.dirtyCounter)

	snapshot := rs.consumeDirtySnapshot()
	assert.Nil(t, snapshot)
	assert.Equal(t, 1, module.persistRequests)
	metaFrontier.Store(5)
	dataFrontier.Store(5)
	rs.NotifyBarrierUpdated()

	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.TimeTick)
	require.NotNil(t, rs.checkpoint.DataCheckpoint)
	assert.True(t, rmq.NewRmqID(5).EQ(rs.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(5), rs.checkpoint.DataCheckpoint.TimeTick)
}

type recordingRecoveryStreamBuilder struct {
	channel types.PChannelInfo
	stream  RecoveryStream
	builds  []BuildRecoveryStreamParam
}

func (b *recordingRecoveryStreamBuilder) WALName() message.WALName {
	return message.WALNameRocksmq
}

func (b *recordingRecoveryStreamBuilder) Channel() types.PChannelInfo {
	return b.channel
}

func (b *recordingRecoveryStreamBuilder) Build(param BuildRecoveryStreamParam) RecoveryStream {
	b.builds = append(b.builds, param)
	return b.stream
}

func (b *recordingRecoveryStreamBuilder) RWWALImpls() walimpls.WALImpls {
	return nil
}

type blockingRecoveryStream struct {
	ch     chan message.ImmutableMessage
	closed atomic.Bool
}

func newBlockingRecoveryStream() *blockingRecoveryStream {
	return &blockingRecoveryStream{ch: make(chan message.ImmutableMessage)}
}

func (s *blockingRecoveryStream) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

func (s *blockingRecoveryStream) Error() error {
	return nil
}

func (s *blockingRecoveryStream) TxnBuffer() *utility.TxnBuffer {
	return nil
}

func (s *blockingRecoveryStream) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
	return nil
}
