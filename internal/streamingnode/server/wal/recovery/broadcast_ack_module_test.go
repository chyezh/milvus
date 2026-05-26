package recovery

import (
	"context"
	"testing"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/growing"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type captureAsyncTaskScheduler struct {
	tasks   []scheduler.Task
	handles []*captureTaskHandle
}

func (s *captureAsyncTaskScheduler) Submit(task scheduler.Task) scheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	handle := &captureTaskHandle{}
	s.handles = append(s.handles, handle)
	return handle
}

func (s *captureAsyncTaskScheduler) Notify() {}

type captureTaskHandle struct {
	done atomic.Bool
}

func (h *captureTaskHandle) Done() bool {
	return h.done.Load()
}

func (h *captureTaskHandle) MarkDone() {
	h.done.Store(true)
}

type recordingDurableFrontierView struct {
	collectionID  int64
	partitionID   int64
	vchannel      string
	allFrontier    bool
	frontier       *atomic.Uint64
}

func (v *recordingDurableFrontierView) CollectionDurableFrontier(collectionID int64) walcheckpoint.Barrier {
	v.collectionID = collectionID
	return walcheckpoint.BarrierFunc(func() uint64 {
		return v.frontier.Load()
	})
}

func (v *recordingDurableFrontierView) PartitionDurableFrontier(collectionID int64, partitionID int64) walcheckpoint.Barrier {
	v.collectionID = collectionID
	v.partitionID = partitionID
	return walcheckpoint.BarrierFunc(func() uint64 {
		return v.frontier.Load()
	})
}

func (v *recordingDurableFrontierView) VChannelDurableFrontier(vchannel string) walcheckpoint.Barrier {
	v.vchannel = vchannel
	return walcheckpoint.BarrierFunc(func() uint64 {
		return v.frontier.Load()
	})
}

func (v *recordingDurableFrontierView) AllDurableFrontier() walcheckpoint.Barrier {
	v.allFrontier = true
	return walcheckpoint.BarrierFunc(func() uint64 {
		return v.frontier.Load()
	})
}

func TestBroadcastAckModuleUsesDurableFrontierView(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	frontierView := &recordingDurableFrontierView{frontier: atomic.NewUint64(4)}
	module := newBroadcastAckModule("p1", frontierView, moduleapi.Runtime{Scheduler: taskScheduler})
	module.SwitchIntoMetaAndData()

	msg := newTestAckSyncUpDropCollectionMessage(10, 100, "v1")
	result := module.ObserveMessage(context.Background(), msg)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, int64(100), frontierView.collectionID)

	task := taskScheduler.tasks[0]
	assert.False(t, task.Precondition().Ready())
	frontierView.frontier.Store(10)
	assert.True(t, task.Precondition().Ready())
}

func TestBroadcastAckModuleUsesMessageSpecificDurableFrontiers(t *testing.T) {
	for name, testCase := range map[string]struct {
		msg                message.ImmutableMessage
		expectedCollection int64
		expectedPartition  int64
		expectedVChannel   string
		expectedAll        bool
	}{
		"drop collection": {
			msg:                newTestAckSyncUpDropCollectionMessage(10, 100, "v1"),
			expectedCollection: 100,
		},
		"drop partition": {
			msg:                newTestAckSyncUpDropPartitionMessage(10, 100, 200, "v1"),
			expectedCollection: 100,
			expectedPartition:  200,
		},
		"truncate collection": {
			msg:                newTestAckSyncUpTruncateCollectionMessage(10, 100, "v1"),
			expectedCollection: 100,
		},
		"flush all": {
			msg:         newTestAckSyncUpFlushAllMessage(10, "v1"),
			expectedAll: true,
		},
		"commit import": {
			msg:              newTestAckSyncUpCommitImportMessage(10, "v1"),
			expectedVChannel: "v1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			taskScheduler := &captureAsyncTaskScheduler{}
			frontierView := &recordingDurableFrontierView{frontier: atomic.NewUint64(4)}
			module := newBroadcastAckModule("p1", frontierView, moduleapi.Runtime{Scheduler: taskScheduler})
			module.SwitchIntoMetaAndData()

			result := module.ObserveMessage(context.Background(), testCase.msg)

			require.NotNil(t, result.Data)
			require.Len(t, taskScheduler.tasks, 1)
			assert.Equal(t, testCase.expectedCollection, frontierView.collectionID)
			assert.Equal(t, testCase.expectedPartition, frontierView.partitionID)
			assert.Equal(t, testCase.expectedVChannel, frontierView.vchannel)
			assert.Equal(t, testCase.expectedAll, frontierView.allFrontier)
			assert.False(t, taskScheduler.tasks[0].Precondition().Ready())

			frontierView.frontier.Store(10)
			assert.True(t, taskScheduler.tasks[0].Precondition().Ready())
		})
	}
}

func TestBroadcastAckModuleWaitsForDurableDropCollectionState(t *testing.T) {
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
			},
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
		},
	}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           100,
			PartitionId:            200,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
		},
	}
	taskScheduler := &captureAsyncTaskScheduler{}
	frontierView := growing.NewManager(vchannels, segments, nil)
	module := newBroadcastAckModule("p1", frontierView, moduleapi.Runtime{Scheduler: taskScheduler})
	module.SwitchIntoMetaAndData()

	msg := newTestAckSyncUpDropCollectionMessage(10, 100, "v1")
	result := module.ObserveMessage(context.Background(), msg)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)

	task := taskScheduler.tasks[0]
	assert.False(t, task.Precondition().Ready())
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	frontierView.VChannels()["v1"].MarkMetaPersisted(10)
	frontierView.VChannels()["v1"].MarkDataCheckpoint(10)
	frontierView.VChannels()["v1"].MarkDataPersisted(10)
	frontierView.Segments()[10].MarkMetaPersisted(10)
	frontierView.Segments()[10].MarkDataCheckpoint(10)
	frontierView.Segments()[10].MarkDataPersisted(10)
	assert.True(t, task.Precondition().Ready())
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	module.markAcked(10)
	assert.Equal(t, uint64(10), result.Data.TimeTick())
}

func TestBroadcastAckModuleCommitImportDoesNotWaitForEmptyGrowingWork(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	frontierView := growing.NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 0,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
			},
		},
	}, nil, nil)
	module := newBroadcastAckModule("p1", frontierView, moduleapi.Runtime{Scheduler: taskScheduler})
	module.SwitchIntoMetaAndData()

	msg := newTestAckSyncUpCommitImportMessage(10, "v1")
	result := module.ObserveMessage(context.Background(), msg)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.True(t, taskScheduler.tasks[0].Precondition().Ready())
}

func TestBroadcastAckModuleSerializesAckTasksWithPrecondition(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	module := newBroadcastAckModule("p1", nil, moduleapi.Runtime{Scheduler: taskScheduler})
	module.SwitchIntoMetaAndData()

	firstMsg := newTestAckSyncUpTimeTickMessage(10, "v1")
	_ = module.ObserveMessage(context.Background(), firstMsg)
	require.Len(t, taskScheduler.tasks, 1)
	assert.True(t, taskScheduler.tasks[0].Precondition().Ready())

	secondMsg := newTestAckSyncUpTimeTickMessage(11, "v1")
	_ = module.ObserveMessage(context.Background(), secondMsg)
	require.Len(t, taskScheduler.tasks, 2)
	require.Len(t, taskScheduler.handles, 2)
	assert.False(t, taskScheduler.tasks[1].Precondition().Ready())

	taskScheduler.handles[0].MarkDone()
	assert.True(t, taskScheduler.tasks[1].Precondition().Ready())
}

func newTestAckSyncUpDropCollectionMessage(
	timetick uint64,
	collectionID int64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}

func newTestAckSyncUpDropPartitionMessage(
	timetick uint64,
	collectionID int64,
	partitionID int64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{CollectionId: collectionID, PartitionId: partitionID}).
		WithBody(&msgpb.DropPartitionRequest{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}

func newTestAckSyncUpTruncateCollectionMessage(
	timetick uint64,
	collectionID int64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: collectionID}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}

func newTestAckSyncUpFlushAllMessage(
	timetick uint64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}

func newTestAckSyncUpCommitImportMessage(
	timetick uint64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{CollectionId: 100, JobId: 1}).
		WithBody(&message.CommitImportMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}

func newTestAckSyncUpTimeTickMessage(
	timetick uint64,
	vchannel string,
) message.ImmutableMessage {
	broadcast := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast()
	broadcast.OverwriteBroadcastHeader(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return msg
}
