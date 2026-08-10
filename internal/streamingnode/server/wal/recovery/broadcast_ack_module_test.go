package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestBroadcastAckWaitsUntilOnlyItsRefRemains(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.SwitchIntoMetaAndData()
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	record := messageack.NewRecord(consumePointFromMessage(msg), nil)
	other := record.Retain()

	module.ObserveMessage(context.Background(), messageack.NewMessage(msg, record))

	require.Len(t, scheduler.tasks, 1)
	assert.Equal(t, int64(3), record.RefCount())
	require.ErrorIs(t, scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay)

	record.Seal()
	require.ErrorIs(t, scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay)
	other.Done()
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, record.Completed())
}

func TestBroadcastAckRetriesSameQueueHeadAfterFailure(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.SwitchIntoMetaAndData()
	attempts := 0
	module.ack = func(context.Context, message.ImmutableMessage) error {
		attempts++
		if attempts == 1 {
			return errors.New("coordinator unavailable")
		}
		return nil
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	record := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: msg.TimeTick()}, nil)
	module.ObserveMessage(context.Background(), messageack.NewMessage(msg, record))
	record.Seal()

	assert.True(t, errors.Is(scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay))
	assert.Equal(t, int64(1), record.RefCount())
	assert.Len(t, scheduler.tasks, 1)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, 2, attempts)
	assert.True(t, record.Completed())
}

func TestBroadcastAckKeepsFIFOUntilQueueHeadSucceeds(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.SwitchIntoMetaAndData()
	var acked []uint64
	failFirst := true
	module.ack = func(_ context.Context, msg message.ImmutableMessage) error {
		if failFirst {
			failFirst = false
			return errors.New("coordinator unavailable")
		}
		acked = append(acked, msg.TimeTick())
		return nil
	}
	firstMsg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	secondMsg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v2"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2, PartitionIds: []int64{20}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	first := messageack.NewRecord(consumePointFromMessage(firstMsg), nil)
	second := messageack.NewRecord(consumePointFromMessage(secondMsg), nil)

	module.ObserveMessage(context.Background(), messageack.NewMessage(firstMsg, first))
	module.ObserveMessage(context.Background(), messageack.NewMessage(secondMsg, second))
	first.Seal()
	second.Seal()
	require.Len(t, scheduler.tasks, 1)

	assert.True(t, errors.Is(scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay))
	assert.Empty(t, acked)
	assert.False(t, first.Completed())
	assert.False(t, second.Completed())
	assert.Len(t, scheduler.tasks, 1)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, first.Completed())
	assert.False(t, second.Completed())
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.True(t, second.Completed())
	assert.Equal(t, []uint64{firstMsg.TimeTick(), secondMsg.TimeTick()}, acked)
}
