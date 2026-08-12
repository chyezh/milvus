package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
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
	controller := message.NewRefCountedImmutableMessage(msg, nil)
	other := controller.Retain()

	module.ObserveMessage(context.Background(), controller)

	require.Len(t, scheduler.tasks, 1)
	require.ErrorIs(t, scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay)

	controller.Seal()
	require.ErrorIs(t, scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay)
	other.Release()
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Panics(t, func() { _ = controller.TimeTick() })
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
	controller := message.NewRefCountedImmutableMessage(msg, nil)
	module.ObserveMessage(context.Background(), controller)
	controller.Seal()

	assert.True(t, errors.Is(scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay))
	assert.NotPanics(t, func() { _ = controller.TimeTick() })
	assert.Len(t, scheduler.tasks, 1)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, 2, attempts)
	assert.Panics(t, func() { _ = controller.TimeTick() })
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
	firstTimeTick := firstMsg.TimeTick()
	secondTimeTick := secondMsg.TimeTick()
	first := message.NewRefCountedImmutableMessage(firstMsg, nil)
	second := message.NewRefCountedImmutableMessage(secondMsg, nil)

	module.ObserveMessage(context.Background(), first)
	module.ObserveMessage(context.Background(), second)
	first.Seal()
	second.Seal()
	require.Len(t, scheduler.tasks, 1)

	assert.True(t, errors.Is(scheduler.tasks[0].Execute(context.Background()), nodescheduler.ErrDelay))
	assert.Empty(t, acked)
	assert.NotPanics(t, func() { _ = first.TimeTick() })
	assert.NotPanics(t, func() { _ = second.TimeTick() })
	assert.Len(t, scheduler.tasks, 1)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Panics(t, func() { _ = first.TimeTick() })
	assert.NotPanics(t, func() { _ = second.TimeTick() })
	require.Len(t, scheduler.tasks, 2)
	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.Panics(t, func() { _ = second.TimeTick() })
	assert.Equal(t, []uint64{firstTimeTick, secondTimeTick}, acked)
}
