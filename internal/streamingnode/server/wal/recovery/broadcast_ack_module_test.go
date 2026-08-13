package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestBroadcastAckReleasesOwnerAndWaitsForTrackerCompletion(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.retryDelay = time.Millisecond
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(msg)
	other := owner.Clone()

	module.Accept(owner, tracked)

	require.Empty(t, scheduler.snapshot())
	assert.Panics(t, func() { _ = owner.Message() })

	other.Release()
	task := scheduler.waitTask(t)
	require.NoError(t, task.Execute(context.Background()))
}

func TestBroadcastAckOnlyReleasesOwner(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.retryDelay = time.Millisecond
	module.ack = func(context.Context, message.ImmutableMessage) error { return nil }
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(msg)
	releaseOnly := &releaseOnlyMessageOwner{owner: owner}

	module.Accept(releaseOnly, tracked)

	assert.True(t, releaseOnly.released)
	task := scheduler.waitTask(t)
	require.NoError(t, task.Execute(context.Background()))
}

func TestBroadcastAckReleasesNonBroadcastOwnerImmediately(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 20, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(raw)

	module.Accept(owner, tracked)

	assert.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)
	assert.Empty(t, scheduler.snapshot())
	assert.Panics(t, func() { _ = owner.Message() })
}

func TestBroadcastAckRetriesSameQueueHeadAfterFailure(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(msg)
	module.Accept(owner, tracked)

	first := scheduler.waitTask(t)
	require.NoError(t, first.Execute(context.Background()))
	assert.Panics(t, func() { _ = owner.Message() })

	retry := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, retry.Execute(context.Background()))
	assert.Equal(t, 2, attempts)
	assert.Panics(t, func() { _ = owner.Message() })
}

func TestBroadcastAckKeepsFIFOUntilQueueHeadSucceeds(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
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
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	firstOwner, firstTracked := tracker.Track(firstMsg)
	secondOwner, secondTracked := tracker.Track(secondMsg)

	module.Accept(firstOwner, firstTracked)
	module.Accept(secondOwner, secondTracked)
	firstTask := scheduler.waitTask(t)

	require.NoError(t, firstTask.Execute(context.Background()))
	assert.Empty(t, acked)
	assert.Panics(t, func() { _ = firstOwner.Message() })
	assert.Panics(t, func() { _ = secondOwner.Message() })

	retryTask := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, retryTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = firstOwner.Message() })
	assert.Panics(t, func() { _ = secondOwner.Message() })
	secondTask := scheduler.waitTaskAfter(t, 2)
	require.NoError(t, secondTask.Execute(context.Background()))
	assert.Panics(t, func() { _ = secondOwner.Message() })
	assert.Equal(t, []uint64{firstTimeTick, secondTimeTick}, acked)
}

func TestBroadcastAckCloseCancelsPendingConsumerWait(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(msg)
	consumer := owner.Clone()

	module.Accept(owner, tracked)
	module.Close()
	consumer.Release()

	assert.Empty(t, scheduler.snapshot())
	assert.Zero(t, tracker.CompletedPoint().TimeTick)
}

func TestBroadcastAckCloseCancelsPendingRetry(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	module.retryDelay = time.Hour
	module.ack = func(context.Context, message.ImmutableMessage) error {
		return errors.New("coordinator unavailable")
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))
	tracker := messageack.NewTracker(utility.WALConsumeCheckpoint{}, nil)
	owner, tracked := tracker.Track(msg)
	module.Accept(owner, tracked)

	first := scheduler.waitTask(t)
	require.NoError(t, first.Execute(context.Background()))
	module.Close()

	assert.Len(t, scheduler.snapshot(), 1)
	assert.Zero(t, tracker.CompletedPoint().TimeTick)
}

type releaseOnlyMessageOwner struct {
	owner    message.OwnedImmutableMessage
	released bool
}

func (*releaseOnlyMessageOwner) Message() message.ImmutableMessage {
	panic("broadcast ack must not read the owner")
}

func (*releaseOnlyMessageOwner) Clone() message.RetainedImmutableMessage {
	panic("broadcast ack must not clone the owner")
}

func (o *releaseOnlyMessageOwner) Release() {
	o.released = true
	o.owner.Release()
}
