package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestTransformLogFailedChunkWriteRetainsMessageRef(t *testing.T) {
	scheduler := &recordingScheduler{}
	store := &failingTransformLogStore{
		memoryStore: newMemoryStore(),
		err:         errors.New("object storage unavailable"),
	}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  1,
		Store:    store,
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	controller := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))

	transformLog.ObserveMessage(context.Background(), controller)
	controller.Seal()
	require.Len(t, scheduler.tasks, 1)

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.False(t, controller.Completed())

	store.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, controller.Completed())
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	assert.True(t, transformLog.HasDirty())
}

func TestTransformLogBarrierRefCompletesWithoutMaterialization(t *testing.T) {
	scheduler := &recordingScheduler{}
	materializer := &recordingMaterializer{}
	transformLog := New(Config{
		VChannel:     "v1",
		MaxRows:      100,
		Store:        newMemoryStore(),
		Materializer: materializer,
		Runtime:      moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	deleteMessage := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	barrierMessage := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))

	transformLog.ObserveMessage(context.Background(), deleteMessage)
	deleteMessage.Seal()
	transformLog.ObserveMessage(context.Background(), barrierMessage)
	barrierMessage.Seal()
	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &transformFlushTask{}, scheduler.tasks[0])
	assert.IsType(t, &transformMaterializeTask{}, scheduler.tasks[1])

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, deleteMessage.Completed())
	assert.True(t, barrierMessage.Completed())
	assert.Empty(t, materializer.requests)
}

func TestTransformLogMultiChunkFlushReleasesRefsByDurablePrefix(t *testing.T) {
	scheduler := &recordingScheduler{}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  1,
		Store:    newMemoryStore(),
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	transformLog.SwitchIntoMetaAndData()
	first := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	second := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 11))
	barrier := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))

	transformLog.ObserveMessage(context.Background(), first)
	first.Seal()
	transformLog.ObserveMessage(context.Background(), second)
	second.Seal()
	transformLog.ObserveMessage(context.Background(), barrier)
	barrier.Seal()
	require.GreaterOrEqual(t, len(scheduler.tasks), 3)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, first.Completed())
	assert.False(t, second.Completed())
	assert.False(t, barrier.Completed())
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())

	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.True(t, second.Completed())
	assert.True(t, barrier.Completed())
	assert.Equal(t, uint64(11), transformLog.SnapshotMeta().GetCheckpointTimeTick())
}

func TestTransformLogRegistersBarrierRefBeforeConcurrentFlushCommit(t *testing.T) {
	store := &blockingTransformLogWriteStore{
		memoryStore:  newMemoryStore(),
		writeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
	}
	transformLog := New(Config{
		VChannel: "v1",
		MaxRows:  100,
		Store:    store,
	})
	transformLog.SwitchIntoMetaAndData()
	deleteMessage := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	transformLog.ObserveMessage(context.Background(), deleteMessage)
	deleteMessage.Seal()

	type flushOutcome struct {
		result flushResult
		err    error
	}
	flushDone := make(chan flushOutcome, 1)
	go func() {
		result, err := transformLog.flush(context.Background(), flushOption{TargetTimeTick: 10})
		flushDone <- flushOutcome{result: result, err: err}
	}()
	<-store.writeStarted

	barrierController := newRefCountedTransformMessage(newTransformLogTestManualFlushMessage(t, 20))
	barrierMessage := &blockingRefCountedMessage{
		ImmutableMessage: barrierController,
		controller:       barrierController,
		retainStarted:    make(chan struct{}),
		releaseRetain:    make(chan struct{}),
	}
	observeDone := make(chan struct{})
	go func() {
		transformLog.ObserveMessage(context.Background(), barrierMessage)
		close(observeDone)
	}()
	<-barrierMessage.retainStarted

	close(store.releaseWrite)
	var flush flushResult
	select {
	case outcome := <-flushDone:
		require.NoError(t, outcome.err)
		flush = outcome.result
		releaseMessages(flush.CompletedMessages)
	case <-time.After(100 * time.Millisecond):
	}
	close(barrierMessage.releaseRetain)
	if !flush.Started {
		outcome := <-flushDone
		require.NoError(t, outcome.err)
		flush = outcome.result
		releaseMessages(flush.CompletedMessages)
	}
	<-observeDone
	barrierController.Seal()

	assert.True(t, deleteMessage.Completed())
	assert.True(t, barrierController.Completed())
}

type failingTransformLogStore struct {
	*memoryStore
	err error
}

type blockingRefCountedMessage struct {
	message.ImmutableMessage
	controller    message.RefCountedImmutableMessageController
	retainStarted chan struct{}
	releaseRetain chan struct{}
}

func (m *blockingRefCountedMessage) Retain() message.RetainedImmutableMessage {
	close(m.retainStarted)
	<-m.releaseRetain
	return m.controller.Retain()
}

func newRefCountedTransformMessage(raw message.ImmutableMessage) message.RefCountedImmutableMessageController {
	return message.NewRefCountedImmutableMessage(raw, nil)
}

type blockingTransformLogWriteStore struct {
	*memoryStore
	writeStarted chan struct{}
	releaseWrite chan struct{}
}

func (s *blockingTransformLogWriteStore) WriteTransformLogChunk(
	ctx context.Context,
	vchannel string,
	chunk *streamingpb.TransformLogChunk,
) error {
	close(s.writeStarted)
	select {
	case <-s.releaseWrite:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.memoryStore.WriteTransformLogChunk(ctx, vchannel, chunk)
}

func (s *failingTransformLogStore) WriteTransformLogChunk(
	ctx context.Context,
	vchannel string,
	chunk *streamingpb.TransformLogChunk,
) error {
	if s.err != nil {
		return s.err
	}
	return s.memoryStore.WriteTransformLogChunk(ctx, vchannel, chunk)
}
