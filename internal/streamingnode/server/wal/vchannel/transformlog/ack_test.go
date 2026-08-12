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
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
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
	owner := newRefCountedTransformMessage(newTransformLogTestDeleteMessage(t, 10))
	probe := owner.Clone()

	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(owner, owner.Message()))
	owner.Release()
	require.Len(t, scheduler.tasks, 1)

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })

	store.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	probe.Release()
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())
	assert.True(t, transformLog.HasDirty())
}

func TestTransformLogDoesNotCloneUnrelatedMessage(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	transformLog.SwitchIntoMetaAndData()
	raw := message.CreateTestTimeTickSyncMessage(t, 1, 10, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	base := newRefCountedTransformMessage(raw)
	owner := &countingTransformOwner{owner: base}

	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(owner, raw))
	owner.Release()

	assert.Zero(t, owner.cloneCount)
	assert.Panics(t, func() { _ = owner.Message() })
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

	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(deleteMessage, deleteMessage.Message()))
	deleteMessage.Release()
	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(barrierMessage, barrierMessage.Message()))
	barrierMessage.Release()
	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &transformFlushTask{}, scheduler.tasks[0])
	assert.IsType(t, &transformMaterializeTask{}, scheduler.tasks[1])

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Panics(t, func() { _ = deleteMessage.Message() })
	assert.Panics(t, func() { _ = barrierMessage.Message() })
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
	firstProbe := first.Clone()
	secondProbe := second.Clone()
	barrierProbe := barrier.Clone()

	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(first, first.Message()))
	first.Release()
	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(second, second.Message()))
	second.Release()
	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(barrier, barrier.Message()))
	barrier.Release()
	require.GreaterOrEqual(t, len(scheduler.tasks), 3)

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.NotPanics(t, func() { _ = firstProbe.Message() })
	assert.NotPanics(t, func() { _ = secondProbe.Message().TimeTick() })
	assert.NotPanics(t, func() { _ = barrierProbe.Message().TimeTick() })
	assert.Equal(t, uint64(10), transformLog.SnapshotMeta().GetCheckpointTimeTick())

	require.NoError(t, scheduler.tasks[1].Execute(context.Background()))
	assert.NotPanics(t, func() { _ = secondProbe.Message() })
	assert.NotPanics(t, func() { _ = barrierProbe.Message() })
	firstProbe.Release()
	secondProbe.Release()
	barrierProbe.Release()
	assert.Panics(t, func() { _ = second.Message() })
	assert.Panics(t, func() { _ = barrier.Message() })
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
	transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage(deleteMessage, deleteMessage.Message()))
	deleteMessage.Release()

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
	barrierMessage := &blockingMessageOwner{
		owner:         barrierController,
		retainStarted: make(chan struct{}),
		releaseRetain: make(chan struct{}),
	}
	observeDone := make(chan struct{})
	go func() {
		transformLog.ObserveDataMessage(context.Background(), message.NewOwnedMessage[message.ImmutableMessage](barrierMessage, barrierMessage.Message()))
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
	barrierController.Release()

	assert.Panics(t, func() { _ = deleteMessage.Message() })
	assert.Panics(t, func() { _ = barrierController.Message() })
}

type failingTransformLogStore struct {
	*memoryStore
	err error
}

type blockingMessageOwner struct {
	owner         message.RefCountedImmutableMessageOwner
	retainStarted chan struct{}
	releaseRetain chan struct{}
}

func (m *blockingMessageOwner) Message() message.ImmutableMessage {
	return m.owner.Message()
}

func (m *blockingMessageOwner) Clone() message.RetainedImmutableMessage {
	close(m.retainStarted)
	<-m.releaseRetain
	return m.owner.Clone()
}

func (m *blockingMessageOwner) Release() {
	m.owner.Release()
}

func newRefCountedTransformMessage(raw message.ImmutableMessage) message.RefCountedImmutableMessageOwner {
	return message.NewRefCountedImmutableMessageOwner(raw, nil)
}

type countingTransformOwner struct {
	owner      message.RefCountedImmutableMessageOwner
	cloneCount int
}

func (o *countingTransformOwner) Message() message.ImmutableMessage {
	return o.owner.Message()
}

func (o *countingTransformOwner) Clone() message.RetainedImmutableMessage {
	o.cloneCount++
	return o.owner.Clone()
}

func (o *countingTransformOwner) Release() {
	o.owner.Release()
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
