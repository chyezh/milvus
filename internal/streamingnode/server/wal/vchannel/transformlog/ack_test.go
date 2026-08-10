package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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
	msg := newTransformLogTestDeleteMessage(t, 10)
	record := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: msg.TimeTick()}, nil)

	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(msg, record))
	record.Seal()
	require.Len(t, scheduler.tasks, 1)

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.False(t, record.Completed())

	store.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, record.Completed())
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
	deleteMsg := newTransformLogTestDeleteMessage(t, 10)
	deleteRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)
	barrierMsg := newTransformLogTestManualFlushMessage(t, 20)
	barrierRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)

	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(deleteMsg, deleteRecord))
	deleteRecord.Seal()
	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(barrierMsg, barrierRecord))
	barrierRecord.Seal()
	require.Len(t, scheduler.tasks, 2)
	assert.IsType(t, &transformFlushTask{}, scheduler.tasks[0])
	assert.IsType(t, &transformMaterializeTask{}, scheduler.tasks[1])

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, deleteRecord.Completed())
	assert.True(t, barrierRecord.Completed())
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
	firstMsg := newTransformLogTestDeleteMessage(t, 10)
	secondMsg := newTransformLogTestDeleteMessage(t, 11)
	barrierMsg := newTransformLogTestManualFlushMessage(t, 20)
	first := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)
	second := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 11}, nil)
	barrier := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)

	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(firstMsg, first))
	first.Seal()
	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(secondMsg, second))
	second.Seal()
	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(barrierMsg, barrier))
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
	deleteMsg := newTransformLogTestDeleteMessage(t, 10)
	deleteRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)
	transformLog.ObserveMessage(context.Background(), messageack.NewMessage(deleteMsg, deleteRecord))
	deleteRecord.Seal()

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

	innerBarrierRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)
	barrierRecord := &blockingRetainRecord{
		Record:        innerBarrierRecord,
		retainStarted: make(chan struct{}),
		releaseRetain: make(chan struct{}),
	}
	barrierMsg := newTransformLogTestManualFlushMessage(t, 20)
	observeDone := make(chan struct{})
	go func() {
		transformLog.ObserveMessage(context.Background(), messageack.NewMessage(barrierMsg, barrierRecord))
		close(observeDone)
	}()
	<-barrierRecord.retainStarted

	close(store.releaseWrite)
	var flush flushResult
	select {
	case outcome := <-flushDone:
		require.NoError(t, outcome.err)
		flush = outcome.result
		completeRefs(flush.CompletedRefs)
	case <-time.After(100 * time.Millisecond):
	}
	close(barrierRecord.releaseRetain)
	if !flush.Started {
		outcome := <-flushDone
		require.NoError(t, outcome.err)
		flush = outcome.result
		completeRefs(flush.CompletedRefs)
	}
	<-observeDone
	innerBarrierRecord.Seal()

	assert.True(t, deleteRecord.Completed())
	assert.True(t, innerBarrierRecord.Completed())
}

type failingTransformLogStore struct {
	*memoryStore
	err error
}

type blockingRetainRecord struct {
	messageack.Record
	retainStarted chan struct{}
	releaseRetain chan struct{}
}

func (r *blockingRetainRecord) Retain() messageack.Ref {
	close(r.retainStarted)
	<-r.releaseRetain
	return r.Record.Retain()
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
