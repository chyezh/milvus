package growing

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSegmentView(t *testing.T) {
	msg := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			PartitionId:    1,
			SegmentId:      2,
			StorageVersion: storage.StorageV1,
			MaxSegmentSize: 100,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("vchannel-1").
		MustBuildMutable()

	id := rmq.NewRmqID(1)
	ts := uint64(12345)
	immutableMsg := msg.WithTimeTick(ts).WithLastConfirmed(id).IntoImmutableMessage(id)

	schema := &schemapb.CollectionSchema{Name: "create-segment-schema"}
	info := NewSegmentViewFromCreateSegmentMessage(message.MustAsImmutableCreateSegmentMessageV2(immutableMsg), schema)
	assert.Equal(t, int64(2), info.meta.SegmentId)
	assert.Same(t, schema, info.schema)
	assert.Equal(t, int64(1), info.meta.PartitionId)
	assert.Equal(t, storage.StorageV1, info.meta.StorageVersion)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, info.meta.State)
	assert.Equal(t, uint64(100), info.meta.Stat.MaxBinarySize)
	assert.NotNil(t, info.meta.PersistedStorage)
	assert.Equal(t, uint64(0), info.MetaTimeTick())

	ts += 1
	insert := newTestInsertMessage(ts, 2, 1, 1, 10)
	info.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])
	assert.True(t, info.dirty)
	snapshot := info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, ts, info.MetaTimeTick())
	assert.False(t, info.dirty)

	assert.Equal(t, uint64(10), snapshot.Stat.ModifiedBinarySize)
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)

	// duplicated non-txn insert should not dirty the segment again.
	info.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])
	assert.False(t, info.dirty)

	ts += 1
	info.Flush(context.Background(), ts)
	snapshot = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.True(t, info.dirty)
	info.MarkSnapshotPersisted(snapshot)
	assert.False(t, info.dirty)

	// idempotent
	info.Flush(context.Background(), ts)
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.False(t, info.dirty)

	// idempotent
	info.Flush(context.Background(), ts+1)
	assert.NotNil(t, snapshot)
	assert.Equal(t, snapshot.State, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED)
	assert.False(t, info.dirty)
}

func TestSegmentViewDirtySnapshotIncludesCurrentDataState(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     5,
			DataCheckpointTimeTick: 5,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{
					{FromTimeTick: 1, ToTimeTick: 5},
				},
			},
			Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
		},
		&schemapb.CollectionSchema{Name: "lagged-segment-data-schema"},
	)
	segment.appendPersistedStorage(&streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{
			{FromTimeTick: 6, ToTimeTick: 20},
		},
	})
	segment.MarkDataCheckpoint(20)

	insert := newTestInsertMessage(10, 2, 1, 1, 10)
	segment.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])
	snapshot := segment.ConsumeDirtyAndGetSnapshot()

	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), snapshot.GetDataCheckpointTimeTick())
	require.Len(t, snapshot.GetPersistedStorage().GetBinlogs(), 2)
	assert.Equal(t, uint64(20), snapshot.GetPersistedStorage().GetBinlogs()[1].GetToTimeTick())
}

func TestSegmentViewMarkSnapshotPersistedDoesNotRegressPersistedDataTimeTick(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{
					{FromTimeTick: 1, ToTimeTick: 4},
				},
			},
			Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
		&schemapb.CollectionSchema{Name: "stale-meta-snapshot-schema"},
	)
	segment.MarkMetaPersisted(4)
	segment.MarkDataPersisted(4)

	segment.MarkSnapshotPersisted(&streamingpb.SegmentAssignmentMeta{
		CollectionId:           100,
		PartitionId:            1,
		SegmentId:              2,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{FromTimeTick: 1, ToTimeTick: 10},
			},
		},
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
	})

	segment.MarkSnapshotPersisted(&streamingpb.SegmentAssignmentMeta{
		CollectionId:           100,
		PartitionId:            1,
		SegmentId:              2,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 4,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{FromTimeTick: 1, ToTimeTick: 4},
			},
		},
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
	})

	assert.Equal(t, uint64(10), segment.PersistedDataTimeTick())
}

func TestSegmentViewStaleDataSnapshotDoesNotClearDirtyTombstone(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
		&schemapb.CollectionSchema{Name: "dirty-tombstone-schema"},
	)

	segment.TryFinalizeTombstone()
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, segment.Meta().GetState())
	require.True(t, segment.HasDirty())

	segment.MarkSnapshotPersisted(&streamingpb.SegmentAssignmentMeta{
		CollectionId:           100,
		PartitionId:            1,
		SegmentId:              2,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
	})

	require.True(t, segment.HasDirty())
	snapshot := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetTombstoneTimeTick())
}

func TestSegmentViewTransitionsFlushedMetaToTombstonedAfterDataCatchesUp(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "segment-tombstone-schema"},
	)

	segment.Flush(context.Background(), 10)
	snapshot := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, snapshot.GetState())

	segment.MarkDataCheckpoint(10)
	snapshot = segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetDataCheckpointTimeTick())

	segment.TryFinalizeTombstone()
	snapshot = segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetTombstoneTimeTick())
}

func TestSegmentViewCoveredByTombstoneAcceptsAllPartitions(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          10,
			Vchannel:           "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10,
			Stat: &streamingpb.SegmentAssignmentStat{
				CreateSegmentTimeTick: 5,
			},
		},
		nil,
	)

	assert.True(t, segment.CoveredByTombstone("v1", common.AllPartitionsID, 10))
}

func TestSegmentViewDoesNotRecommitAlreadyDurableFlushedSegment(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "already-flushed-schema"},
		runtimeConfig{
			lifecycle:   &testSegmentLifecycleWriter{},
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			metaAndData: true,
		},
	)

	result := segment.Flush(context.Background(), 20)

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(10), result.Meta.TimeTick())
	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	assert.Equal(t, uint64(10), segment.meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(10), segment.meta.GetDataCheckpointTimeTick())
}

func TestSegmentViewDataUpdatedCallbackRunsAfterOwnerLockReleased(t *testing.T) {
	callbackSawUnlocked := false
	var segment *SegmentView
	segment = NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "callback-schema"},
		runtimeConfig{
			onDataUpdated: func() {
				if segment.mu.TryLock() {
					callbackSawUnlocked = true
					segment.mu.Unlock()
				}
			},
		},
	)

	segment.notifyDataUpdated()

	assert.True(t, callbackSawUnlocked)
}

func TestSegmentViewCommitTaskDoesNotRewriteLifecycleState(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "commit-task-state-schema"},
		runtimeConfig{
			lifecycle:   &testSegmentLifecycleWriter{},
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			metaAndData: true,
		},
	)

	result := segment.Flush(context.Background(), 10)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, segment.meta.GetState())

	segment.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	segment.meta.TombstoneTimeTick = 10
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))

	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, segment.meta.GetState())
	assert.Equal(t, uint64(10), segment.meta.GetTombstoneTimeTick())
}

func TestSegmentViewObserveTxnAppliesSameTimeTickInsertsTogether(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
		},
		&schemapb.CollectionSchema{Name: "txn-schema"},
		runtimeConfig{metaAndData: true},
	)
	txn := newTestTxnMessage(
		10,
		newTestInsertMessage(5, 2, 1, 7, 70),
		newTestInsertMessage(6, 2, 1, 8, 80),
	)

	result := segment.ObserveTxnMessage(context.Background(), txn)

	require.NotNil(t, result.Meta)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Meta.TimeTick())
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	assert.Equal(t, uint64(10), segment.meta.GetCheckpointTimeTick())
	assert.Equal(t, uint64(15), segment.meta.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(150), segment.meta.GetStat().GetModifiedBinarySize())
	assert.Equal(t, uint64(10), segment.pending.DataTimeTick())
	assert.Len(t, segment.pending.entries, 2)

	result = segment.ObserveTxnMessage(context.Background(), txn)
	require.NotNil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Equal(t, uint64(15), segment.meta.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(150), segment.meta.GetStat().GetModifiedBinarySize())
	assert.Len(t, segment.pending.entries, 2)
}

func TestSegmentViewObserveTxnInsertUsesCommitTimeTickForDataBuffer(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
		},
		&schemapb.CollectionSchema{Name: "txn-schema"},
		runtimeConfig{metaAndData: true},
	)
	bodyInsert := newTestInsertMessage(5, 2, 1, 7, 70)
	txn := newTestTxnMessage(10, bodyInsert)
	txn = txnWithBodyMessages{ImmutableTxnMessage: txn, messages: []message.ImmutableMessage{bodyInsert}}

	result := segment.ObserveTxnMessage(context.Background(), txn)

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(10), segment.pending.DataTimeTick())
	assert.Len(t, segment.pending.entries, 1)
}

func TestSegmentViewSerializesOwnSchedulerTasks(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "serial-schema"},
		runtimeConfig{
			lifecycle:   &testSegmentLifecycleWriter{},
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			flushPolicy: newWriteOnlyFlushPolicy(1, 0, 0),
			metaAndData: true,
		},
	)

	create := newTestCreateSegmentMessage(5, 2, 1)
	insert := newTestInsertMessage(6, 2, 1, 1, 10)
	segment.ObserveCreateSegmentMessageV2(context.Background(), create)
	segment.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])

	require.Len(t, taskScheduler.tasks, 2)
	assert.True(t, taskScheduler.tasks[0].Precondition().Ready())
	assert.False(t, taskScheduler.tasks[1].Precondition().Ready())

	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	assert.True(t, taskScheduler.tasks[1].Precondition().Ready())
}

func TestSegmentViewFlushTaskUsesChunkFixedAtSubmission(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "fixed-chunk-schema"},
		runtimeConfig{
			lifecycle:   &testSegmentLifecycleWriter{},
			packWriter:  writer,
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			flushPolicy: newWriteOnlyFlushPolicy(1, 0, 0),
			metaAndData: true,
		},
	)

	first := newTestInsertMessage(5, 2, 1, 1, 10)
	second := newTestInsertMessage(6, 2, 1, 2, 20)
	segment.ObserveInsertMessageV1(context.Background(), first, first.Header().GetPartitions()[0])
	segment.ObserveInsertMessageV1(context.Background(), second, second.Header().GetPartitions()[0])

	require.Len(t, taskScheduler.tasks, 2)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	require.NotNil(t, writer.pack)
	assert.Equal(t, uint64(5), writer.pack.FromTimeTick)
	assert.Equal(t, uint64(5), writer.pack.ToTimeTick)
	assert.Equal(t, uint64(1), writer.pack.Rows)
	assert.Equal(t, uint64(10), writer.pack.BinarySize)
	assert.Len(t, writer.pack.Inserts, 1)
}

func TestSegmentViewFlushTaskKeepsChunkInViewAndDoesNotHoldLockDuringIO(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &blockingInsertPackWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "retained-chunk-schema"},
		runtimeConfig{
			packWriter:  writer,
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			flushPolicy: newWriteOnlyFlushPolicy(1, 0, 0),
			metaAndData: true,
		},
	)

	insert := newTestInsertMessage(5, 2, 1, 1, 10)
	segment.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])

	require.Empty(t, segment.pending.entries)
	require.Len(t, segment.pendingFlushChunks, 1)
	require.Len(t, taskScheduler.tasks, 1)

	done := make(chan error, 1)
	go func() {
		done <- taskScheduler.tasks[0].Run(context.Background())
	}()
	<-writer.started

	require.True(t, segment.mu.TryLock())
	assert.Len(t, segment.pendingFlushChunks, 1)
	segment.mu.Unlock()

	close(writer.release)
	require.NoError(t, <-done)
	assert.Empty(t, segment.pendingFlushChunks)
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())
}

func TestSegmentViewEnsureTaskDoesNotHoldLockDuringLifecycleIO(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	lifecycle := &blockingSegmentLifecycle{
		ensureStarted: make(chan struct{}),
		ensureRelease: make(chan struct{}),
	}
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           100,
			PartitionId:            1,
			SegmentId:              2,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
		&schemapb.CollectionSchema{Name: "ensure-unlocked-schema"},
		runtimeConfig{
			lifecycle:   lifecycle,
			runtime:     moduleapi.Runtime{Scheduler: taskScheduler},
			metaAndData: true,
		},
	)

	create := newTestCreateSegmentMessage(5, 2, 1)
	segment.ObserveCreateSegmentMessageV2(context.Background(), create)
	require.Len(t, taskScheduler.tasks, 1)

	done := make(chan error, 1)
	go func() {
		done <- taskScheduler.tasks[0].Run(context.Background())
	}()
	<-lifecycle.ensureStarted

	require.True(t, segment.mu.TryLock())
	segment.mu.Unlock()

	close(lifecycle.ensureRelease)
	require.NoError(t, <-done)
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())
}

type blockingInsertPackWriter struct {
	started chan struct{}
	release chan struct{}
	result  *FlushResult
	pack    *FlushPack
}

func (w *blockingInsertPackWriter) FlushInsertBuffer(_ context.Context, pack *FlushPack) (*FlushResult, error) {
	w.pack = pack
	close(w.started)
	<-w.release
	return w.result, nil
}

func (w *blockingInsertPackWriter) FlushDeleteBuffer(context.Context, *DeleteFlushPack) (*DeleteFlushResult, error) {
	return nil, nil
}

type blockingSegmentLifecycle struct {
	ensureStarted chan struct{}
	ensureRelease chan struct{}
}

func (l *blockingSegmentLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	close(l.ensureStarted)
	<-l.ensureRelease
	return nil
}

func (l *blockingSegmentLifecycle) CommitL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return nil
}

func (l *blockingSegmentLifecycle) CommitL0Segment(context.Context, *L0DeleteBatch) error {
	return nil
}

func TestSegmentViewObserveInsertPendingBuffer(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			DataCheckpointTimeTick: 4,
		},
		nil,
		runtimeConfig{metaAndData: true},
	)
	skipped := newTestInsertMessage(4, 10, 1, 7, 70)
	appendedMsg := newTestInsertMessage(5, 10, 1, 7, 70)

	result := segment.ObserveInsertMessageV1(context.Background(), skipped, skipped.Header().GetPartitions()[0])
	assert.Nil(t, result.Data)
	assert.Equal(t, uint64(0), segment.pending.rows)

	result = segment.ObserveInsertMessageV1(context.Background(), appendedMsg, appendedMsg.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)
	assert.Equal(t, uint64(5), segment.pending.fromTimeTick)
	assert.Equal(t, uint64(5), segment.pending.toTimeTick)
	assert.Equal(t, uint64(7), segment.pending.rows)
	assert.Equal(t, uint64(70), segment.pending.binarySize)
	assert.Len(t, segment.pending.entries, 1)
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}

func TestSegmentViewObserveInsertUsesPendingDataState(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			DataCheckpointTimeTick: 4,
		},
		nil,
		runtimeConfig{metaAndData: true},
	)
	first := newTestInsertMessage(5, 10, 1, 7, 70)
	sameTick := newTestInsertMessage(5, 10, 1, 8, 80)
	stale := newTestInsertMessage(4, 10, 1, 9, 90)

	result := segment.ObserveInsertMessageV1(context.Background(), first, first.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)
	result = segment.ObserveInsertMessageV1(context.Background(), sameTick, sameTick.Header().GetPartitions()[0])
	assert.Nil(t, result.Data)
	result = segment.ObserveInsertMessageV1(context.Background(), stale, stale.Header().GetPartitions()[0])
	assert.Nil(t, result.Data)

	assert.Equal(t, uint64(5), segment.pending.DataTimeTick())
	assert.Equal(t, uint64(7), segment.pending.rows)
	assert.Len(t, segment.pending.entries, 1)
}

func TestSegmentViewFlushPolicyKeepsSmallBufferPending(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
	}
	segment := NewSegmentViewFromMeta(
		meta,
		nil,
		runtimeConfig{
			packWriter:  writer,
			flushPolicy: newWriteOnlyFlushPolicy(0, 100, 0),
			metaAndData: true,
		},
	)
	msg := newTestInsertMessage(5, 10, 2, 7, 70)

	result := segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)

	assert.Nil(t, writer.pack)
	assert.Len(t, segment.pending.entries, 1)
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}

func TestSegmentViewFlushPolicyFlushesFullBuffer(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 6}},
			},
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
	}
	segment := NewSegmentViewFromMeta(
		meta,
		nil,
		runtimeConfig{
			packWriter:  writer,
			runtime:     moduleapi.Runtime{Scheduler: &retryingImmediateAsyncTaskScheduler{}},
			flushPolicy: newWriteOnlyFlushPolicy(0, 100, 0),
			metaAndData: true,
		},
	)
	first := newTestInsertMessage(5, 10, 2, 7, 70)
	second := newTestInsertMessage(6, 10, 2, 8, 80)

	result := segment.ObserveInsertMessageV1(context.Background(), first, first.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)
	result = segment.ObserveInsertMessageV1(context.Background(), second, second.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)

	requireEventually(t, func() bool {
		return writer.pack != nil && segment.Meta().GetDataCheckpointTimeTick() == 6
	})
	require.NotNil(t, writer.pack)
	assert.Equal(t, uint64(5), writer.pack.FromTimeTick)
	assert.Equal(t, uint64(6), writer.pack.ToTimeTick)
	assert.Equal(t, uint64(15), writer.pack.Rows)
	assert.Equal(t, uint64(150), writer.pack.BinarySize)
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, uint64(6), meta.GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}

func TestSegmentViewFlushPolicyRetriesWriterError(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
		errs: []error{errors.New("write failed"), nil},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
	}
	segment := NewSegmentViewFromMeta(
		meta,
		nil,
		runtimeConfig{
			packWriter:  writer,
			runtime:     moduleapi.Runtime{Scheduler: &retryingImmediateAsyncTaskScheduler{}},
			flushPolicy: newWriteOnlyFlushPolicy(0, 100, 0),
			metaAndData: true,
		},
	)
	msg := newTestInsertMessage(5, 10, 2, 7, 100)

	result := segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)

	requireEventually(t, func() bool {
		return writer.pack != nil && segment.Meta().GetDataCheckpointTimeTick() == 5
	})
	require.NotNil(t, writer.pack)
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, uint64(5), meta.GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
	assert.True(t, segment.dirty)
}

func TestSegmentViewFlushPolicyFlushesStaleBuffer(t *testing.T) {
	policy := newWriteOnlyFlushPolicy(0, 0, time.Minute)
	now := time.Now()
	start := tsoutil.ComposeTSByTime(now, 0)
	buffer := writeOnlyInsertBuffer{entries: []InsertEntry{{}}, fromTimeTick: start, toTimeTick: start, rows: 1, binarySize: 1}

	assert.False(t, policy.ShouldFlush(buffer, start))
	assert.True(t, policy.ShouldFlush(buffer, tsoutil.ComposeTSByTime(now.Add(time.Minute+time.Millisecond), 0)))
}

func TestSegmentViewDataBarrierAdvancesAfterMetaPersisted(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
	}
	segment := NewSegmentViewFromMeta(meta, nil, runtimeConfig{
		metaAndData: true,
	})
	msg := newTestInsertMessage(5, 10, 1, 7, 70)
	_ = segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])

	segment.markPendingDataDurable(5)
	assert.Equal(t, uint64(4), segment.DataTimeTick())

	result := segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])
	assert.Nil(t, result.Data)
	assert.Len(t, segment.pending.entries, 1)

	snapshot := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(5), snapshot.GetDataCheckpointTimeTick())

	segment.MarkDataPersisted(snapshot.GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(5), segment.DataTimeTick())
}

func TestSegmentViewFlushBufferPersistsPendingInsertStorage(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				ManifestPath: "manifest-1",
				Binlogs: []*streamingpb.L1SegmentBinLogs{
					{
						FieldBinlog:  []*datapb.FieldBinlog{{FieldID: 100}},
						FromTimeTick: 5,
						ToTimeTick:   5,
					},
				},
			},
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
	}
	segment := NewSegmentViewFromMeta(meta, nil, runtimeConfig{
		packWriter:  writer,
		metaAndData: true,
	})
	msg := newTestInsertMessage(5, 10, 2, 7, 70)
	_ = segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])

	require.NoError(t, segment.FlushBuffer(context.Background()))
	require.NotNil(t, writer.pack)
	assert.Equal(t, int64(10), writer.pack.SegmentID)
	assert.Equal(t, uint64(5), writer.pack.FromTimeTick)
	assert.Equal(t, uint64(5), writer.pack.ToTimeTick)
	assert.Equal(t, uint64(7), writer.pack.Rows)
	assert.Equal(t, uint64(70), writer.pack.BinarySize)
	assert.Len(t, writer.pack.Inserts, 1)
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, "manifest-1", meta.GetPersistedStorage().GetManifestPath())
	assert.Len(t, meta.GetPersistedStorage().GetBinlogs(), 1)
	assert.Equal(t, uint64(5), meta.GetDataCheckpointTimeTick())
	assert.True(t, segment.dirty)
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}
func TestSegmentViewEnqueueFlushBufferRunsAfterPendingInsert(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
	}
	callbacks := 0
	segment := NewSegmentViewFromMeta(
		meta,
		nil,
		runtimeConfig{
			packWriter:    writer,
			runtime:       moduleapi.Runtime{Scheduler: &retryingImmediateAsyncTaskScheduler{}},
			onDataUpdated: func() { callbacks++ },
			metaAndData:   true,
		},
	)
	msg := newTestInsertMessage(5, 10, 2, 7, 70)
	_ = segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])
	require.NoError(t, segment.EnqueueFlushBuffer(context.Background()))

	requireEventually(t, func() bool {
		return writer.pack != nil && segment.Meta().GetDataCheckpointTimeTick() == 5 && callbacks == 1
	})
	require.NotNil(t, writer.pack)
	assert.Equal(t, uint64(7), writer.pack.Rows)
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
	assert.Equal(t, 1, callbacks)
}

func TestSegmentViewFlushBufferKeepsPendingOnWriterError(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{err: errors.New("write failed")}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:           1,
		PartitionId:            2,
		SegmentId:              10,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: 4,
	}
	segment := NewSegmentViewFromMeta(meta, nil, runtimeConfig{
		packWriter:  writer,
		metaAndData: true,
	})
	msg := newTestInsertMessage(5, 10, 2, 7, 70)
	_ = segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])

	err := segment.FlushBuffer(context.Background())
	require.Error(t, err)
	assert.Empty(t, segment.pending.entries)
	assert.Len(t, segment.pendingFlushChunks, 1)
	assert.Equal(t, uint64(4), meta.GetDataCheckpointTimeTick())
	assert.True(t, segment.dirty)
}
