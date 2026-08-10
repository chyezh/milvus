package segment

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
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestEnsureGrowingRetainsRefUntilLifecycleSucceeds(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{err: errors.New("not ready")}
	msg := newSegmentAckCreateMessage(t, 10)
	view := NewSegmentViewFromCreateSegmentMessage(
		msg,
		nil,
		runtimeConfig{
			lifecycle:   lifecycle,
			metaAndData: true,
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
	record := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: msg.TimeTick()}, nil)

	assert.True(t, view.ObserveCreateSegmentMessageV2(context.Background(), msg, record))
	record.Seal()
	require.Len(t, scheduler.tasks, 1)
	assert.Equal(t, int64(1), record.RefCount())

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.Equal(t, int64(1), record.RefCount())

	lifecycle.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, record.Completed())
	assert.Equal(t, msg.TimeTick(), view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.True(t, view.HasDirty())
}

func TestInsertChunkReleasesEveryCoveredMessageRefAfterDurableMetadataUpdate(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	writer := &recordingPackWriter{}
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:     1,
			PartitionId:      10,
			SegmentId:        100,
			Vchannel:         "v1",
			State:            streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
			Stat:             &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
		},
		0,
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			packWriter:  writer,
			metaAndData: true,
			flushPolicy: newWriteOnlyFlushPolicy(2, 0, 0),
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
	firstMsg, firstAssignment := newSegmentAckInsertMessage(t, 10, 1)
	secondMsg, secondAssignment := newSegmentAckInsertMessage(t, 20, 2)
	firstRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)
	secondRecord := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)

	assert.True(t, view.ObserveInsertMessageV1(context.Background(), firstMsg, firstAssignment, firstRecord))
	firstRecord.Seal()
	assert.True(t, view.ObserveInsertMessageV1(context.Background(), secondMsg, secondAssignment, secondRecord))
	secondRecord.Seal()
	require.Len(t, scheduler.tasks, 1)
	assert.False(t, firstRecord.Completed())
	assert.False(t, secondRecord.Completed())

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, 1, writer.calls)
	assert.True(t, firstRecord.Completed())
	assert.True(t, secondRecord.Completed())
	assert.Equal(t, uint64(20), view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.True(t, view.HasDirty())
}

func TestFinalCommitRetainsRefUntilLifecycleSucceeds(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{err: errors.New("not ready")}
	view := newSegmentAckGrowingView(scheduler, lifecycle)
	record := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)

	assert.True(t, view.Flush(context.Background(), 20, record))
	record.Seal()
	require.Len(t, scheduler.tasks, 1)
	assert.False(t, record.Completed())

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.False(t, record.Completed())
	assert.Equal(t, uint64(1), view.AssignmentMeta().GetDataCheckpointTimeTick())

	lifecycle.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, record.Completed())
	assert.Equal(t, 2, lifecycle.commitCalls)
	assert.Equal(t, uint64(20), view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.NotNil(t, view.AssignmentMeta().GetSealedAtDataVersion())
	assert.True(t, view.HasDirty())
}

func TestRepeatedFlushRefsSharePendingFinalCommit(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{}
	view := newSegmentAckGrowingView(scheduler, lifecycle)
	first := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 20}, nil)
	second := messageack.NewRecord(utility.WALConsumeCheckpoint{TimeTick: 30}, nil)

	assert.True(t, view.Flush(context.Background(), 20, first))
	first.Seal()
	assert.False(t, view.Flush(context.Background(), 30, second))
	second.Seal()
	require.Len(t, scheduler.tasks, 1)
	assert.False(t, first.Completed())
	assert.False(t, second.Completed())

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.True(t, first.Completed())
	assert.True(t, second.Completed())
	assert.Equal(t, 1, lifecycle.commitCalls)
}

func newSegmentAckGrowingView(scheduler *recordingSegmentScheduler, lifecycle *failingSegmentLifecycle) *SegmentView {
	return NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 1,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
		},
		1,
		1,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			lifecycle:   lifecycle,
			metaAndData: true,
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
}

type recordingSegmentScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingSegmentScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingSegmentTaskHandle{}
}

type recordingSegmentTaskHandle struct{}

func (recordingSegmentTaskHandle) Cancel()                    {}
func (recordingSegmentTaskHandle) Wait(context.Context) error { return nil }

type failingSegmentLifecycle struct {
	err         error
	commitCalls int
}

func (l *failingSegmentLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return l.err
}

func (l *failingSegmentLifecycle) CommitL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	l.commitCalls++
	return &viewpb.DataVersion{StreamingVersion: 1}, l.err
}

type recordingPackWriter struct {
	calls int
}

func (w *recordingPackWriter) FlushInsertBuffer(context.Context, *flushPack) (*flushResult, error) {
	w.calls++
	return &flushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}}, nil
}

func newSegmentAckCreateMessage(t *testing.T, timetick uint64) message.ImmutableCreateSegmentMessageV2 {
	t.Helper()
	mutable := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId: 1,
			PartitionId:  10,
			SegmentId:    100,
			MaxRows:      1000,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable()
	return message.MustAsImmutableCreateSegmentMessageV2(mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick))))
}

func newSegmentAckInsertMessage(
	t *testing.T,
	timetick uint64,
	messageID int64,
) (message.ImmutableInsertMessageV1, *messagespb.PartitionSegmentAssignment) {
	t.Helper()
	assignment := &messagespb.PartitionSegmentAssignment{
		PartitionId: 10,
		Rows:        1,
		BinarySize:  1,
		SegmentAssignment: &messagespb.SegmentAssignment{
			SegmentId: 100,
		},
	}
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*messagespb.PartitionSegmentAssignment{assignment},
		}).
		WithBody(&msgpb.InsertRequest{NumRows: 1}).
		BuildMutable()
	require.NoError(t, err)
	return message.MustAsImmutableInsertMessageV1(mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(messageID - 1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID))), assignment
}
