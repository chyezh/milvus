package growing

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

func newTestManager(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	lifecycle SegmentLifecycle,
	opts ...ManagerOption,
) *Manager {
	ensureTestVChannelPartitionsFromSegments(vchannels, segments)
	opts = append([]ManagerOption{
		WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: &retryingImmediateAsyncTaskScheduler{}}),
		WithRecoveryCatalog("test-channel", &testRecoveryCatalog{}),
	}, opts...)
	manager := NewManager(vchannels, segments, lifecycle, opts...)
	manager.SwitchIntoMetaAndData()
	return manager
}

func ensureTestVChannelPartitionsFromSegments(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) {
	for _, segment := range segments {
		vchannel := vchannels[segment.GetVchannel()]
		if vchannel == nil || segment.GetPartitionId() == 0 {
			continue
		}
		if vchannel.CollectionInfo == nil {
			vchannel.CollectionInfo = &streamingpb.CollectionInfoOfVChannel{CollectionId: segment.GetCollectionId()}
		}
		hasPartition := false
		for _, partition := range vchannel.CollectionInfo.GetPartitions() {
			if partition.GetPartitionId() == segment.GetPartitionId() {
				hasPartition = true
				break
			}
		}
		if !hasPartition {
			vchannel.CollectionInfo.Partitions = append(vchannel.CollectionInfo.Partitions, &streamingpb.PartitionInfoOfVChannel{
				PartitionId: segment.GetPartitionId(),
				State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
			})
		}
	}
}

type retryingImmediateAsyncTaskScheduler struct{}

func (s *retryingImmediateAsyncTaskScheduler) Submit(task scheduler.Task) scheduler.TaskHandle {
	handle := &captureTaskHandle{}
	go func() {
		for !task.Precondition().Ready() {
			time.Sleep(time.Millisecond)
		}
		for {
			if err := task.Run(context.Background()); err == nil {
				handle.MarkDone()
				return
			}
		}
	}()
	return handle
}

func (s *retryingImmediateAsyncTaskScheduler) Notify() {}

func runCapturedAsyncTasks(t *testing.T, taskScheduler *captureAsyncTaskScheduler) {
	t.Helper()
	for _, task := range taskScheduler.tasks {
		require.NoError(t, task.Run(context.Background()))
	}
}

func runCapturedAsyncTasksAndMarkDone(t *testing.T, taskScheduler *captureAsyncTaskScheduler) {
	t.Helper()
	for i, task := range taskScheduler.tasks {
		require.NoError(t, task.Run(context.Background()))
		taskScheduler.handles[i].MarkDone()
	}
}

func resetCapturedAsyncTasks(taskScheduler *captureAsyncTaskScheduler) {
	taskScheduler.tasks = nil
	taskScheduler.handles = nil
}

func runRequiredPersistTask(t *testing.T, manager *Manager, taskScheduler *captureAsyncTaskScheduler) {
	t.Helper()
	resetCapturedAsyncTasks(taskScheduler)
	manager.RequirePersist()
	require.Len(t, taskScheduler.tasks, 1)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
}

func requireEventually(t *testing.T, condition func() bool) {
	t.Helper()
	require.Eventually(t, condition, time.Second, time.Millisecond)
}

func TestNewGrowingManagerBuildsActiveWriteOnlySegments(t *testing.T) {
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
		"dropped": {
			Vchannel: "dropped",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		},
	}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			SegmentId: 1,
			Vchannel:  "v1",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		2: {
			SegmentId: 2,
			Vchannel:  "v1",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		},
		3: {
			SegmentId: 3,
			Vchannel:  "missing",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}

	writer := &testGrowingSegmentPackWriter{}
	manager := newTestManager(vchannels, segments, nil, WithPackWriter(writer))
	vchannelManager := manager.VChannel("v1")
	require.NotNil(t, vchannelManager)
	assert.Equal(t, streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY, vchannelManager.AssignmentMeta().GetGrowingSegmentMode())
	assert.NotNil(t, vchannelManager.Segment(1))
	assert.Same(t, writer, vchannelManager.Segment(1).packWriter)
	assert.NotNil(t, vchannelManager.Segment(2))
	assert.False(t, vchannelManager.Segment(2).IsGrowing())
	assert.Nil(t, manager.VChannel("dropped"))
}

func TestGrowingManagerObserveExistingCreateCollectionReturnsMetaBarrier(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "test-collection"}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 5,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, nil, nil)
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{100},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	result := manager.observeMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(5), result.Meta.TimeTick())
}

func TestGrowingManagerFutureCreateCollectionDoesNotReuseDroppedVChannelBarrier(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{Name: "test-collection"})
	require.NoError(t, err)
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{100},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(12).
		WithLastConfirmed(rmq.NewRmqID(12)).
		IntoImmutableMessage(rmq.NewRmqID(12))

	result := manager.observeMessage(context.Background(), msg)

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	vchannel := manager.retainedVChannel("v1").AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.GetState())
	assert.Equal(t, uint64(10), vchannel.GetCheckpointTimeTick())
}

func TestGrowingManagerCreateCollectionReplacesExpiredTombstonedVChannel(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "recreated-collection"}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil)
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{200},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "recreated-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{200},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(12).
		WithLastConfirmed(rmq.NewRmqID(12)).
		IntoImmutableMessage(rmq.NewRmqID(12))

	result := manager.observeMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(0), result.Meta.TimeTick())
	vchannel := manager.VChannel("v1")
	require.NotNil(t, vchannel)
	meta := vchannel.AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, meta.GetState())
	assert.Equal(t, uint64(12), meta.GetCheckpointTimeTick())
	require.Len(t, meta.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(200), meta.GetCollectionInfo().GetPartitions()[0].GetPartitionId())
	_, actualSchema := vchannel.GetSchema(12)
	assert.True(t, proto.Equal(schema, actualSchema))
}

func TestSegmentViewReportsInsertDataBarrier(t *testing.T) {
	segment := NewSegmentViewFromMeta(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              10,
			DataCheckpointTimeTick: 8,
		},
		nil,
	)

	assert.Equal(t, int64(10), segment.ID())
	assert.Equal(t, uint64(8), segment.DataTimeTick())
	assert.Equal(t, int64(10), segment.Meta().SegmentId)
}

func TestSegmentViewObserveInsertRunsInOrder(t *testing.T) {
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
	second := newTestInsertMessage(6, 10, 1, 8, 80)

	result := segment.ObserveInsertMessageV1(context.Background(), first, first.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)
	result = segment.ObserveInsertMessageV1(context.Background(), second, second.Header().GetPartitions()[0])
	assert.NotNil(t, result.Data)

	require.Len(t, segment.pending.entries, 2)
	assert.Equal(t, uint64(5), segment.pending.entries[0].timeTick)
	assert.Equal(t, uint64(6), segment.pending.entries[1].timeTick)
	assert.Equal(t, uint64(5), segment.pending.fromTimeTick)
	assert.Equal(t, uint64(6), segment.pending.toTimeTick)
	assert.Equal(t, uint64(15), segment.pending.rows)
	assert.Equal(t, uint64(150), segment.pending.binarySize)
}

func TestGrowingManagerObserveInsertDataBarrier(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId: 1,
			PartitionId:  100,
			SegmentId:    10,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}, nil)

	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId:       100,
					Rows:              10,
					BinarySize:        100,
					SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 10},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))

	result := manager.observeMessage(context.Background(), msg)
	assert.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	manager.VChannel("v1").Segment(10).markPendingDataDurable(5)
	manager.VChannel("v1").Segment(10).MarkDataPersisted(5)
	result = manager.observeMessage(context.Background(), msg)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerObserveCreateSegmentEnsuresGrowingSegment(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	lifecycle := &testSegmentLifecycleWriter{}
	catalog := &testRecoveryCatalog{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			StorageVersion:         1,
			DataCheckpointTimeTick: 4,
			CheckpointTimeTick:     5,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
		},
	}, lifecycle, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))
	msg := newTestCreateSegmentMessage(5, 10, 100)

	result := manager.observeMessage(context.Background(), msg)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())

	segment := manager.VChannel("v1").Segment(10)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	ensureCalls, ensureMeta := lifecycle.ensureSnapshot()
	assert.Equal(t, 1, ensureCalls)
	require.NotNil(t, ensureMeta)
	assert.Equal(t, int64(10), ensureMeta.GetSegmentId())
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.PersistedDataTimeTick())
	assert.True(t, segment.dirty)
	assert.Empty(t, catalog.savedSegments)

	runRequiredPersistTask(t, manager, taskScheduler)
	assert.Equal(t, uint64(5), segment.PersistedDataTimeTick())
	require.Len(t, catalog.savedSegments, 1)
	assert.Equal(t, uint64(5), catalog.savedSegments[0][10].GetDataCheckpointTimeTick())
	assert.False(t, segment.dirty)
}

func TestGrowingManagerObserveCreateSegmentSubmitsSchedulerTask(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			StorageVersion:         1,
			DataCheckpointTimeTick: 4,
			CheckpointTimeTick:     5,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
		},
	}, lifecycle, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	msg := newTestCreateSegmentMessage(5, 10, 100)

	result := manager.observeMessage(context.Background(), msg)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-ensure-growing-segment", taskScheduler.tasks[0].Name())

	ensureCalls, _ := lifecycle.ensureSnapshot()
	assert.Equal(t, 0, ensureCalls)
	segment := manager.VChannel("v1").Segment(10)
	assert.Equal(t, uint64(4), segment.DataTimeTick())

	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	ensureCalls, ensureMeta := lifecycle.ensureSnapshot()
	assert.Equal(t, 1, ensureCalls)
	require.NotNil(t, ensureMeta)
	assert.Equal(t, int64(10), ensureMeta.GetSegmentId())
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
	assert.True(t, segment.dirty)
}

func TestGrowingManagerCreateSegmentRequiresRetainedPartitionMeta(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			LatestDataVersion:      &viewpb.DataVersion{},
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             newTestGrowingPackWriterSchema(),
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	result := manager.observeMessage(context.Background(), newTestCreateSegmentMessage(12, 20, 101))

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Nil(t, manager.Segments()[20])
	assert.Empty(t, taskScheduler.tasks)
}

func TestGrowingManagerObserveFlushSubmitsSchedulerTask(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	insert := newTestInsertMessage(5, 10, 100, 7, 70)
	flush := newTestFlushMessage(10, 10)

	insertResult := manager.observeMessage(context.Background(), insert)
	assert.NotNil(t, insertResult.Data)
	assert.Empty(t, taskScheduler.tasks)
	segment := manager.VChannel("v1").Segment(10)
	require.Len(t, segment.pending.entries, 1)

	flushResult := manager.observeMessage(context.Background(), flush)
	assert.NotNil(t, flushResult.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())

	assert.Nil(t, writer.pack)
	commitL1Calls, _ := lifecycle.commitL1Snapshot()
	assert.Equal(t, 0, commitL1Calls)
	assert.Equal(t, uint64(4), segment.DataTimeTick())

	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	require.NotNil(t, writer.pack)
	commitL1Calls, commitL1Meta := lifecycle.commitL1Snapshot()
	assert.Equal(t, 1, commitL1Calls)
	require.NotNil(t, commitL1Meta)
	assert.Equal(t, int64(10), commitL1Meta.GetSegmentId())
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, segment.Meta().GetState())
	assert.Equal(t, uint64(0), segment.Meta().GetTombstoneTimeTick())
	assert.Equal(t, uint64(10), segment.Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}

func TestGrowingManagerSegmentTombstoneFinalizeRunsAfterDataTask(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithPackWriter(&testGrowingSegmentPackWriter{
		result: &FlushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}},
	}), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	flushResult := manager.observeMessage(context.Background(), newTestFlushMessage(10, 10))
	require.NotNil(t, flushResult.Data)
	require.Len(t, taskScheduler.tasks, 1)

	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	assert.Empty(t, catalog.savedSegments)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())

	runRequiredPersistTask(t, manager, taskScheduler)
	require.Len(t, catalog.savedSegments, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, catalog.savedSegments[0][10].GetState())
	assert.Equal(t, uint64(10), catalog.savedSegments[0][10].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, manager.Segments()[10].Meta().GetState())

	require.Len(t, taskScheduler.tasks, 2)
	require.True(t, taskScheduler.tasks[1].Precondition().Ready())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	require.Len(t, catalog.savedSegments, 2)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, catalog.savedSegments[1][10].GetState())
	assert.Equal(t, uint64(10), catalog.savedSegments[1][10].GetDataCheckpointTimeTick())
}

func TestGrowingManagerObserveFlushSegmentDoesNotFlushTransformLog(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   6,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithTransformLogBufferMaxRows(100))

	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(6, 100, []int64{1}))
	_ = manager.observeMessage(context.Background(), newTestInsertMessage(5, 10, 100, 7, 70))
	assert.Empty(t, taskScheduler.tasks)

	flushResult := manager.observeMessage(context.Background(), newTestFlushMessage(10, 10))

	require.NotNil(t, flushResult.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
}

func TestGrowingManagerObserveFlushCommitsL1Segment(t *testing.T) {
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, lifecycle, WithPackWriter(writer))
	insert := newTestInsertMessage(5, 10, 100, 7, 70)
	flush := newTestFlushMessage(10, 10)

	insertResult := manager.observeMessage(context.Background(), insert)
	assert.NotNil(t, insertResult.Data)
	flushResult := manager.observeMessage(context.Background(), flush)
	assert.NotNil(t, flushResult.Data)

	segment := manager.VChannel("v1").Segment(10)
	requireEventually(t, func() bool {
		commitL1Calls, _ := lifecycle.commitL1Snapshot()
		return writer.pack != nil && commitL1Calls == 1 && segment.Meta().GetDataCheckpointTimeTick() == 10
	})
	require.NotNil(t, writer.pack)
	assert.Equal(t, uint64(5), writer.pack.ToTimeTick)
	commitL1Calls, commitL1Meta := lifecycle.commitL1Snapshot()
	assert.Equal(t, 1, commitL1Calls)
	require.NotNil(t, commitL1Meta)
	assert.Equal(t, int64(10), commitL1Meta.GetSegmentId())
	assert.Len(t, commitL1Meta.GetPersistedStorage().GetBinlogs(), 1)
	assert.Empty(t, segment.pending.entries)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, segment.Meta().GetState())
	assert.Equal(t, uint64(0), segment.Meta().GetTombstoneTimeTick())
	assert.Equal(t, uint64(10), segment.Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), segment.DataTimeTick())
}

func TestGrowingManagerDoesNotCreateIndependentTombstoneMetaOnCloseMessages(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	_ = manager.observeMessage(context.Background(), newTestFlushMessage(10, 10))
	_ = manager.observeMessage(context.Background(), newTestDropPartitionMessage(11, 1, 100))
	_ = manager.observeMessage(context.Background(), newTestDropCollectionMessage(12))

	snapshot := manager.collectDirtyOwners().consumeSnapshot()

	require.NotNil(t, snapshot)
	assert.Contains(t, snapshot.SegmentAssignments, int64(10))
	assert.Contains(t, snapshot.VChannels, "v1")
}

func TestGrowingManagerObserveDropPartitionFlushesOnlyTargetPartitionSegments(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
		11: newTestSegmentRecoveryInfo(1, 101, 11, "v1", 4),
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	msg := newTestDropPartitionMessage(10, 1, 100)

	result := manager.observeMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[1].Name())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, manager.Segments()[11].Meta().GetState())
}

func TestGrowingManagerObserveDropPartitionRequiresRetainedPartitionMeta(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	msg := newTestDropPartitionMessage(10, 1, 101)

	result := manager.observeMessage(context.Background(), msg)

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	meta := manager.retainedVChannel("v1").AssignmentMeta()
	assert.Equal(t, uint64(4), meta.GetCheckpointTimeTick())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(meta, 100))
	assert.False(t, manager.retainedVChannel("v1").HasDirty())
}

func TestGrowingManagerObserveCreatePartitionUsesRetainedDroppedVChannel(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil)

	result := manager.observeMessage(context.Background(), newTestCreatePartitionMessage("v1", 1, 101, 5))

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(10), result.Meta.TimeTick())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, manager.retainedVChannel("v1").AssignmentMeta().GetState())
}

func TestGrowingManagerObserveCreatePartitionDoesNotMutateDroppedVChannel(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil)

	result := manager.observeMessage(context.Background(), newTestCreatePartitionMessage("v1", 1, 101, 12))

	assert.Nil(t, result.Meta)
	meta := manager.retainedVChannel("v1").AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, meta.GetState())
	assert.Equal(t, uint64(10), meta.GetCheckpointTimeTick())
	assert.Len(t, meta.GetCollectionInfo().GetPartitions(), 1)
	assert.False(t, manager.retainedVChannel("v1").HasDirty())
}

func TestGrowingManagerObserveCreatePartitionReactivatesClosedPartition(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       101,
						State:             streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED,
						TombstoneTimeTick: 10,
					},
				},
			},
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
		},
	}, nil, nil)

	result := manager.ObserveMessage(context.Background(), newTestCreatePartitionMessage("v1", 1, 101, 20))

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(10), result.Meta.TimeTick())
	assert.True(t, manager.retainedVChannel("v1").HasDirty())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(manager.retainedVChannel("v1").AssignmentMeta(), 101))
	assert.Equal(t, uint64(0), partitionTombstoneTimeTick(manager.retainedVChannel("v1").AssignmentMeta(), 101))
	assert.Equal(t, uint64(20), manager.retainedVChannel("v1").AssignmentMeta().GetCheckpointTimeTick())
}

func TestGrowingManagerObserveDropPartitionUsesRetainedDroppedVChannel(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, nil)
	dropPartition := newTestDropPartitionMessage(5, 1, 100)

	result := manager.observeMessage(context.Background(), dropPartition)

	require.NotNil(t, result.Meta)
	assert.Equal(t, uint64(10), result.Meta.TimeTick())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, manager.retainedVChannel("v1").AssignmentMeta().GetState())
}

func TestGrowingManagerObserveManualFlushCommitsVChannelSegmentsCreatedBeforeMessage(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
		20: newTestSegmentRecoveryInfo(1, 101, 20, "v1", 4),
		30: newTestSegmentRecoveryInfo(2, 200, 30, "v1", 4),
		40: newTestSegmentRecoveryInfo(1, 102, 40, "v1", 4),
		50: newTestSegmentRecoveryInfo(1, 103, 50, "v2", 4),
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	segment40 := manager.Segments()[40]
	segment40.mu.Lock()
	segment40.meta.Stat.CreateSegmentTimeTick = 10
	segment40.mu.Unlock()
	msg := newTestManualFlushMessage(10, 10)

	result := manager.observeMessage(context.Background(), msg)
	assert.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 3)

	runCapturedAsyncTasks(t, taskScheduler)
	assert.ElementsMatch(t, []int64{10, 20}, lifecycle.commitL1SegmentIDSnapshot())
	assert.Equal(t, uint64(10), manager.VChannel("v1").Segment(10).Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").Segment(20).Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.Segments()[30].DataTimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.VChannel("v1").Segment(40).DataTimeTick())
	assert.Equal(t, uint64(4), manager.Segments()[50].DataTimeTick())
	assert.Equal(t, uint64(4), result.Data.TimeTick())
}

func TestGrowingManagerManualFlushRequiresRetainedVChannel(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	result := manager.observeMessage(context.Background(), newTestManualFlushMessage(10, 10))

	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, manager.Segments()[10].Meta().GetState())
}

func TestGrowingManagerObserveFlushAllFlushesSegmentsAndTransformLogs(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}},
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(100))
	_ = manager.observeMessage(context.Background(), newTestInsertMessage(5, 10, 100, 7, 70))
	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))

	result := manager.observeMessage(context.Background(), newTestFlushAllMessage(10))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[1].Name())

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	assert.Equal(t, []int64{10}, lifecycle.commitL1SegmentIDSnapshot())
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 1, commitL0Calls)
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
}

func TestGrowingManagerObservePChannelLevelFlushAllOnControlChannel(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
		20: newTestSegmentRecoveryInfo(2, 200, 20, "v2", 4),
	}, lifecycle, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	result := manager.ObserveMessage(context.Background(), newTestPChannelLevelFlushAllMessage(10))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[1].Name())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, manager.Segments()[20].Meta().GetState())
}

func TestGrowingManagerObserveFlushAllFlushesRetainedSegmentFromDifferentCollection(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
	}, lifecycle, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	result := manager.observeMessage(context.Background(), newTestFlushAllMessage(20))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())

	runCapturedAsyncTasks(t, taskScheduler)
	assert.Equal(t, []int64{10}, lifecycle.commitL1SegmentIDSnapshot())
	assert.Equal(t, uint64(20), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())
}

func TestGrowingManagerObserveTruncateCollectionFlushesVChannelData(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}},
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(100))
	_ = manager.observeMessage(context.Background(), newTestInsertMessage(5, 10, 100, 7, 70))
	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))

	result := manager.observeMessage(context.Background(), newTestTruncateCollectionMessage(10))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[1].Name())

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	assert.Equal(t, []int64{10}, lifecycle.commitL1SegmentIDSnapshot())
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 1, commitL0Calls)
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
}

func TestGrowingManagerObserveAlterWALCommitsRetainedFlushedSegmentsToFlushTimeTick(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 20,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, lifecycle, WithRecoveryCatalog("test-channel", catalog), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	result := manager.observeMessage(context.Background(), newTestAlterWALMessage(20))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())

	runCapturedAsyncTasks(t, taskScheduler)

	assert.Equal(t, []int64{10}, lifecycle.commitL1SegmentIDSnapshot())
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())
	assert.Empty(t, catalog.savedSegments)
	assert.Equal(t, uint64(4), result.Data.TimeTick())

	runRequiredPersistTask(t, manager, taskScheduler)
	require.Len(t, catalog.savedSegments, 1)
	assert.Equal(t, uint64(10), catalog.savedSegments[0][10].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, catalog.savedSegments[0][10].GetState())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, manager.Segments()[10].Meta().GetState())

	require.Len(t, taskScheduler.tasks, 2)
	require.True(t, taskScheduler.tasks[1].Precondition().Ready())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	require.Len(t, catalog.savedSegments, 2)
	assert.Equal(t, uint64(10), catalog.savedSegments[1][10].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, catalog.savedSegments[1][10].GetState())
}

func TestGrowingManagerObserveDeleteBuffersUntilThreshold(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				PartitionID:  100,
				SegmentID:    1000,
				ToTimeTick:   6,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	schema := newTestGrowingPackWriterSchema()
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: schema, CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog), WithTransformLogBufferMaxRows(3))
	first := newTestDeleteMessage(5, 100, []int64{1, 2})
	second := newTestDeleteMessage(6, 100, []int64{3})

	result := manager.observeMessage(context.Background(), first)
	assert.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	assert.Empty(t, taskScheduler.tasks)
	assert.Nil(t, writer.deletePack)

	result = manager.observeMessage(context.Background(), second)
	assert.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[0].Name())

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	require.NotNil(t, writer.deletePack)
	assert.Same(t, schema, writer.deletePack.Schema)
	assert.Equal(t, common.AllPartitionsID, writer.deletePack.PartitionID)
	assert.Equal(t, uint64(5), writer.deletePack.FromTimeTick)
	assert.Equal(t, uint64(6), writer.deletePack.ToTimeTick)
	assert.Len(t, writer.deletePack.Deletes, 2)
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 1, commitL0Calls)
	assert.Equal(t, uint64(6), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.VChannel("v1").PersistedDataTimeTick())
	assert.Empty(t, catalog.savedVChannels)

	runRequiredPersistTask(t, manager, taskScheduler)
	assert.Equal(t, uint64(6), result.Data.TimeTick())
	assert.Equal(t, uint64(6), manager.VChannel("v1").PersistedDataTimeTick())
	require.Len(t, catalog.savedVChannels, 1)
	assert.Equal(t, uint64(6), catalog.savedVChannels[0]["v1"].GetDataCheckpointTimeTick())
	assert.False(t, manager.VChannel("v1").dirty)
}

func TestGrowingManagerObserveDeleteAcceptsAllPartitions(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				PartitionID:  common.AllPartitionsID,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 200, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(1))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(5, common.AllPartitionsID, []int64{1}))

	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	require.NotNil(t, writer.deletePack)
	assert.Equal(t, common.AllPartitionsID, writer.deletePack.PartitionID)
	assert.Len(t, writer.deletePack.Deletes, 1)
}

func TestGrowingManagerFlushTransformLogDoesNotFallbackToFutureSchema(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	futureSchema := newTestGrowingPackWriterSchema()
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: futureSchema, CheckpointTimeTick: 10},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(1))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)

	require.NotNil(t, writer.deletePack)
	assert.Nil(t, writer.deletePack.Schema)
}

func TestGrowingManagerObserveTxnDeleteIsIdempotent(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))
	txn := newTestTxnMessage(
		10,
		newTestDeleteMessage(5, 100, []int64{1}),
		newTestDeleteMessage(6, 100, []int64{2}),
	)

	result := manager.observeMessage(context.Background(), txn)

	require.NotNil(t, result.Data)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, uint64(10), vchannel.transformLogBuffer.DataTimeTick())
	assert.Len(t, vchannel.transformLogBuffer.entries, 2)

	result = manager.observeMessage(context.Background(), txn)
	assert.Nil(t, result.Data)
	assert.Len(t, vchannel.transformLogBuffer.entries, 2)
}

func TestGrowingManagerObserveTxnDeleteRejectsDroppedPartitionByCommitTimeTick(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       100,
						State:             streamingpb.PartitionState_PARTITION_STATE_DROPPED,
						TombstoneTimeTick: 6,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))
	bodyDelete := newTestDeleteMessage(5, 100, []int64{1})
	txn := newTestTxnMessage(10, bodyDelete)
	txn = txnWithBodyMessages{ImmutableTxnMessage: txn, messages: []message.ImmutableMessage{bodyDelete}}

	result := manager.observeTxnMessage(context.Background(), txn)

	assert.Nil(t, result.Data)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, uint64(0), vchannel.transformLogBuffer.DataTimeTick())
	assert.Empty(t, vchannel.transformLogBuffer.entries)
}

func TestGrowingManagerObserveDeleteRequiresRetainedPartitionMeta(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(12, 101, []int64{1}))

	assert.Nil(t, result.Data)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, uint64(0), vchannel.transformLogBuffer.DataTimeTick())
	assert.Empty(t, vchannel.transformLogBuffer.entries)
}

func TestGrowingManagerObserveDeleteRejectsVChannelMissingPartitionMeta(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(12, 100, []int64{1}))

	assert.Nil(t, result.Data)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, uint64(0), vchannel.transformLogBuffer.DataTimeTick())
	assert.Empty(t, vchannel.transformLogBuffer.entries)
}

func TestGrowingManagerObserveDeleteUsesDroppedVChannelUntilTombstoned(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(3))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
}

func TestGrowingManagerObserveInsertUsesDroppedVChannelUntilTombstoned(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, &testSegmentLifecycleWriter{})

	result := manager.observeMessage(context.Background(), newTestInsertMessage(5, 10, 100, 1, 10))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
}

func TestGrowingManagerObserveInsertReplaysDataOnFlushedSegment(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))

	result := manager.observeMessage(context.Background(), newTestInsertMessage(5, 10, 100, 7, 70))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	segment := manager.retainedVChannel("v1").Segment(10)
	require.NotNil(t, segment)
	assert.Len(t, segment.pending.entries, 1)
	snapshot := segment.AssignmentMeta()
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
}

func TestGrowingManagerObserveTxnInsertReplaysDataOnFlushedSegment(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithTransformLogBufferMaxRows(100))
	txn := newTestTxnMessage(9, newTestInsertMessage(5, 10, 100, 7, 70))

	result := manager.observeMessage(context.Background(), txn)

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), result.Data.TimeTick())
	segment := manager.retainedVChannel("v1").Segment(10)
	require.NotNil(t, segment)
	assert.Len(t, segment.pending.entries, 1)
	snapshot := segment.AssignmentMeta()
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
}

func TestGrowingManagerObserveDropPartitionSkipsTombstonedPartitionReplay(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       100,
						State:             streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED,
						TombstoneTimeTick: 10,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
	result := manager.observeMessage(context.Background(), newTestDropPartitionMessage(10, 1, 100))

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	assert.False(t, manager.retainedVChannel("v1").HasDirty())
}

func TestGrowingManagerObserveCreateSegmentSkipsTombstonedSegmentReplay(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{})

	result := manager.observeMessage(context.Background(), newTestCreateSegmentMessage(10, 10, 100))

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerVChannelTombstoneWaitsForCoveredSegments(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{})

	vchannel := manager.retainedVChannel("v1")
	vchannel.MarkDataCheckpoint(10)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.AssignmentMeta().GetState())

	manager.Segments()[10].MarkDataCheckpoint(10)
	manager.finalizeTombstones()
	meta := vchannel.AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, meta.GetState())

	manager.Segments()[10].MarkDataPersisted(10)
	vchannel.MarkDataPersisted(10)
	manager.finalizeTombstones()
	segmentSnapshot := manager.Segments()[10].ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, segmentSnapshot)
	manager.Segments()[10].MarkSnapshotPersisted(segmentSnapshot)
	manager.finalizeTombstones()
	meta = vchannel.AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, meta.GetState())
	assert.Equal(t, uint64(10), meta.GetTombstoneTimeTick())
}

func TestGrowingManagerVChannelTombstoneUsesCoveredSegmentTombstoneState(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{})
	vchannel := manager.retainedVChannel("v1")
	vchannel.MarkDataPersisted(10)
	segment := manager.Segments()[10]

	manager.finalizeTombstones()
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, segment.Meta().GetState())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.AssignmentMeta().GetState())

	segmentSnapshot := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, segmentSnapshot)
	segment.MarkSnapshotPersisted(segmentSnapshot)

	manager.finalizeTombstones()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, vchannel.AssignmentMeta().GetState())
}

func TestGrowingManagerPartitionTombstoneWaitsForCoveredSegments(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{})

	vchannel := manager.retainedVChannel("v1")
	vchannel.MarkDataCheckpoint(10)
	partition := vchannel.AssignmentMeta().GetCollectionInfo().GetPartitions()[0]
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partition.GetState())

	manager.Segments()[10].MarkDataCheckpoint(10)
	manager.finalizeTombstones()
	partition = vchannel.AssignmentMeta().GetCollectionInfo().GetPartitions()[0]
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partition.GetState())

	manager.Segments()[10].MarkDataPersisted(10)
	vchannel.MarkDataPersisted(10)
	manager.finalizeTombstones()
	segmentSnapshot := manager.Segments()[10].ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, segmentSnapshot)
	manager.Segments()[10].MarkSnapshotPersisted(segmentSnapshot)
	manager.finalizeTombstones()
	partition = vchannel.AssignmentMeta().GetCollectionInfo().GetPartitions()[0]
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partition.GetState())
	assert.Equal(t, uint64(10), partition.GetTombstoneTimeTick())
}

func TestGrowingManagerCleanupTombstonedMetaWaitsForBothPhysicalCheckpoints(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	manager.NotifyCheckpointPersisted(11, 10)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	assert.Contains(t, manager.VChannels(), "v1")
	assert.Contains(t, manager.Segments(), int64(10))
	assert.Empty(t, catalog.droppedVChannels)
	assert.Empty(t, catalog.droppedSegments)

	taskScheduler.tasks = nil
	taskScheduler.handles = nil
	manager.NotifyCheckpointPersisted(11, 11)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	assert.NotContains(t, manager.VChannels(), "v1")
	assert.NotContains(t, manager.Segments(), int64(10))
	require.Len(t, catalog.droppedVChannels, 1)
	assert.Contains(t, catalog.droppedVChannels[0], "v1")
	require.Len(t, catalog.droppedSegments, 1)
	assert.Equal(t, []int64{10}, catalog.droppedSegments[0])
}

func TestGrowingManagerCleanupTombstonedPartitionMarksRetainedVChannelDirty(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	manager.NotifyCheckpointPersisted(11, 11)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)

	require.Empty(t, catalog.droppedVChannels)
	require.Empty(t, catalog.savedVChannels)

	retained := manager.retainedVChannel("v1").AssignmentMeta()
	require.Len(t, retained.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(100), retained.GetCollectionInfo().GetPartitions()[0].GetPartitionId())
	assert.True(t, manager.retainedVChannel("v1").HasDirty())

	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-persist", taskScheduler.tasks[1].Name())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	taskScheduler.handles[1].MarkDone()

	require.Len(t, catalog.savedVChannels, 1)
	saved := catalog.savedVChannels[0]["v1"]
	require.NotNil(t, saved)
	require.Len(t, saved.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(100), saved.GetCollectionInfo().GetPartitions()[0].GetPartitionId())
	assert.False(t, manager.retainedVChannel("v1").HasDirty())
}

func TestGrowingManagerPendingVChannelCleanupKeepsRecreatedVChannel(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))
	manager.retainedVChannel("v1").MarkMetaPersisted(10)
	manager.retainedVChannel("v1").MarkDataPersisted(10)

	manager.NotifyCheckpointPersisted(11, 11)
	require.Len(t, taskScheduler.tasks, 1)
	cleanupTask := taskScheduler.tasks[0]

	schema := &schemapb.CollectionSchema{Name: "recreated-collection"}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{200},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "recreated-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{200},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(12).
		WithLastConfirmed(rmq.NewRmqID(12)).
		IntoImmutableMessage(rmq.NewRmqID(12))
	result := manager.observeMessage(context.Background(), msg)
	require.NotNil(t, result.Meta)

	require.NoError(t, cleanupTask.Run(context.Background()))
	assert.Empty(t, catalog.droppedVChannels)
	vchannel := manager.VChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, vchannel.AssignmentMeta().GetState())
	assert.Equal(t, uint64(12), vchannel.AssignmentMeta().GetCheckpointTimeTick())
}

func TestGrowingManagerCleanupDropsSegmentsBeforePartitionMetaCleanup(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            101,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	manager.NotifyCheckpointPersisted(11, 11)
	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)

	require.Len(t, catalog.droppedSegments, 1)
	require.Empty(t, catalog.savedVChannels)
	assert.Equal(t, []string{"drop-segments"}, catalog.ops)

	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-persist", taskScheduler.tasks[1].Name())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	taskScheduler.handles[1].MarkDone()

	require.Len(t, catalog.savedVChannels, 1)
	assert.Equal(t, []string{"drop-segments", "save-vchannels"}, catalog.ops)
}

func TestGrowingManagerPartitionCleanupUsesLatestVChannelMeta(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	manager.NotifyCheckpointPersisted(11, 11)
	require.Len(t, taskScheduler.tasks, 1)
	cleanupTask := taskScheduler.tasks[0]

	_ = manager.observeMessage(context.Background(), newTestSchemaChangeMessage(12))
	manager.RequirePersist()
	require.Len(t, taskScheduler.tasks, 2)
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	taskScheduler.handles[1].MarkDone()
	require.Len(t, catalog.savedVChannels, 1)
	assert.Len(t, catalog.savedVChannels[0]["v1"].GetCollectionInfo().GetSchemas(), 2)

	require.NoError(t, cleanupTask.Run(context.Background()))
	require.Len(t, catalog.savedVChannels, 1)

	require.Len(t, taskScheduler.tasks, 3)
	assert.Equal(t, "growing-persist", taskScheduler.tasks[2].Name())
	require.NoError(t, taskScheduler.tasks[2].Run(context.Background()))
	taskScheduler.handles[2].MarkDone()

	require.Len(t, catalog.savedVChannels, 2)
	cleanupMeta := catalog.savedVChannels[1]["v1"]
	assert.Len(t, cleanupMeta.GetCollectionInfo().GetSchemas(), 2)
	assert.Len(t, cleanupMeta.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(100), cleanupMeta.GetCollectionInfo().GetPartitions()[0].GetPartitionId())
}

func TestGrowingManagerPendingPartitionCleanupKeepsChangedTombstonedPartition(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 10},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	manager.NotifyCheckpointPersisted(11, 11)
	require.Len(t, taskScheduler.tasks, 1)
	cleanupTask := taskScheduler.tasks[0]

	vchannel := manager.retainedVChannel("v1")
	result := manager.observeMessage(context.Background(), newTestCreatePartitionMessage("v1", 1, 101, 12))
	require.NotNil(t, result.Meta)
	result = manager.observeMessage(context.Background(), newTestDropPartitionMessage(20, 1, 101))
	require.NotNil(t, result.Meta)
	vchannel.MarkDataCheckpoint(20)
	vchannel.MarkMetaPersisted(20)
	vchannel.MarkDataPersisted(20)
	vchannel.TryFinalizeTombstone()
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(vchannel.AssignmentMeta(), 101))
	require.Equal(t, uint64(20), partitionTombstoneTimeTick(vchannel.AssignmentMeta(), 101))

	require.NoError(t, cleanupTask.Run(context.Background()))
	retained := manager.retainedVChannel("v1").AssignmentMeta()
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, partitionState(retained, 101))
	assert.Equal(t, uint64(20), partitionTombstoneTimeTick(retained, 101))
	require.Empty(t, catalog.savedVChannels)
}

func TestGrowingManagerManualFlushFlushesPendingTransformLogBuffer(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog), WithTransformLogBufferMaxRows(100))

	deleteResult := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1, 2}))
	assert.NotNil(t, deleteResult.Data)
	assert.Empty(t, taskScheduler.tasks)

	flushResult := manager.observeMessage(context.Background(), newTestManualFlushMessage(10, 10))
	require.NotNil(t, flushResult.Data)
	assert.Equal(t, uint64(4), flushResult.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[0].Name())

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	require.NotNil(t, writer.deletePack)
	assert.Equal(t, uint64(5), writer.deletePack.FromTimeTick)
	assert.Equal(t, uint64(5), writer.deletePack.ToTimeTick)
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 1, commitL0Calls)
	assert.Equal(t, uint64(10), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.VChannel("v1").PersistedDataTimeTick())
	assert.Empty(t, catalog.savedVChannels)

	runRequiredPersistTask(t, manager, taskScheduler)
	assert.Equal(t, uint64(10), flushResult.Data.TimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").PersistedDataTimeTick())
	require.Len(t, catalog.savedVChannels, 1)
	assert.Equal(t, uint64(10), catalog.savedVChannels[0]["v1"].GetDataCheckpointTimeTick())
}

func TestGrowingManagerTransformLogFlushUsesTargetTimeTickSchema(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	oldSchema := &schemapb.CollectionSchema{Name: "schema-1"}
	futureSchema := &schemapb.CollectionSchema{Name: "schema-12"}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: oldSchema, CheckpointTimeTick: 1},
					{Schema: futureSchema, CheckpointTimeTick: 12},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(100))

	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))
	flushResult := manager.observeMessage(context.Background(), newTestManualFlushMessage(10, 10))
	require.NotNil(t, flushResult.Data)
	require.Len(t, taskScheduler.tasks, 1)
	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(12, 100, []int64{2}))

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)

	require.NotNil(t, writer.deletePack)
	assert.Same(t, oldSchema, writer.deletePack.Schema)
	assert.Equal(t, uint64(5), writer.deletePack.ToTimeTick)
}

func TestGrowingManagerDropCollectionAdvancesEmptyTransformLogCheckpoint(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))

	dropCollection := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DropCollectionRequest{CollectionID: 1}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(10)
	msg := message.MustAsImmutableDropCollectionMessageV1(dropCollection.WithTimeTick(10).WithLastConfirmed(msgID).IntoImmutableMessage(msgID))

	result := manager.observeMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[0].Name())

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.AssignmentMeta().GetState())
	assert.Equal(t, uint64(10), vchannel.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), vchannel.PersistedDataTimeTick())
	assert.Empty(t, catalog.savedVChannels)

	resetCapturedAsyncTasks(taskScheduler)
	manager.RequirePersist()
	require.Len(t, taskScheduler.tasks, 1)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	assert.Equal(t, uint64(10), vchannel.PersistedDataTimeTick())
	require.Len(t, catalog.savedVChannels, 1)
	assert.Equal(t, uint64(10), catalog.savedVChannels[0]["v1"].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, catalog.savedVChannels[0]["v1"].GetState())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, vchannel.AssignmentMeta().GetState())

	require.Len(t, taskScheduler.tasks, 2)
	assert.False(t, taskScheduler.tasks[1].Precondition().Ready())
	taskScheduler.handles[0].MarkDone()
	require.True(t, taskScheduler.tasks[1].Precondition().Ready())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	require.Len(t, catalog.savedVChannels, 2)
	assert.Equal(t, uint64(10), catalog.savedVChannels[1]["v1"].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, catalog.savedVChannels[1]["v1"].GetState())
}

func TestGrowingManagerTombstoneFinalizeWaitsForPreviousPersistTask(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog))
	previousPersist := &captureTaskHandle{}
	manager.lastPersistTask = previousPersist

	manager.RequirePersist()

	require.Len(t, taskScheduler.tasks, 1)
	assert.False(t, taskScheduler.tasks[0].Precondition().Ready())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, manager.retainedVChannel("v1").AssignmentMeta().GetState())

	previousPersist.MarkDone()
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	taskScheduler.handles[0].MarkDone()

	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, manager.retainedVChannel("v1").AssignmentMeta().GetState())
	require.Len(t, catalog.savedVChannels, 1)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, catalog.savedVChannels[0]["v1"].GetState())
}

func TestGrowingManagerObserveRepeatedDropCollectionKeepsDirtyMetaBarrier(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{})
	msg := newTestDropCollectionMessage(10)

	first := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, first.Meta)
	assert.Equal(t, uint64(4), first.Meta.TimeTick())

	second := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, second.Meta)
	assert.Equal(t, uint64(4), second.Meta.TimeTick())

	manager.retainedVChannel("v1").MarkMetaPersisted(10)
	third := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, third.Meta)
	assert.Equal(t, uint64(10), third.Meta.TimeTick())
}

func TestGrowingManagerObserveRepeatedFlushKeepsDirtySegmentMetaBarrier(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 10,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
		},
	}, &testSegmentLifecycleWriter{})
	msg := newTestFlushMessage(10, 10)

	first := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, first.Meta)
	assert.Equal(t, uint64(4), first.Meta.TimeTick())

	second := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, second.Meta)
	assert.Equal(t, uint64(4), second.Meta.TimeTick())

	manager.Segments()[10].MarkMetaPersisted(10)
	third := manager.ObserveMessage(context.Background(), msg)
	require.NotNil(t, third.Meta)
	assert.Equal(t, uint64(10), third.Meta.TimeTick())
}

func TestGrowingManagerManualFlushExtendsRunningTransformLogTaskTarget(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog), WithTransformLogBufferMaxRows(1))

	deleteResult := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))
	require.NotNil(t, deleteResult.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, uint64(4), deleteResult.Data.TimeTick())

	flushResult := manager.observeMessage(context.Background(), newTestManualFlushMessage(10, 10))
	require.NotNil(t, flushResult.Data)
	assert.Equal(t, uint64(4), flushResult.Data.TimeTick())
	require.Len(t, taskScheduler.tasks, 1)

	runCapturedAsyncTasksAndMarkDone(t, taskScheduler)
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 1, commitL0Calls)
	assert.Equal(t, uint64(10), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), deleteResult.Data.TimeTick())
	assert.Equal(t, uint64(4), flushResult.Data.TimeTick())
	runRequiredPersistTask(t, manager, taskScheduler)
	assert.Equal(t, uint64(10), deleteResult.Data.TimeTick())
	assert.Equal(t, uint64(10), flushResult.Data.TimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v1").PersistedDataTimeTick())
}

func TestGrowingManagerTransformLogFlushSubmitsOneTaskPerFixedChunk(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	writer := &testGrowingSegmentPackWriter{
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", catalog), WithTransformLogBufferMaxRows(3))

	manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1, 2}))
	manager.observeMessage(context.Background(), newTestDeleteMessage(6, 100, []int64{3}))
	require.Len(t, taskScheduler.tasks, 1)
	manager.observeMessage(context.Background(), newTestDeleteMessage(7, 100, []int64{4}))
	require.Len(t, taskScheduler.tasks, 1)

	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	taskScheduler.handles[0].MarkDone()
	require.NotNil(t, writer.deletePack)
	assert.Equal(t, uint64(5), writer.deletePack.FromTimeTick)
	assert.Equal(t, uint64(6), writer.deletePack.ToTimeTick)
	assert.Len(t, writer.deletePack.Deletes, 2)
	assert.Equal(t, uint64(6), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.VChannel("v1").PersistedDataTimeTick())
	require.Len(t, taskScheduler.tasks, 2)

	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	taskScheduler.handles[1].MarkDone()
	assert.Equal(t, uint64(7), writer.deletePack.FromTimeTick)
	assert.Equal(t, uint64(7), writer.deletePack.ToTimeTick)
	assert.Len(t, writer.deletePack.Deletes, 1)
	assert.Equal(t, uint64(7), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), manager.VChannel("v1").PersistedDataTimeTick())
	commitL0Calls, _ := lifecycle.commitL0Snapshot()
	assert.Equal(t, 2, commitL0Calls)
}

func TestGrowingManagerAlterCollectionWithoutSchemaChangeDoesNotFlushData(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(100))

	deleteResult := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))
	require.NotNil(t, deleteResult.Data)
	assert.Empty(t, taskScheduler.tasks)

	result := manager.observeMessage(context.Background(), newTestAlterCollectionMessage(10, []string{"properties"}, &message.AlterCollectionMessageUpdates{
		Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
	}))

	require.NotNil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, taskScheduler.tasks)
	assert.Nil(t, writer.pack)
	assert.Nil(t, writer.deletePack)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, uint64(4), manager.Segments()[10].PersistedDataTimeTick())
}

func TestGrowingManagerDroppedVChannelSkipsFutureReplay(t *testing.T) {
	testCases := []struct {
		name    string
		observe func(*Manager) moduleapi.ObserveResult
		assert  func(*testing.T, *Manager)
	}{
		{name: "schema change", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestSchemaChangeMessage(12))
		}},
		{name: "drop collection", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestDropCollectionMessage(12))
		}},
		{name: "drop partition", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestDropPartitionMessage(12, 1, 100))
		}},
		{name: "manual flush", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestManualFlushMessage(12, 10))
		}},
		{name: "truncate collection", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestTruncateCollectionMessage(12))
		}},
		{
			name: "create segment",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestCreateSegmentMessage(12, 20, 100))
			},
			assert: func(t *testing.T, manager *Manager) {
				assert.Nil(t, manager.Segments()[20])
			},
		},
		{
			name: "dml",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				insertResult := manager.observeMessage(context.Background(), newTestInsertMessage(12, 10, 100, 7, 70))
				deleteResult := manager.observeMessage(context.Background(), newTestDeleteMessage(12, 100, []int64{1}))
				assert.Nil(t, insertResult.Meta)
				assert.Nil(t, insertResult.Data)
				return deleteResult
			},
			assert: assertNoPendingGrowingData,
		},
		{
			name: "txn",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestTxnMessage(
					12,
					newTestInsertMessage(11, 10, 100, 7, 70),
					newTestDeleteMessage(11, 100, []int64{1}),
				))
			},
			assert: assertNoPendingGrowingData,
		},
		{name: "flush segment", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestFlushMessage(12, 10))
		}},
		{name: "flush all", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestFlushAllMessage(12))
		}},
		{name: "alter wal", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestAlterWALMessage(12))
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskScheduler := &captureAsyncTaskScheduler{}
			manager := newTestDroppedVChannelWithGrowingSegmentManager(taskScheduler)

			result := tc.observe(manager)

			assert.Nil(t, result.Meta)
			assert.Nil(t, result.Data)
			assert.Empty(t, taskScheduler.tasks)
			vchannel := manager.retainedVChannel("v1").AssignmentMeta()
			assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.GetState())
			assert.Equal(t, uint64(10), vchannel.GetCheckpointTimeTick())
			assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(vchannel, 100))
			assertGrowingSegmentUnchanged(t, manager)
			if tc.assert != nil {
				tc.assert(t, manager)
			}
		})
	}
}

func TestGrowingManagerDroppedPartitionSkipsFutureReplay(t *testing.T) {
	testCases := []struct {
		name    string
		observe func(*Manager) moduleapi.ObserveResult
		assert  func(*testing.T, *Manager)
	}{
		{name: "drop partition", observe: func(manager *Manager) moduleapi.ObserveResult {
			return manager.observeMessage(context.Background(), newTestDropPartitionMessage(12, 1, 100))
		}},
		{
			name: "create segment",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestCreateSegmentMessage(12, 20, 100))
			},
			assert: func(t *testing.T, manager *Manager) {
				assert.Nil(t, manager.Segments()[20])
			},
		},
		{
			name: "dml",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				insertResult := manager.observeMessage(context.Background(), newTestInsertMessage(12, 10, 100, 7, 70))
				deleteResult := manager.observeMessage(context.Background(), newTestDeleteMessage(12, 100, []int64{1}))
				assert.Nil(t, insertResult.Meta)
				assert.Nil(t, insertResult.Data)
				return deleteResult
			},
			assert: assertNoPendingGrowingData,
		},
		{
			name: "txn",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestTxnMessage(
					12,
					newTestInsertMessage(11, 10, 100, 7, 70),
					newTestDeleteMessage(11, 100, []int64{1}),
				))
			},
			assert: assertNoPendingGrowingData,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskScheduler := &captureAsyncTaskScheduler{}
			manager := newTestDroppedPartitionWithGrowingSegmentManager(taskScheduler)

			result := tc.observe(manager)

			assert.Nil(t, result.Meta)
			assert.Nil(t, result.Data)
			assert.Empty(t, taskScheduler.tasks)
			vchannel := manager.retainedVChannel("v1").AssignmentMeta()
			assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_DROPPED, partitionState(vchannel, 100))
			assert.Equal(t, uint64(10), partitionTombstoneTimeTick(vchannel, 100))
			assertGrowingSegmentUnchanged(t, manager)
			if tc.assert != nil {
				tc.assert(t, manager)
			}
		})
	}
}

func assertGrowingSegmentUnchanged(t *testing.T, manager *Manager) {
	segment := manager.Segments()[10]
	require.NotNil(t, segment)
	assert.Equal(t, uint64(4), segment.Meta().GetCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, segment.Meta().GetState())
}

func assertNoPendingGrowingData(t *testing.T, manager *Manager) {
	assert.Empty(t, manager.Segments()[10].pending.entries)
	assert.Empty(t, manager.retainedVChannel("v1").transformLogBuffer.entries)
}

func newTestDroppedVChannelWithGrowingSegmentManager(taskScheduler *captureAsyncTaskScheduler) *Manager {
	return newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
}

func newTestDroppedPartitionWithGrowingSegmentManager(taskScheduler *captureAsyncTaskScheduler) *Manager {
	return newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       100,
						State:             streamingpb.PartitionState_PARTITION_STATE_DROPPED,
						TombstoneTimeTick: 10,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))
}

func TestGrowingManagerAlterCollectionSchemaChangeFlushesData(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}},
		deleteResult: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, lifecycle, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(100))

	_ = manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))

	result := manager.observeMessage(context.Background(), newTestAlterCollectionMessage(10, []string{message.FieldMaskCollectionSchema}, &message.AlterCollectionMessageUpdates{
		Schema: newTestGrowingPackWriterSchema(),
	}))

	require.NotNil(t, result.Meta)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 2)
	assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[1].Name())
}

func TestGrowingManagerCollectionScopedFlushesRetainedSegmentFromDifferentCollection(t *testing.T) {
	testcases := []struct {
		name    string
		observe func(*Manager) moduleapi.ObserveResult
	}{
		{
			name: "manual flush",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestManualFlushMessage(10, 10))
			},
		},
		{
			name: "truncate collection",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestTruncateCollectionMessage(10))
			},
		},
		{
			name: "drop collection",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestDropCollectionMessage(10))
			},
		},
		{
			name: "drop partition",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestDropPartitionMessage(10, 1, 100))
			},
		},
		{
			name: "schema change",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestSchemaChangeMessage(10))
			},
		},
		{
			name: "alter collection schema change",
			observe: func(manager *Manager) moduleapi.ObserveResult {
				return manager.observeMessage(context.Background(), newTestAlterCollectionMessage(10, []string{message.FieldMaskCollectionSchema}, &message.AlterCollectionMessageUpdates{
					Schema: newTestGrowingPackWriterSchema(),
				}))
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			taskScheduler := &captureAsyncTaskScheduler{}
			lifecycle := &testSegmentLifecycleWriter{}
			manager := newTestManager(map[string]*streamingpb.VChannelMeta{
				"v1": {
					Vchannel:               "v1",
					State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
					CheckpointTimeTick:     4,
					DataCheckpointTimeTick: 4,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 2,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
						},
						Schemas: []*streamingpb.CollectionSchemaOfVChannel{
							{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
						},
					},
				},
			}, map[int64]*streamingpb.SegmentAssignmentMeta{
				10: {
					CollectionId:           1,
					PartitionId:            100,
					SegmentId:              10,
					Vchannel:               "v1",
					State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					CheckpointTimeTick:     6,
					DataCheckpointTimeTick: 4,
					PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
					Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
				},
			}, lifecycle, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

			result := tc.observe(manager)

			require.NotNil(t, result.Meta)
			assert.Equal(t, uint64(6), result.Meta.TimeTick())
			require.NotNil(t, result.Data)
			assert.Equal(t, uint64(4), result.Data.TimeTick())
			require.Len(t, taskScheduler.tasks, 1)
			assert.Equal(t, "growing-commit-l1-segment", taskScheduler.tasks[0].Name())
			runCapturedAsyncTasks(t, taskScheduler)

			vchannel := manager.retainedVChannel("v1").AssignmentMeta()
			assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, vchannel.GetState())
			assert.Equal(t, uint64(4), vchannel.GetCheckpointTimeTick())
			assert.Equal(t, uint64(4), vchannel.GetDataCheckpointTimeTick())
			assert.Len(t, vchannel.GetCollectionInfo().GetSchemas(), 1)
			assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, partitionState(vchannel, 100))
			assert.Nil(t, manager.retainedVChannel("v1").Segment(10))
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, manager.Segments()[10].Meta().GetState())
			assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetDataCheckpointTimeTick())
			assert.Equal(t, []int64{10}, lifecycle.commitL1SegmentIDSnapshot())
		})
	}
}

func TestGrowingManagerTombstonedVChannelFiltersVChannelMetaReplay(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick: 10,
			TombstoneTimeTick:  10,
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			LatestDataVersion:  &viewpb.DataVersion{},
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	schemaBytes, err := proto.Marshal(newTestGrowingPackWriterSchema())
	require.NoError(t, err)
	createCollection := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{100},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(10)).
		IntoImmutableMessage(rmq.NewRmqID(10))
	schemaChangeResult := manager.observeMessage(context.Background(), newTestSchemaChangeMessage(9))
	alterResult := manager.observeMessage(context.Background(), newTestAlterCollectionMessage(10, []string{message.FieldMaskCollectionSchema}, &message.AlterCollectionMessageUpdates{
		Schema: newTestGrowingPackWriterSchema(),
	}))
	dropCollectionResult := manager.observeMessage(context.Background(), newTestDropCollectionMessage(10))
	createCollectionResult := manager.observeMessage(context.Background(), createCollection)

	assert.Nil(t, schemaChangeResult.Meta)
	assert.Nil(t, schemaChangeResult.Data)
	assert.Nil(t, alterResult.Meta)
	assert.Nil(t, alterResult.Data)
	assert.Nil(t, dropCollectionResult.Meta)
	assert.Nil(t, dropCollectionResult.Data)
	assert.Nil(t, createCollectionResult.Meta)
	assert.Nil(t, createCollectionResult.Data)
	assert.Empty(t, taskScheduler.tasks)
	snapshot := manager.retainedVChannel("v1").AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, snapshot.GetState())
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	assert.Len(t, snapshot.GetCollectionInfo().GetSchemas(), 1)
}

func TestGrowingManagerFlushAllIgnoresTombstonedVChannelTransformLog(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			LatestDataVersion:      &viewpb.DataVersion{},
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}))

	result := manager.ObserveMessage(context.Background(), newTestFlushAllMessage(20))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(20), result.Data.TimeTick())
	assert.Empty(t, taskScheduler.tasks)
	vchannel := manager.retainedVChannel("v1")
	require.NotNil(t, vchannel)
	assert.Equal(t, uint64(10), vchannel.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.False(t, vchannel.HasDirty())
}

func TestVChannelTransformLogTaskDoesNotHoldLockDuringIO(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	writer := &blockingDeletePackWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: &DeleteFlushResult{
			Batch: &L0DeleteBatch{
				VChannel:     "v1",
				CollectionID: 1,
				SegmentID:    1000,
				ToTimeTick:   5,
				Deltalogs:    []*datapb.FieldBinlog{{FieldID: 100}},
			},
		},
	}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}), WithRecoveryCatalog("p1", &testRecoveryCatalog{}), WithTransformLogBufferMaxRows(1))

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)

	done := make(chan error, 1)
	go func() {
		done <- taskScheduler.tasks[0].Run(context.Background())
	}()
	<-writer.started

	vchannel := manager.retainedVChannel("v1")
	require.True(t, vchannel.mu.TryLock())
	vchannel.mu.Unlock()

	close(writer.release)
	require.NoError(t, <-done)
	assert.Equal(t, uint64(5), vchannel.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.Equal(t, uint64(4), vchannel.DataTimeTick())
}

func TestGrowingManagerObserveImportMessagesAreIgnored(t *testing.T) {
	for name, msg := range map[string]message.ImmutableMessage{
		"import":          newTestImportMessage(10, "v1"),
		"commit import":   newTestCommitImportMessage(10, 3001, "v1"),
		"rollback import": newTestRollbackImportMessage(10, 3001, "v1"),
	} {
		t.Run(name, func(t *testing.T) {
			writer := &testGrowingSegmentPackWriter{
				result: &FlushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
					Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
				}},
			}
			lifecycle := &testSegmentLifecycleWriter{}
			manager := newTestManager(map[string]*streamingpb.VChannelMeta{
				"v1": {
					Vchannel:               "v1",
					State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
					GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
					DataCheckpointTimeTick: 4,
				},
			}, map[int64]*streamingpb.SegmentAssignmentMeta{
				10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
			}, lifecycle, WithPackWriter(writer))
			insert := newTestInsertMessage(5, 10, 100, 7, 70)
			_ = manager.observeMessage(context.Background(), insert)

			result := manager.ObserveMessage(context.Background(), msg)

			require.NotNil(t, result.Meta)
			assert.Equal(t, uint64(10), result.Meta.TimeTick())
			require.NotNil(t, result.Data)
			assert.Equal(t, uint64(10), result.Data.TimeTick())
			assert.Nil(t, writer.pack)
			assert.Equal(t, uint64(4), manager.VChannel("v1").AssignmentMeta().GetDataCheckpointTimeTick())
			assert.Equal(t, uint64(4), manager.VChannel("v1").Segment(10).AssignmentMeta().GetDataCheckpointTimeTick())
			assert.Len(t, manager.VChannel("v1").Segment(10).pending.entries, 1)
		})
	}
}

func TestGrowingManagerObserveCommitImportStaleVChannelIsImmediate(t *testing.T) {
	lifecycle := &testSegmentLifecycleWriter{}
	manager := newTestManager(nil, nil, lifecycle)
	commit := newTestCommitImportMessage(10, 3001, "stale-vchannel")

	result := manager.observeMessage(context.Background(), commit)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerRecoveredSegmentSchemaRequiresCreateTimeTick(t *testing.T) {
	createSchema := &schemapb.CollectionSchema{Name: "create-schema"}
	latestSchema := &schemapb.CollectionSchema{Name: "latest-schema"}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: createSchema, CheckpointTimeTick: 3},
					{Schema: latestSchema, CheckpointTimeTick: 8},
				},
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     12,
			DataCheckpointTimeTick: 4,
			Stat:                   &streamingpb.SegmentAssignmentStat{},
		},
	}, &testSegmentLifecycleWriter{})

	assert.Nil(t, manager.Segments()[10].schema)
}

func TestGrowingManagerCreateSegmentDoesNotFallbackToFutureSchema(t *testing.T) {
	futureSchema := &schemapb.CollectionSchema{Name: "future-schema"}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: futureSchema, CheckpointTimeTick: 20},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{})

	result := manager.observeMessage(context.Background(), newTestCreateSegmentMessage(10, 10, 100))

	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Nil(t, manager.Segments()[10])
}

func TestGrowingManagerCreateCollectionAttachesRecoveredSegments(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "collection-schema"}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
		},
	}, &testSegmentLifecycleWriter{})
	require.Contains(t, manager.Segments(), int64(10))
	require.Nil(t, manager.Segments()[10].schema)

	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{
				100,
			},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "collection",
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))

	result := manager.observeMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	require.NotNil(t, manager.VChannel("v1"))
	require.Same(t, manager.Segments()[10], manager.VChannel("v1").Segment(10))
	assert.True(t, proto.Equal(schema, manager.Segments()[10].schema))
}

func TestGrowingManagerSchemaChangeRefreshesRecoveredSegmentSchema(t *testing.T) {
	createSchema := &schemapb.CollectionSchema{Name: "create-schema"}
	createSchemaBytes, err := proto.Marshal(createSchema)
	require.NoError(t, err)
	schemaChange := &schemapb.CollectionSchema{Name: "schema-change"}
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 10},
		},
	}, &testSegmentLifecycleWriter{})

	createCollection := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "collection",
			Schema:         createSchemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	manager.observeMessage(context.Background(), createCollection)
	require.True(t, proto.Equal(createSchema, manager.Segments()[10].schema))

	schemaChangeMessage := message.NewSchemaChangeMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 1}).
		WithBody(&message.SchemaChangeMessageBody{Schema: schemaChange}).
		MustBuildMutable().
		WithTimeTick(8).
		WithLastConfirmed(rmq.NewRmqID(8)).
		IntoImmutableMessage(rmq.NewRmqID(8))
	manager.observeMessage(context.Background(), schemaChangeMessage)

	assert.True(t, proto.Equal(schemaChange, manager.Segments()[10].schema))
}

func TestGrowingManagerSchemaChangeDoesNotRefreshRecoveredSegmentFromDifferentCollection(t *testing.T) {
	orphanSchema := &schemapb.CollectionSchema{Name: "orphan-segment-schema"}
	createSchema := &schemapb.CollectionSchema{Name: "new-collection-schema"}
	createSchemaBytes, err := proto.Marshal(createSchema)
	require.NoError(t, err)
	schemaChange := &schemapb.CollectionSchema{Name: "new-collection-schema-change"}
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 10},
		},
	}, &testSegmentLifecycleWriter{})
	manager.Segments()[10].SetSchema(orphanSchema)

	createCollection := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 2,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "new-collection",
			CollectionID:   2,
			PartitionIDs:   []int64{100},
			Schema:         createSchemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	manager.observeMessage(context.Background(), createCollection)
	require.True(t, proto.Equal(orphanSchema, manager.Segments()[10].schema))

	schemaChangeMessage := message.NewSchemaChangeMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 2}).
		WithBody(&message.SchemaChangeMessageBody{Schema: schemaChange}).
		MustBuildMutable().
		WithTimeTick(8).
		WithLastConfirmed(rmq.NewRmqID(8)).
		IntoImmutableMessage(rmq.NewRmqID(8))
	manager.observeMessage(context.Background(), schemaChangeMessage)

	assert.True(t, proto.Equal(orphanSchema, manager.Segments()[10].schema))
}

func TestGrowingManagerCurrentOwnerDropCollectionSkipsForeignRetainedSegment(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
			LatestDataVersion:      &viewpb.DataVersion{},
			GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			DataCheckpointTimeTick: 1,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 4),
	}, &testSegmentLifecycleWriter{}, WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}))

	drop := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 2}).
		WithBody(&msgpb.DropCollectionRequest{CollectionID: 2}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(20).
		WithLastConfirmed(rmq.NewRmqID(20)).
		IntoImmutableMessage(rmq.NewRmqID(20))
	result := manager.observeMessage(context.Background(), drop)

	require.NotNil(t, result.Meta)
	require.NotNil(t, result.Data)
	require.Len(t, taskScheduler.tasks, 1)
	assert.Equal(t, "growing-flush-transform-log-buffer", taskScheduler.tasks[0].Name())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, manager.retainedVChannel("v1").AssignmentMeta().GetState())
	assert.True(t, manager.Segments()[10].IsGrowing())
	assert.Nil(t, manager.retainedVChannel("v1").Segment(10))
}

func TestGrowingPersistDoesNotOverwriteNewerSegmentDataMeta(t *testing.T) {
	catalog := newBlockingRecoveryCatalog(4)
	manager := newTestManager(nil, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            100,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
		},
	}, &testSegmentLifecycleWriter{}, WithRecoveryCatalog("p1", catalog))
	segment := manager.Segments()[10]
	segment.persistedMetaTimeTick = 4
	segment.dirty = true
	task := manager.NewPersistTask("p1", catalog, nil, nil, nil)
	require.NotNil(t, task)

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Run(context.Background())
	}()
	<-catalog.blocked

	segment.mu.Lock()
	segment.markDataCheckpointLocked(10)
	segment.mu.Unlock()

	close(catalog.release)
	require.NoError(t, <-errCh)
	assert.Equal(t, uint64(10), catalog.segment(10).GetDataCheckpointTimeTick())
}

func TestGrowingPersistDoesNotOverwriteNewerVChannelDataMeta(t *testing.T) {
	catalog := newBlockingRecoveryCatalog(4)
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
		},
	}, nil, &testSegmentLifecycleWriter{}, WithRecoveryCatalog("p1", catalog))
	vchannel := manager.retainedVChannel("v1")
	vchannel.persistedMetaTimeTick = 4
	vchannel.dirty = true
	task := manager.NewPersistTask("p1", catalog, nil, nil, nil)
	require.NotNil(t, task)

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Run(context.Background())
	}()
	<-catalog.blocked

	vchannel.mu.Lock()
	vchannel.markDataCheckpointLocked(10)
	vchannel.mu.Unlock()

	close(catalog.release)
	require.NoError(t, <-errCh)
	assert.Equal(t, uint64(10), catalog.vchannel("v1").GetDataCheckpointTimeTick())
}

func TestGrowingPersistSavesVChannelBeforeSegments(t *testing.T) {
	catalog := newOrderingRecoveryCatalog()
	manager := NewManager(nil, nil, &testSegmentLifecycleWriter{}, WithRecoveryCatalog("p1", catalog))
	schemaBytes, err := proto.Marshal(newTestGrowingPackWriterSchema())
	require.NoError(t, err)
	createCollection := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   1,
			PartitionIDs:   []int64{100},
			Schema:         schemaBytes,
		}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(5).
		WithLastConfirmed(rmq.NewRmqID(5)).
		IntoImmutableMessage(rmq.NewRmqID(5))
	manager.observeMessage(context.Background(), createCollection)
	manager.observeMessage(context.Background(), newTestCreateSegmentMessage(6, 10, 100))
	task := manager.NewPersistTask("p1", catalog, nil, nil, nil)
	require.NotNil(t, task)

	errCh := make(chan error, 1)
	go func() {
		errCh <- task.Run(context.Background())
	}()
	require.Eventually(t, catalog.vchannelSaveStarted, time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	catalog.releaseVChannelSave()
	require.NoError(t, <-errCh)

	assert.False(t, catalog.segmentSavedBeforeVChannel())
	require.Len(t, catalog.savedVChannels, 1)
	require.Len(t, catalog.savedSegments, 1)
}

type testGrowingSegmentPackWriter struct {
	pack         *FlushPack
	result       *FlushResult
	err          error
	errs         []error
	deletePack   *DeleteFlushPack
	deleteResult *DeleteFlushResult
	deleteErr    error
	deleteErrs   []error
}

type blockingDeletePackWriter struct {
	started chan struct{}
	release chan struct{}
	result  *DeleteFlushResult
	pack    *DeleteFlushPack
}

func (w *blockingDeletePackWriter) FlushInsertBuffer(context.Context, *FlushPack) (*FlushResult, error) {
	return nil, nil
}

func (w *blockingDeletePackWriter) FlushDeleteBuffer(_ context.Context, pack *DeleteFlushPack) (*DeleteFlushResult, error) {
	w.pack = pack
	close(w.started)
	<-w.release
	return w.result, nil
}

func (w *testGrowingSegmentPackWriter) FlushInsertBuffer(
	_ context.Context,
	pack *FlushPack,
) (*FlushResult, error) {
	w.pack = pack
	return w.result, nextTestErr(&w.errs, w.err)
}

func (w *testGrowingSegmentPackWriter) FlushDeleteBuffer(
	_ context.Context,
	pack *DeleteFlushPack,
) (*DeleteFlushResult, error) {
	w.deletePack = pack
	return w.deleteResult, nextTestErr(&w.deleteErrs, w.deleteErr)
}

type testSegmentLifecycleWriter struct {
	mu                 sync.Mutex
	ensureCalls        int
	ensureMeta         *streamingpb.SegmentAssignmentMeta
	commitL1Calls      int
	commitL1Meta       *streamingpb.SegmentAssignmentMeta
	commitL1SegmentIDs []int64
	commitL0Calls      int
	commitL0Batch      *L0DeleteBatch
}

type testRecoveryCatalog struct {
	savedSegments    []map[int64]*streamingpb.SegmentAssignmentMeta
	savedVChannels   []map[string]*streamingpb.VChannelMeta
	droppedSegments  [][]int64
	droppedVChannels []map[string]*streamingpb.VChannelMeta
	ops              []string
}

type orderingRecoveryCatalog struct {
	testRecoveryCatalog
	mu                             sync.Mutex
	vchannelStarted                chan struct{}
	vchannelRelease                chan struct{}
	vchannelDone                   bool
	segmentSavedBeforeVChannelDone bool
}

func newOrderingRecoveryCatalog() *orderingRecoveryCatalog {
	return &orderingRecoveryCatalog{
		vchannelStarted: make(chan struct{}),
		vchannelRelease: make(chan struct{}),
	}
}

func (c *orderingRecoveryCatalog) SaveVChannels(ctx context.Context, pchannel string, vchannels map[string]*streamingpb.VChannelMeta) error {
	close(c.vchannelStarted)
	<-c.vchannelRelease
	if err := c.testRecoveryCatalog.SaveVChannels(ctx, pchannel, vchannels); err != nil {
		return err
	}
	c.mu.Lock()
	c.vchannelDone = true
	c.mu.Unlock()
	return nil
}

func (c *orderingRecoveryCatalog) SaveSegmentAssignments(ctx context.Context, pchannel string, segments map[int64]*streamingpb.SegmentAssignmentMeta) error {
	c.mu.Lock()
	if !c.vchannelDone {
		c.segmentSavedBeforeVChannelDone = true
	}
	c.mu.Unlock()
	return c.testRecoveryCatalog.SaveSegmentAssignments(ctx, pchannel, segments)
}

func (c *orderingRecoveryCatalog) vchannelSaveStarted() bool {
	select {
	case <-c.vchannelStarted:
		return true
	default:
		return false
	}
}

func (c *orderingRecoveryCatalog) releaseVChannelSave() {
	close(c.vchannelRelease)
}

func (c *orderingRecoveryCatalog) segmentSavedBeforeVChannel() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.segmentSavedBeforeVChannelDone
}

func (c *testRecoveryCatalog) SaveVChannels(_ context.Context, _ string, vchannels map[string]*streamingpb.VChannelMeta) error {
	c.ops = append(c.ops, "save-vchannels")
	c.savedVChannels = append(c.savedVChannels, lo.MapValues(vchannels, func(meta *streamingpb.VChannelMeta, _ string) *streamingpb.VChannelMeta {
		return proto.Clone(meta).(*streamingpb.VChannelMeta)
	}))
	return nil
}

func (c *testRecoveryCatalog) SaveSegmentAssignments(_ context.Context, _ string, segments map[int64]*streamingpb.SegmentAssignmentMeta) error {
	c.ops = append(c.ops, "save-segments")
	c.savedSegments = append(c.savedSegments, lo.MapValues(segments, func(meta *streamingpb.SegmentAssignmentMeta, _ int64) *streamingpb.SegmentAssignmentMeta {
		return proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
	}))
	return nil
}

func (c *testRecoveryCatalog) DropVChannels(_ context.Context, _ string, vchannels map[string]*streamingpb.VChannelMeta) error {
	c.ops = append(c.ops, "drop-vchannels")
	c.droppedVChannels = append(c.droppedVChannels, lo.MapValues(vchannels, func(meta *streamingpb.VChannelMeta, _ string) *streamingpb.VChannelMeta {
		return proto.Clone(meta).(*streamingpb.VChannelMeta)
	}))
	return nil
}

func (c *testRecoveryCatalog) DropSegmentAssignments(_ context.Context, _ string, segmentIDs []int64) error {
	c.ops = append(c.ops, "drop-segments")
	c.droppedSegments = append(c.droppedSegments, append([]int64(nil), segmentIDs...))
	return nil
}

type blockingRecoveryCatalog struct {
	mu        sync.Mutex
	blockTick uint64
	blocked   chan struct{}
	release   chan struct{}
	blockOnce sync.Once
	segments  map[int64]*streamingpb.SegmentAssignmentMeta
	vchannels map[string]*streamingpb.VChannelMeta
}

func newBlockingRecoveryCatalog(blockTick uint64) *blockingRecoveryCatalog {
	return &blockingRecoveryCatalog{
		blockTick: blockTick,
		blocked:   make(chan struct{}),
		release:   make(chan struct{}),
		segments:  make(map[int64]*streamingpb.SegmentAssignmentMeta),
		vchannels: make(map[string]*streamingpb.VChannelMeta),
	}
}

func (c *blockingRecoveryCatalog) SaveVChannels(_ context.Context, _ string, vchannels map[string]*streamingpb.VChannelMeta) error {
	for _, vchannel := range vchannels {
		c.blockIfNeeded(vchannel.GetDataCheckpointTimeTick())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for vchannelName, vchannel := range vchannels {
		c.vchannels[vchannelName] = proto.Clone(vchannel).(*streamingpb.VChannelMeta)
	}
	return nil
}

func (c *blockingRecoveryCatalog) SaveSegmentAssignments(_ context.Context, _ string, segments map[int64]*streamingpb.SegmentAssignmentMeta) error {
	for _, segment := range segments {
		c.blockIfNeeded(segment.GetDataCheckpointTimeTick())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for segmentID, segment := range segments {
		c.segments[segmentID] = proto.Clone(segment).(*streamingpb.SegmentAssignmentMeta)
	}
	return nil
}

func (c *blockingRecoveryCatalog) DropVChannels(context.Context, string, map[string]*streamingpb.VChannelMeta) error {
	return nil
}

func (c *blockingRecoveryCatalog) DropSegmentAssignments(context.Context, string, []int64) error {
	return nil
}

func (c *blockingRecoveryCatalog) blockIfNeeded(timetick uint64) {
	if timetick != c.blockTick {
		return
	}
	c.blockOnce.Do(func() {
		close(c.blocked)
		<-c.release
	})
}

func (c *blockingRecoveryCatalog) segment(segmentID int64) *streamingpb.SegmentAssignmentMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Clone(c.segments[segmentID]).(*streamingpb.SegmentAssignmentMeta)
}

func (c *blockingRecoveryCatalog) vchannel(vchannelName string) *streamingpb.VChannelMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	return proto.Clone(c.vchannels[vchannelName]).(*streamingpb.VChannelMeta)
}

func (w *testSegmentLifecycleWriter) EnsureGrowingSegment(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.ensureCalls++
	w.ensureMeta = proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
	return nil
}

func (w *testSegmentLifecycleWriter) CommitL1Segment(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.commitL1Calls++
	w.commitL1Meta = proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
	w.commitL1SegmentIDs = append(w.commitL1SegmentIDs, meta.GetSegmentId())
	return nil
}

func (w *testSegmentLifecycleWriter) CommitL0Segment(_ context.Context, batch *L0DeleteBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.commitL0Calls++
	w.commitL0Batch = batch
	return nil
}

func nextTestErr(errs *[]error, fallback error) error {
	if len(*errs) == 0 {
		return fallback
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (w *testSegmentLifecycleWriter) ensureSnapshot() (int, *streamingpb.SegmentAssignmentMeta) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.ensureMeta == nil {
		return w.ensureCalls, nil
	}
	return w.ensureCalls, proto.Clone(w.ensureMeta).(*streamingpb.SegmentAssignmentMeta)
}

func (w *testSegmentLifecycleWriter) commitL1Snapshot() (int, *streamingpb.SegmentAssignmentMeta) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.commitL1Meta == nil {
		return w.commitL1Calls, nil
	}
	return w.commitL1Calls, proto.Clone(w.commitL1Meta).(*streamingpb.SegmentAssignmentMeta)
}

func (w *testSegmentLifecycleWriter) commitL1SegmentIDSnapshot() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]int64(nil), w.commitL1SegmentIDs...)
}

func (w *testSegmentLifecycleWriter) commitL0Snapshot() (int, *L0DeleteBatch) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.commitL0Calls, w.commitL0Batch
}

func newTestCreateSegmentMessage(
	timetick uint64,
	segmentID int64,
	partitionID int64,
) message.ImmutableCreateSegmentMessageV2 {
	msg := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    partitionID,
			SegmentId:      segmentID,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableCreateSegmentMessageV2(msg)
}

func newTestFlushMessage(
	timetick uint64,
	segmentID int64,
) message.ImmutableFlushMessageV2 {
	msg := message.NewFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 1,
			SegmentId:    segmentID,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableFlushMessageV2(msg)
}

func newTestManualFlushMessage(
	timetick uint64,
	segmentIDs ...int64,
) message.ImmutableManualFlushMessageV2 {
	msg := message.NewManualFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 1,
			FlushTs:      timetick,
			SegmentIds:   segmentIDs,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableManualFlushMessageV2(msg)
}

func newTestFlushAllMessage(timetick uint64) message.ImmutableFlushAllMessageV2 {
	msg := message.NewFlushAllMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableFlushAllMessageV2(msg)
}

func newTestPChannelLevelFlushAllMessage(timetick uint64) message.ImmutableFlushAllMessageV2 {
	broadcast := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithClusterLevelBroadcast(message.ClusterChannels{
			Channels:       []string{"test-channel"},
			ControlChannel: "test-channel_vcchan",
		}).
		MustBuildBroadcast()
	broadcast.WithBroadcastID(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableFlushAllMessageV2(msg)
}

func newTestTruncateCollectionMessage(timetick uint64) message.ImmutableTruncateCollectionMessageV2 {
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableTruncateCollectionMessageV2(msg)
}

func newTestDropCollectionMessage(timetick uint64) message.ImmutableDropCollectionMessageV1 {
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DropCollectionRequest{CollectionID: 1}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableDropCollectionMessageV1(msg)
}

func newTestDropPartitionMessage(timetick uint64, collectionID int64, partitionID int64) message.ImmutableDropPartitionMessageV1 {
	msg := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{CollectionId: collectionID, PartitionId: partitionID}).
		WithBody(&msgpb.DropPartitionRequest{CollectionID: collectionID, PartitionID: partitionID}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableDropPartitionMessageV1(msg)
}

func newTestAlterWALMessage(timetick uint64) message.ImmutableAlterWALMessageV2 {
	broadcast := message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{
			TargetWalName: commonpb.WALName_Test,
			Config:        map[string]string{},
		}).
		WithBody(&message.AlterWALMessageBody{}).
		WithClusterLevelBroadcast(message.ClusterChannels{
			Channels:       []string{"test-channel"},
			ControlChannel: "test-channel_vcchan",
		}).
		MustBuildBroadcast()
	broadcast.WithBroadcastID(1)
	msg := broadcast.SplitIntoMutableMessage()[0].
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableAlterWALMessageV2(msg)
}

func newTestAlterCollectionMessage(
	timetick uint64,
	updateMask []string,
	updates *message.AlterCollectionMessageUpdates,
) message.ImmutableAlterCollectionMessageV2 {
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: updateMask},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: updates,
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableAlterCollectionMessageV2(msg)
}

func newTestSchemaChangeMessage(timetick uint64) message.ImmutableSchemaChangeMessageV2 {
	msg := message.NewSchemaChangeMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SchemaChangeMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&message.SchemaChangeMessageBody{
			Schema: newTestGrowingPackWriterSchema(),
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableSchemaChangeMessageV2(msg)
}

func newTestDeleteMessage(
	timetick uint64,
	partitionID int64,
	primaryKeys []int64,
) message.ImmutableDeleteMessageV1 {
	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DeleteRequest{
			CollectionID: 1,
			PartitionID:  partitionID,
			PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: primaryKeys}},
			},
			Timestamps: lo.RepeatBy(len(primaryKeys), func(int) uint64 { return timetick }),
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func newTestTxnMessage(timetick uint64, messages ...message.ImmutableMessage) message.ImmutableTxnMessage {
	txnCtx := message.TxnContext{TxnID: message.TxnID(timetick)}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick - 1).
		WithTxnContext(txnCtx).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick - 1))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick - 1)))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	for _, msg := range messages {
		switch msg.MessageType() {
		case message.MessageTypeInsert:
			insert := message.MustAsImmutableInsertMessageV1(msg)
			builder.Add(message.NewInsertMessageBuilderV1().
				WithVChannel(insert.VChannel()).
				WithHeader(insert.Header()).
				WithBody(insert.MustBody()).
				MustBuildMutable().
				WithTimeTick(insert.TimeTick()).
				WithTxnContext(txnCtx).
				WithLastConfirmed(rmq.NewRmqID(int64(insert.TimeTick()))).
				IntoImmutableMessage(rmq.NewRmqID(int64(insert.TimeTick()))))
		case message.MessageTypeDelete:
			deleted := message.MustAsImmutableDeleteMessageV1(msg)
			builder.Add(message.NewDeleteMessageBuilderV1().
				WithVChannel(deleted.VChannel()).
				WithHeader(deleted.Header()).
				WithBody(deleted.MustBody()).
				MustBuildMutable().
				WithTimeTick(deleted.TimeTick()).
				WithTxnContext(txnCtx).
				WithLastConfirmed(rmq.NewRmqID(int64(deleted.TimeTick()))).
				IntoImmutableMessage(rmq.NewRmqID(int64(deleted.TimeTick()))))
		}
	}
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithTxnContext(txnCtx).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	if err != nil {
		panic(err)
	}
	return txn
}

type txnWithBodyMessages struct {
	message.ImmutableTxnMessage
	messages []message.ImmutableMessage
}

func (t txnWithBodyMessages) RangeOver(visitor func(message.ImmutableMessage) error) error {
	for _, msg := range t.messages {
		if err := visitor(msg); err != nil {
			return err
		}
	}
	return nil
}

func newTestCommitImportMessage(timetick uint64, jobID int64, vchannel string) message.ImmutableCommitImportMessageV2 {
	msg := message.NewCommitImportMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 1,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableCommitImportMessageV2(msg)
}

func newTestImportMessage(timetick uint64, vchannel string) message.ImmutableImportMessageV1 {
	msg := message.NewImportMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			CollectionID: 1,
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableImportMessageV1(msg)
}

func newTestRollbackImportMessage(timetick uint64, jobID int64, vchannel string) message.ImmutableRollbackImportMessageV2 {
	msg := message.NewRollbackImportMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.RollbackImportMessageHeader{
			CollectionId: 1,
			JobId:        jobID,
		}).
		WithBody(&message.RollbackImportMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableRollbackImportMessageV2(msg)
}

func newTestSegmentRecoveryInfo(
	collectionID int64,
	partitionID int64,
	segmentID int64,
	vchannel string,
	dataCheckpoint uint64,
) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		CollectionId:           collectionID,
		PartitionId:            partitionID,
		SegmentId:              segmentID,
		Vchannel:               vchannel,
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		DataCheckpointTimeTick: dataCheckpoint,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 3},
	}
}

func newTestInsertMessage(
	timetick uint64,
	segmentID int64,
	partitionID int64,
	rows uint64,
	binarySize uint64,
) message.ImmutableInsertMessageV1 {
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId:       partitionID,
					Rows:              rows,
					BinarySize:        binarySize,
					SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: segmentID},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1, PartitionID: partitionID, SegmentID: segmentID, NumRows: rows}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableInsertMessageV1(msg)
}
