package growing

import (
	"context"
	"math"
	"testing"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/log"
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

func TestGrowingManagerPersistSavesAllDirtyOwners(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveVChannels(mock.Anything, "test_channel", mock.MatchedBy(func(vchannels map[string]*streamingpb.VChannelMeta) bool {
		return assert.Len(t, vchannels, 2) &&
			assert.Contains(t, vchannels, "v1") &&
			assert.Contains(t, vchannels, "v2") &&
			assert.Equal(t, uint64(5), vchannels["v1"].GetCheckpointTimeTick()) &&
			assert.Equal(t, uint64(10), vchannels["v2"].GetCheckpointTimeTick())
	})).Return(nil).Once()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
		"v2": {
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
			},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}

	taskScheduler := &captureAsyncTaskScheduler{}
	manager := NewManager(
		vchannels,
		nil,
		nil,
		WithRecoveryCatalog("test_channel", snCatalog),
		WithModuleRuntime(log.With(), moduleapi.Runtime{Scheduler: taskScheduler}),
	)
	manager.VChannel("v1").ObserveCreatePartitionMessageV1(newTestCreatePartitionMessage("v1", 1, 11, 5))
	manager.VChannel("v2").ObserveCreatePartitionMessageV1(newTestCreatePartitionMessage("v2", 2, 22, 10))

	manager.RequirePersist()
	require.Len(t, taskScheduler.tasks, 1)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))

	assert.Equal(t, uint64(5), manager.VChannel("v1").MetaTimeTick())
	assert.Equal(t, uint64(10), manager.VChannel("v2").MetaTimeTick())
}

func TestGrowingManagerDirtyVChannelSnapshotIncludesCurrentDataCheckpoint(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     5,
			DataCheckpointTimeTick: 5,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	vchannel := manager.VChannel("v1")
	vchannel.MarkDataCheckpoint(20)
	vchannel.ObserveCreatePartitionMessageV1(newTestCreatePartitionMessage("v1", 1, 101, 10))

	snapshot := manager.collectDirtyOwners().consumeSnapshot()
	require.NotNil(t, snapshot)
	require.Contains(t, snapshot.VChannels, "v1")
	assert.Equal(t, uint64(10), snapshot.VChannels["v1"].GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), snapshot.VChannels["v1"].GetDataCheckpointTimeTick())
	manager.markSnapshotPersisted(snapshot)

	repeated := manager.collectDirtyOwners().consumeSnapshot()
	assert.Nil(t, repeated)
}

func TestGrowingManagerDirtySegmentSnapshotIncludesCurrentDataCheckpoint(t *testing.T) {
	manager := newTestManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     5,
			DataCheckpointTimeTick: 5,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{Schema: newTestGrowingPackWriterSchema(), CheckpointTimeTick: 1},
				},
			},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: newTestSegmentRecoveryInfo(1, 100, 10, "v1", 5),
	}, nil)
	segment := manager.Segments()[10]
	segment.MarkDataCheckpoint(20)
	insert := newTestInsertMessage(10, 10, 100, 1, 10)
	segment.ObserveInsertMessageV1(context.Background(), insert, insert.Header().GetPartitions()[0])

	snapshot := manager.collectDirtyOwners().consumeSnapshot()
	require.NotNil(t, snapshot)
	require.Contains(t, snapshot.SegmentAssignments, int64(10))
	assert.Equal(t, uint64(10), snapshot.SegmentAssignments[10].GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), snapshot.SegmentAssignments[10].GetDataCheckpointTimeTick())
	manager.markSnapshotPersisted(snapshot)

	repeated := manager.collectDirtyOwners().consumeSnapshot()
	assert.Nil(t, repeated)
}

func TestGrowingManagerPersistRunsWithoutLogger(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveVChannels(mock.Anything, "test_channel", mock.MatchedBy(func(vchannels map[string]*streamingpb.VChannelMeta) bool {
		return assert.Len(t, vchannels, 1) &&
			assert.Contains(t, vchannels, "v1") &&
			assert.Equal(t, uint64(5), vchannels["v1"].GetCheckpointTimeTick())
	})).Return(nil).Once()

	taskScheduler := &captureAsyncTaskScheduler{}
	manager := NewManager(
		map[string]*streamingpb.VChannelMeta{
			"v1": {
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
				},
				GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			},
		},
		nil,
		nil,
		WithRecoveryCatalog("test_channel", snCatalog),
		WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}),
	)
	manager.VChannel("v1").ObserveCreatePartitionMessageV1(newTestCreatePartitionMessage("v1", 1, 11, 5))

	manager.RequirePersist()
	require.Len(t, taskScheduler.tasks, 1)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
}

func TestGrowingManagerPersistSkipsCleanManager(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	manager := NewManager(
		map[string]*streamingpb.VChannelMeta{
			"v1": {
				Vchannel:               "v1",
				State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CheckpointTimeTick:     5,
				DataCheckpointTimeTick: 5,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
				},
				GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
			},
		},
		nil,
		nil,
		WithModuleRuntime(nil, moduleapi.Runtime{Scheduler: taskScheduler}),
	)

	manager.RequirePersist()

	assert.Empty(t, taskScheduler.tasks)
}

func TestGrowingManagerPersistFinalizesRecoveredFlushedSegmentTombstone(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveSegmentAssignments(mock.Anything, "test_channel", mock.MatchedBy(func(segments map[int64]*streamingpb.SegmentAssignmentMeta) bool {
		return assert.Len(t, segments, 1) &&
			assert.Contains(t, segments, int64(10)) &&
			assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, segments[10].GetState()) &&
			assert.Equal(t, uint64(10), segments[10].GetTombstoneTimeTick())
	})).Return(nil).Once()

	taskScheduler := &captureAsyncTaskScheduler{}
	manager := NewManager(
		nil,
		map[int64]*streamingpb.SegmentAssignmentMeta{
			10: {
				SegmentId:              10,
				Vchannel:               "v1",
				State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				CheckpointTimeTick:     10,
				DataCheckpointTimeTick: 10,
			},
		},
		nil,
		WithRecoveryCatalog("test_channel", snCatalog),
		WithModuleRuntime(log.With(), moduleapi.Runtime{Scheduler: taskScheduler}),
	)

	manager.RequirePersist()

	require.Len(t, taskScheduler.tasks, 1)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, manager.Segments()[10].Meta().GetState())
}

func TestGrowingManagerPersistSavesDataCheckpointBeforeSegmentTombstone(t *testing.T) {
	taskScheduler := &captureAsyncTaskScheduler{}
	catalog := &testRecoveryCatalog{}
	manager := NewManager(
		nil,
		map[int64]*streamingpb.SegmentAssignmentMeta{
			10: {
				SegmentId:              10,
				Vchannel:               "v1",
				State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				CheckpointTimeTick:     10,
				DataCheckpointTimeTick: 4,
			},
		},
		nil,
		WithRecoveryCatalog("test_channel", catalog),
		WithModuleRuntime(log.With(), moduleapi.Runtime{Scheduler: taskScheduler}),
	)
	manager.Segments()[10].MarkDataCheckpoint(10)

	manager.RequirePersist()

	require.Len(t, taskScheduler.tasks, 1)
	require.NoError(t, taskScheduler.tasks[0].Run(context.Background()))
	require.Len(t, catalog.savedSegments, 1)
	require.Contains(t, catalog.savedSegments[0], int64(10))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, catalog.savedSegments[0][10].GetState())
	assert.Equal(t, uint64(10), catalog.savedSegments[0][10].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetTombstoneTimeTick())

	require.Len(t, taskScheduler.tasks, 2)
	assert.False(t, taskScheduler.tasks[1].Precondition().Ready())
	taskScheduler.handles[0].MarkDone()
	require.True(t, taskScheduler.tasks[1].Precondition().Ready())
	require.NoError(t, taskScheduler.tasks[1].Run(context.Background()))
	require.Len(t, catalog.savedSegments, 2)
	require.Contains(t, catalog.savedSegments[1], int64(10))
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, catalog.savedSegments[1][10].GetState())
	assert.Equal(t, uint64(10), catalog.savedSegments[1][10].GetDataCheckpointTimeTick())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, manager.Segments()[10].Meta().GetState())
	assert.Equal(t, uint64(10), manager.Segments()[10].Meta().GetTombstoneTimeTick())
}

func TestGrowingManagerVChannelDurableFrontierIgnoresEmptyActiveOwner(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 20,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, nil, nil)

	assert.Equal(t, uint64(math.MaxUint64), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(math.MaxUint64), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(math.MaxUint64), manager.AllDurableFrontier().TimeTick())
}

func TestGrowingManagerSnapshotFiltersPartitionsByVChannel(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
			},
		},
		"v2": {
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 999, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
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
		20: {
			CollectionId: 1,
			PartitionId:  999,
			SegmentId:    20,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		30: {
			CollectionId: 2,
			PartitionId:  999,
			SegmentId:    30,
			Vchannel:     "v2",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		40: {
			CollectionId: 2,
			PartitionId:  100,
			SegmentId:    40,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}, nil)

	snapshot := manager.Snapshot()

	assert.Contains(t, snapshot.SegmentAssignments, int64(10))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(20))
	assert.Contains(t, snapshot.SegmentAssignments, int64(30))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(40))
}

func TestGrowingManagerSnapshotExposesOnlyActivePartitions(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					{PartitionId: 101, State: streamingpb.PartitionState_PARTITION_STATE_DROPPED, TombstoneTimeTick: 10},
					{PartitionId: 102, State: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, TombstoneTimeTick: 20},
					{PartitionId: 103, State: streamingpb.PartitionState_PARTITION_STATE_UNKNOWN},
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
		20: {
			CollectionId: 1,
			PartitionId:  101,
			SegmentId:    20,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		30: {
			CollectionId: 1,
			PartitionId:  102,
			SegmentId:    30,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
		40: {
			CollectionId: 1,
			PartitionId:  103,
			SegmentId:    40,
			Vchannel:     "v1",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}, nil)

	snapshot := manager.Snapshot()

	require.Len(t, snapshot.VChannels["v1"].GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(100), snapshot.VChannels["v1"].GetCollectionInfo().GetPartitions()[0].GetPartitionId())
	assert.Contains(t, snapshot.SegmentAssignments, int64(10))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(20))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(30))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(40))
}

func TestGrowingManagerSnapshotRequiresExplicitNormalVChannel(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 100, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
			},
		},
		"v2": {
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_UNKNOWN,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 200, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
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
		20: {
			CollectionId: 2,
			PartitionId:  200,
			SegmentId:    20,
			Vchannel:     "v2",
			State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}, nil)

	snapshot := manager.Snapshot()

	assert.Contains(t, snapshot.VChannels, "v1")
	assert.NotContains(t, snapshot.VChannels, "v2")
	assert.Contains(t, snapshot.SegmentAssignments, int64(10))
	assert.NotContains(t, snapshot.SegmentAssignments, int64(20))
}

func TestGrowingManagerVChannelDurableFrontierUsesDataCheckpointWhenDeletePending(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     1,
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
	}, nil, nil, WithTransformLogBufferMaxRows(100))
	manager.SwitchIntoMetaAndData()

	result := manager.observeMessage(context.Background(), newTestDeleteMessage(5, 100, []int64{1}))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(4), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(4), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(4), manager.AllDurableFrontier().TimeTick())
}

func TestGrowingManagerVChannelDurableFrontierWaitsForUnpersistedDataCheckpoint(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 4,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, nil, nil)

	manager.VChannels()["v1"].MarkDataCheckpoint(20)

	assert.Equal(t, uint64(4), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(4), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(4), manager.AllDurableFrontier().TimeTick())
}

func TestGrowingManagerSegmentDurableFrontierWaitsForUnpersistedDataCheckpoint(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
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
			DataCheckpointTimeTick: 4,
		},
	}, nil)

	manager.Segments()[10].MarkDataCheckpoint(20)

	assert.Equal(t, uint64(4), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(4), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(4), manager.AllDurableFrontier().TimeTick())
}

func TestGrowingManagerDurableFrontierIgnoresTombstonedOwners(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
		"v2": {
			Vchannel:               "v2",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 20,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
			},
		},
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			TombstoneTimeTick:      10,
		},
	}, nil)

	assert.Equal(t, uint64(math.MaxUint64), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(math.MaxUint64), manager.AllDurableFrontier().TimeTick())
}

func TestGrowingManagerDurableFrontierWaitsForDirtySegmentTombstone(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
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
	}, nil)
	segment := manager.Segments()[10]

	segment.TryFinalizeTombstone()

	assert.Equal(t, uint64(9), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(9), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(9), manager.AllDurableFrontier().TimeTick())

	snapshot := segment.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(9), manager.VChannelDurableFrontier("v1").TimeTick())

	segment.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, uint64(math.MaxUint64), manager.VChannelDurableFrontier("v1").TimeTick())
}

func TestGrowingManagerDurableFrontierWaitsForDirtyVChannelTombstone(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, nil, nil)
	vchannel := manager.VChannels()["v1"]

	vchannel.TryFinalizeTombstone()

	assert.Equal(t, uint64(9), manager.VChannelDurableFrontier("v1").TimeTick())
	assert.Equal(t, uint64(9), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(9), manager.AllDurableFrontier().TimeTick())

	snapshot := vchannel.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(9), manager.VChannelDurableFrontier("v1").TimeTick())

	vchannel.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, uint64(math.MaxUint64), manager.VChannelDurableFrontier("v1").TimeTick())
}

func TestGrowingManagerDurableFrontierWaitsForDirtyPartitionTombstone(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:               "v1",
			State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:       100,
						State:             streamingpb.PartitionState_PARTITION_STATE_DROPPED,
						TombstoneTimeTick: 10,
					},
				},
			},
		},
	}, nil, nil)
	vchannel := manager.VChannels()["v1"]

	vchannel.TryFinalizeTombstone()

	assert.Equal(t, uint64(9), manager.PartitionDurableFrontier(1, 100).TimeTick())
	assert.Equal(t, uint64(9), manager.CollectionDurableFrontier(1).TimeTick())
	assert.Equal(t, uint64(9), manager.AllDurableFrontier().TimeTick())

	snapshot := vchannel.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(9), manager.PartitionDurableFrontier(1, 100).TimeTick())

	vchannel.MarkSnapshotPersisted(snapshot)
	assert.Equal(t, uint64(math.MaxUint64), manager.PartitionDurableFrontier(1, 100).TimeTick())
}

func newTestCreatePartitionMessage(vchannel string, collectionID int64, partitionID int64, timetick uint64) message.ImmutableCreatePartitionMessageV1 {
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: collectionID,
			PartitionId:  partitionID,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
	return message.MustAsImmutableCreatePartitionMessageV1(msg)
}
