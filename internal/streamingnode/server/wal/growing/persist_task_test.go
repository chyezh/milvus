package growing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

func TestGrowingPersistTaskSavesCurrentDirtyDataMeta(t *testing.T) {
	catalog := &testRecoveryCatalog{}
	writer := &testGrowingSegmentPackWriter{
		result: &FlushResult{
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: 5, ToTimeTick: 5}},
			},
		},
	}
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
	}, map[int64]*streamingpb.SegmentAssignmentMeta{
		10: {
			CollectionId:           1,
			PartitionId:            2,
			SegmentId:              10,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     4,
			DataCheckpointTimeTick: 4,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 4},
		},
	}, &testSegmentLifecycleWriter{}, WithPackWriter(writer), WithRecoveryCatalog("test-channel", catalog))
	segment := manager.Segments()[10]
	msg := newTestInsertMessage(5, 10, 2, 7, 70)
	segment.ObserveInsertMessageV1(context.Background(), msg, msg.Header().GetPartitions()[0])

	persistTask := manager.NewPersistTask(
		"test-channel",
		catalog,
		nil,
		scheduler.AlwaysReady{},
		nil,
	)
	require.NotNil(t, persistTask)

	require.NoError(t, segment.FlushBuffer(context.Background()))
	assert.Empty(t, catalog.savedSegments)
	assert.Equal(t, uint64(5), segment.Meta().GetDataCheckpointTimeTick())

	require.NoError(t, persistTask.Run(context.Background()))
	require.Len(t, catalog.savedSegments, 1)
	assert.Equal(t, uint64(5), catalog.savedSegments[0][10].GetDataCheckpointTimeTick())
}
