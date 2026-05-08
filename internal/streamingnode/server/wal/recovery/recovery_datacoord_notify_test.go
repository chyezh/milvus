package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestBuildSaveBinlogPathRequests(t *testing.T) {
	snapshot := &RecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {
				CollectionId:       10,
				PartitionId:        20,
				SegmentId:          1,
				Vchannel:           "ch-1",
				State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				StorageVersion:     2,
				CheckpointTimeTick: 1000,
				Stat: &streamingpb.SegmentAssignmentStat{
					ModifiedRows: 7,
				},
				PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
					L1: &streamingpb.L1SegmentPersistedStorage{
						ManifestPath: "manifest/1",
						MergedStatsBinlog: &datapb.FieldBinlog{
							FieldID: 101,
							Binlogs: []*datapb.Binlog{{
								LogPath:    "stats/compound",
								EntriesNum: 7,
							}},
						},
						Binlogs: []*streamingpb.L1SegmentBinLogs{{
							FieldBinlog: []*datapb.FieldBinlog{{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{{
									LogPath:    "binlog/1",
									EntriesNum: 7,
								}},
							}},
							StatsBinlog: []*datapb.FieldBinlog{{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{{
									LogPath:    "stats/1",
									EntriesNum: 7,
								}},
							}},
						}},
					},
				},
			},
			2: {
				CollectionId:       10,
				PartitionId:        21,
				SegmentId:          2,
				Vchannel:           "ch-1",
				State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
				CheckpointTimeTick: 2000,
				Stat: &streamingpb.SegmentAssignmentStat{
					ModifiedRows: 3,
				},
				PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
					L0: &streamingpb.L0SegmentPersistedStorage{
						DeltaBinlog: []*datapb.FieldBinlog{{
							FieldID: 102,
							Binlogs: []*datapb.Binlog{{
								LogPath:    "delta/1",
								EntriesNum: 3,
							}},
						}},
					},
				},
			},
			3: {
				CollectionId:       10,
				PartitionId:        22,
				SegmentId:          3,
				Vchannel:           "ch-1",
				State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				CheckpointTimeTick: 3000,
				Stat:               &streamingpb.SegmentAssignmentStat{},
				PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
					L1: &streamingpb.L1SegmentPersistedStorage{},
				},
			},
		},
	}

	requests := buildSaveBinlogPathRequests(snapshot)
	assert.Len(t, requests, 2)
	bySegment := map[int64]*datapb.SaveBinlogPathsRequest{}
	for _, req := range requests {
		bySegment[req.GetSegmentID()] = req
	}

	l1Req := bySegment[1]
	assert.NotNil(t, l1Req)
	assert.Equal(t, datapb.SegmentLevel_L1, l1Req.GetSegLevel())
	assert.Equal(t, "manifest/1", l1Req.GetManifestPath())
	assert.True(t, l1Req.GetFlushed())
	assert.True(t, l1Req.GetWithFullBinlogs())
	assert.Len(t, l1Req.GetField2BinlogPaths(), 1)
	assert.Len(t, l1Req.GetField2StatslogPaths(), 2)
	assert.Equal(t, int64(7), l1Req.GetCheckPoints()[0].GetNumOfRows())
	assert.Equal(t, uint64(1000), l1Req.GetCheckPoints()[0].GetPosition().GetTimestamp())

	l0Req := bySegment[2]
	assert.NotNil(t, l0Req)
	assert.Equal(t, datapb.SegmentLevel_L0, l0Req.GetSegLevel())
	assert.True(t, l0Req.GetFlushed())
	assert.Len(t, l0Req.GetDeltalogs(), 1)
	assert.Equal(t, int64(3), l0Req.GetCheckPoints()[0].GetNumOfRows())
}

func TestPersistDirtySnapshotNotifiesDataCoord(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveSegmentAssignments(mock.Anything, "test-pchannel", mock.Anything).Return(nil)
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, "test-pchannel", mock.Anything).Return(nil)

	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
			assert.Equal(t, int64(1), req.GetSegmentID())
			assert.Equal(t, int64(10), req.GetCollectionID())
			assert.Equal(t, int64(20), req.GetPartitionID())
			assert.Equal(t, "ch-1", req.GetChannel())
			assert.True(t, req.GetWithFullBinlogs())
			assert.Len(t, req.GetField2BinlogPaths(), 1)
			assert.Equal(t, int64(7), req.GetCheckPoints()[0].GetNumOfRows())
			return merr.Success(), nil
		})
	future := syncutil.NewFuture[internaltypes.MixCoordClient]()
	future.Set(mixCoord)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog), resource.OptMixCoordClient(future))

	rs := &recoveryStorageImpl{
		cfg:       newConfig(),
		channel:   types.PChannelInfo{Name: "test-pchannel"},
		vchannels: map[string]*vchannelRecoveryInfo{},
		checkpoint: &WALCheckpoint{
			MetaCheckpoint: &Checkpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 10},
		},
		pendingPersistSnapshot: &RecoverySnapshot{
			SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
				1: {
					CollectionId:       10,
					PartitionId:        20,
					SegmentId:          1,
					Vchannel:           "ch-1",
					State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					CheckpointTimeTick: 1000,
					Stat: &streamingpb.SegmentAssignmentStat{
						ModifiedRows: 7,
					},
					PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
						L1: &streamingpb.L1SegmentPersistedStorage{
							Binlogs: []*streamingpb.L1SegmentBinLogs{{
								FieldBinlog: []*datapb.FieldBinlog{{
									FieldID: 101,
									Binlogs: []*datapb.Binlog{{
										LogPath:    "binlog/1",
										EntriesNum: 7,
									}},
								}},
							}},
						},
					},
				},
			},
			Checkpoint: &utility.WALCheckpoint{
				MetaCheckpoint: &Checkpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 10},
			},
		},
		metrics:        newRecoveryStorageMetrics(types.PChannelInfo{Name: "test-pchannel"}),
		segmentManager: newTestSegmentManager(t),
	}

	err := rs.persistDirtySnapshot(context.Background(), zap.InfoLevel)
	assert.NoError(t, err)
	assert.Nil(t, rs.pendingPersistSnapshot)
}
