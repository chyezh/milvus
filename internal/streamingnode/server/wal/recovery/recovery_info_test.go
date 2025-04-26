package recovery

import (
	"context"
	"os"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMain(m *testing.M) {
	// Initialize the paramtable package
	paramtable.Init()

	// Run the tests
	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}
}

func TestInitRecoveryInfoFromMeta(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return([]*streamingpb.SegmentAssignmentMeta{}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).Return([]*streamingpb.VChannelMeta{}, nil)

	snCatalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(
		&streamingpb.WALCheckpoint{
			MessageId: &messagespb.MessageID{
				Id: rmq.NewRmqID(1).Marshal(),
			},
			TimeTick:      1,
			RecoveryMagic: 0,
		}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))
	walName := "rocksmq"
	channel := types.PChannelInfo{Name: "test_channel"}

	lastConfirmed := message.CreateTestTimeTickSyncMessage(t, 1, 1, rmq.NewRmqID(1))
	recoverInfo, err := recoverRecoveryInfoFromMeta(context.Background(), walName, channel, lastConfirmed.IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.NotNil(t, recoverInfo)
	assert.NoError(t, err)
	assert.NotNil(t, recoverInfo.Checkpoint)
	assert.Equal(t, int64(recoveryMagicStreamingInitialized), recoverInfo.Checkpoint.Magic)
	assert.True(t, recoverInfo.Checkpoint.MessageID.EQ(rmq.NewRmqID(1)))
}

func TestInitRecoveryInfoFromCoord(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return([]*streamingpb.SegmentAssignmentMeta{}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).Return([]*streamingpb.VChannelMeta{}, nil)
	snCatalog.EXPECT().SaveVChannels(mock.Anything, mock.Anything, mock.Anything).Return([]*streamingpb.VChannelMeta{}, nil)
	snCatalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(nil, nil)

	c := mocks.NewMockMixCoordClient(t)
	c.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collections: []*rootcoordpb.CollectionInfoOnPChannel{
			{
				CollectionId: 1,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 1},
					{PartitionId: 2},
				},
				Vchannel: "v1",
			},
			{
				CollectionId: 2,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 3},
					{PartitionId: 4},
				},
				Vchannel: "v2",
			},
		},
	}, nil)

	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))
}
