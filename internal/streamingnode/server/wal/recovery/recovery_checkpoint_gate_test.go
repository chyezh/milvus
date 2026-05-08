package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestConsumeDirtySnapshotGatesCheckpointByGSegment(t *testing.T) {
	pchannel := "test_channel"
	vchannel := pchannel + "_v0"
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{Name: "test_collection"})
	require.NoError(t, err)

	rs := &recoveryStorageImpl{
		cfg:              newConfig(),
		currentClusterID: "test1",
		channel:          types.PChannelInfo{Name: pchannel},
		checkpoint: &WALCheckpoint{
			MetaCheckpoint: &Checkpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1},
		},
		vchannels:      make(map[string]*vchannelRecoveryInfo),
		metrics:        newRecoveryStorageMetrics(types.PChannelInfo{Name: pchannel}),
		segmentManager: newTestSegmentManager(t),
	}

	createCollection := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{
				10,
			},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test_collection",
			CollectionID:   1,
			PartitionIDs:   []int64{10},
			Schema:         schemaBytes,
		}).
		MustBuildMutable().
		WithTimeTick(2).
		WithLastConfirmed(rmq.NewRmqID(2)).
		IntoImmutableMessage(rmq.NewRmqID(2))
	rs.observeMessage(createCollection)

	createSegment := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   1,
			PartitionId:    10,
			SegmentId:      100,
			StorageVersion: 1,
			MaxSegmentSize: 1024,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable().
		WithTimeTick(3).
		WithLastConfirmed(rmq.NewRmqID(3)).
		IntoImmutableMessage(rmq.NewRmqID(3))
	rs.observeMessage(createSegment)

	insert := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId:       10,
					Rows:              1,
					BinarySize:        16,
					SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().
		WithTimeTick(4).
		WithLastConfirmed(rmq.NewRmqID(4)).
		IntoImmutableMessage(rmq.NewRmqID(4))
	rs.observeMessage(insert)

	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(4), snapshot.Checkpoint.MetaCheckpoint.TimeTick)
	assert.True(t, snapshot.Checkpoint.MetaCheckpoint.MessageID.EQ(rmq.NewRmqID(4)))
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	assert.Equal(t, uint64(3), snapshot.Checkpoint.DataCheckpoint.TimeTick)
	assert.True(t, snapshot.Checkpoint.DataCheckpoint.MessageID.EQ(rmq.NewRmqID(3)))
	assert.Equal(t, uint64(4), rs.observedCheckpoint.MetaCheckpoint.TimeTick)
}
