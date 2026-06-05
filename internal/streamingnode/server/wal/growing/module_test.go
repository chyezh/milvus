package growing

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestGrowingManagerReturnsNoBarrierForIrrelevantMessage(t *testing.T) {
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(10).IntoImmutableMessage(nil)

	manager := NewManager(nil, nil, nil)
	result := manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)

	manager.metaAndData = true
	result = manager.ObserveMessage(context.Background(), msg)
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
}

func TestGrowingManagerTruncateCollectionAdvancesVChannelMeta(t *testing.T) {
	manager := NewManager(map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
			CheckpointTimeTick: 1,
			LatestDataVersion:  &viewpb.DataVersion{},
			GrowingSegmentMode: streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		},
	}, nil, nil)
	mutableMsg := message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))

	result := manager.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Meta)
	vchannel := manager.vChannels()["v1"].AssignmentMeta()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, vchannel.GetState())
	assert.Equal(t, uint64(10), vchannel.GetCheckpointTimeTick())
}
