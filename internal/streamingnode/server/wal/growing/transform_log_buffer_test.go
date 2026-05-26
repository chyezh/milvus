package growing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestTransformLogBufferFlushesEntriesThroughTargetTimeTick(t *testing.T) {
	buffer := newTransformLogBuffer(3)
	buffer.AppendDelete(newTestDeleteMessage(5, 100, []int64{1, 2}))
	buffer.AppendDelete(newTestDeleteMessage(6, 100, []int64{3}))
	buffer.AppendDelete(newTestDeleteMessage(12, 100, []int64{4}))

	assert.True(t, buffer.ShouldFlush())
	assert.True(t, buffer.StartFlush(10))
	pack := buffer.FlushPack(&streamingpb.VChannelMeta{
		Vchannel: "v1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
		},
	}, nil, buffer.FlushTargetTimeTick())

	require.NotNil(t, pack)
	assert.Equal(t, uint64(5), pack.FromTimeTick)
	assert.Equal(t, uint64(6), pack.ToTimeTick)
	assert.Len(t, pack.Deletes, 2)
	buffer.DiscardThrough(pack.ToTimeTick)

	assert.Equal(t, uint64(12), buffer.DataTimeTick())
	assert.False(t, buffer.ShouldFlush())
	pack = buffer.FlushPack(&streamingpb.VChannelMeta{
		Vchannel: "v1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
		},
	}, nil, 10)
	assert.Nil(t, pack)
}

func TestTransformLogBufferFlushPackUsesFixedChunkSize(t *testing.T) {
	buffer := newTransformLogBuffer(3)
	buffer.AppendDelete(newTestDeleteMessage(5, 100, []int64{1, 2}))
	buffer.AppendDelete(newTestDeleteMessage(6, 100, []int64{3}))
	buffer.AppendDelete(newTestDeleteMessage(7, 100, []int64{4}))

	assert.True(t, buffer.StartFlush(7))
	pack := buffer.FlushPack(&streamingpb.VChannelMeta{
		Vchannel: "v1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
		},
	}, nil, buffer.FlushTargetTimeTick())

	require.NotNil(t, pack)
	assert.Equal(t, uint64(5), pack.FromTimeTick)
	assert.Equal(t, uint64(6), pack.ToTimeTick)
	assert.Len(t, pack.Deletes, 2)
}

func TestTransformLogBufferFlushPackUsesAllPartitionsForVChannelL0(t *testing.T) {
	buffer := newTransformLogBuffer(10)
	buffer.AppendDelete(newTestDeleteMessage(5, 100, []int64{1}))
	buffer.AppendDelete(newTestDeleteMessage(6, 200, []int64{2}))

	assert.True(t, buffer.StartFlush(6))
	pack := buffer.FlushPack(&streamingpb.VChannelMeta{
		Vchannel: "v1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
		},
	}, nil, buffer.FlushTargetTimeTick())

	require.NotNil(t, pack)
	assert.Equal(t, common.AllPartitionsID, pack.PartitionID)
	assert.Len(t, pack.Deletes, 2)
}
