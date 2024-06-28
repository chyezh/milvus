package channel

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/stretchr/testify/assert"
)

func TestPChannel(t *testing.T) {
	channelInfo := &streamingpb.PChannelInfo{
		Name:     "test-channel",
		Term:     1,
		ServerId: 123,
	}

	pChannel := newPChannel(channelInfo)
	assert.Equal(t, channelInfo.ServerId, pChannel.ServerID())
	assert.Equal(t, channelInfo.Name, pChannel.Name())
	assert.Equal(t, channelInfo.Term, pChannel.Term())

	// Test CopyForWrite()
	mutablePChannel := pChannel.CopyForWrite()
	assert.NotNil(t, mutablePChannel)

	// Test AssignToServerID()
	newServerID := int64(456)
	assert.True(t, mutablePChannel.AssignToServerID(newServerID))
	assert.False(t, mutablePChannel.AssignToServerID(newServerID))

	// Test ToPChannel()
	updatedChannelInfo := mutablePChannel.ToPChannel()
	assert.Equal(t, channelInfo.Name, updatedChannelInfo.Name)
	assert.Equal(t, channelInfo.Term+1, updatedChannelInfo.Term)
	assert.Equal(t, newServerID, updatedChannelInfo.ServerId)
}
