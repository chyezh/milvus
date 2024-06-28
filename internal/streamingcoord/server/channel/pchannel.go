package channel

import (
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

// newPChannel creates a new PChannel.
func newPChannel(channel *streamingpb.PChannelInfo) *PChannel {
	return &PChannel{
		inner: channel,
	}
}

// PChannel is the read only version of PChannelInfo, to be used in balancer,
// If you need to update PChannel, please use CopyForWrite to get mutablePChannel.
type PChannel struct {
	inner *streamingpb.PChannelInfo
}

// Name returns the name of the channel.
func (c *PChannel) Name() string {
	return c.inner.Name
}

// Term returns the term of the channel.
func (c *PChannel) Term() int64 {
	return c.inner.Term
}

// ServerID returns the server id of the channel.
func (c *PChannel) ServerID() int64 {
	return c.inner.ServerId
}

// CopyForWrite returns mutablePChannel to modify pchannel
// but didn't affect other replicas.
func (c *PChannel) CopyForWrite() *mutablePChannel {
	return &mutablePChannel{
		PChannel: &PChannel{
			inner: proto.Clone(c.inner).(*streamingpb.PChannelInfo),
		},
	}
}

// mutablePChannel is a mutable version of PChannel.
// use to update the channel info.
type mutablePChannel struct {
	*PChannel
}

// AssignToServerID assigns the channel to a new server.
func (m *mutablePChannel) AssignToServerID(serverID int64) bool {
	if m.inner.ServerId == serverID {
		return false
	}
	m.inner.Term++
	m.inner.ServerId = serverID
	return true
}

// ToPChannel returns the PChannel.
func (m *mutablePChannel) ToPChannel() *streamingpb.PChannelInfo {
	c := m.PChannel
	m.PChannel = nil
	return c.inner
}
