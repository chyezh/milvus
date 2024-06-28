package channel

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
)

var ErrChannelNotExist = errors.New("channel not exist")

// RecoverChannelManager creates a new channel manager.
func RecoverChannelManager(ctx context.Context) (*ChannelManager, error) {
	channelInfos, err := resource.Resource().StreamingCatalog().ListPChannel(ctx)
	if err != nil {
		return nil, err
	}
	channels := make(map[string]*PChannel, len(channelInfos))
	for _, channel := range channelInfos {
		channels[channel.Name] = newPChannel(channel)
	}
	return &ChannelManager{
		channels: channels,
	}, nil
}

// ChannelManager manages the channels.
type ChannelManager struct {
	mu       sync.Mutex
	channels map[string]*PChannel
}

// UpdatePChannelAndLoad update the pchannels to servers and get latest version of pchannels.
func (cm *ChannelManager) UpdatePChannelAndLoad(ctx context.Context, pChannelToServerID map[string]int64) (map[string]*PChannel, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// modified channels.
	pChannels := make([]*streamingpb.PChannelInfo, 0, len(pChannelToServerID))
	for channelName, serverID := range pChannelToServerID {
		pchannel, ok := cm.channels[channelName]
		if !ok {
			return nil, ErrChannelNotExist
		}
		mutablePchannel := pchannel.CopyForWrite()
		if mutablePchannel.AssignToServerID(serverID) {
			pChannels = append(pChannels, mutablePchannel.ToPChannel())
		}
	}

	// update metas
	if err := resource.Resource().StreamingCatalog().SavePChannels(ctx, pChannels); err != nil {
		return nil, err
	}

	// update in-memory copy
	for _, pchannel := range pChannels {
		cm.channels[pchannel.Name] = newPChannel(pchannel)
	}
	return cm.copyChannels(), nil
}

// copyChannels returns a copy of channels.
func (cm *ChannelManager) copyChannels() map[string]*PChannel {
	channels := make(map[string]*PChannel, len(cm.channels))
	for k, v := range cm.channels {
		channels[k] = v
	}
	return channels
}
