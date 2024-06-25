package service

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/channel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// NewChannelService opens a meta instance and create channel service.
func NewChannelService(
	channelMeta channel.Meta,
	balancer balancer.Balancer,
) ChannelService {
	return &channelServiceImpl{
		channelMeta: channelMeta,
		balancer:    balancer,
	}
}

var _ ChannelService = (*channelServiceImpl)(nil)

type ChannelService interface {
	streamingpb.StreamingCoordChannelServiceServer
}

type channelServiceImpl struct {
	mu          sync.Mutex
	channelMeta channel.Meta
	balancer    balancer.Balancer
}

func (cs *channelServiceImpl) CreatePChannel(ctx context.Context, request *streamingpb.CreatePChannelRequest) (*streamingpb.CreatePChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Create a pchannel in meta.
	err := cs.channelMeta.CreatePChannel(ctx, request.Pchannel)
	if errors.Is(err, channel.ErrExists) {
		return nil, status.NewChannelExist("create pchannel %s", request.Pchannel)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// Add new pChannel to balancer.
	pChannel := cs.channelMeta.GetPChannel(ctx, request.Pchannel)
	if pChannel == nil {
		log.Warn("unexpected pChannel not found", zap.String("pChannelName", request.Pchannel))
		return nil, status.NewInner("channel meta error: pChannel not found")
	}
	cs.balancer.UpdateChannel(map[string]channel.PhysicalChannel{
		pChannel.Name(): pChannel,
	})

	return &streamingpb.CreatePChannelResponse{}, nil
}

func (cs *channelServiceImpl) RemovePChannel(ctx context.Context, request *streamingpb.RemovePChannelRequest) (*streamingpb.RemovePChannelResponse, error) {
	// All channel operations should be protected by a lock to avoid in-consistency between balancer and meta.
	cs.mu.Lock()
	defer cs.mu.Unlock()
	err := cs.channelMeta.RemovePChannel(ctx, request.Pchannel)
	if errors.Is(err, channel.ErrNotExists) {
		return nil, status.NewChannelNotExist("remove pchannel %s", request.Pchannel)
	} else if err != nil {
		return nil, status.NewInner("meta error:", err)
	}

	// remove pChannel from balancer.
	cs.balancer.UpdateChannel(map[string]channel.PhysicalChannel{
		request.Pchannel: nil,
	})
	return &streamingpb.RemovePChannelResponse{}, nil
}
