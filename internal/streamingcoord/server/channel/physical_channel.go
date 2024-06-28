package channel

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

var _ PhysicalChannel = (*physicalChannelImpl)(nil)

type PhysicalChannel interface {
	// Name returns the name of the channel.
	Name() string

	// Term returns the term of the channel.
	Term() int64

	// PChannelInfo returns the info of the channel.
	Info() *streamingpb.PChannelInfo

	// IsDrop returns whether the channel is dropped.
	IsDrop() bool

	// Assign allocates a new term for the channel, update assigned serverID and return the term.
	Assign(ctx context.Context, serverID int64) (*streamingpb.PChannelInfo, error)

	// Drop the channel.
	Drop(ctx context.Context) error
}

// NewPhysicalChannel creates a new PhysicalChannel.
func NewPhysicalChannel(catalog metastore.StreamingCoordCataLog, info *streamingpb.PChannelInfo) PhysicalChannel {
	return &physicalChannelImpl{
		catalog: catalog,
		mu:      &sync.RWMutex{},
		info:    info,
		dropped: false,
	}
}

type physicalChannelImpl struct {
	catalog metastore.StreamingCoordCataLog
	mu      *sync.RWMutex
	info    *streamingpb.PChannelInfo
	dropped bool
}

// Name returns the name of the channel.
func (pc *physicalChannelImpl) Name() string {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.info.Name
}

// Term returns the term of the channel.
func (pc *physicalChannelImpl) Term() int64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.info.Term
}

// IsDrop returns whether the channel is dropped.
func (pc *physicalChannelImpl) IsDrop() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.dropped
}

// Info returns the copy of the channel info.
func (pc *physicalChannelImpl) Info() *streamingpb.PChannelInfo {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return proto.Clone(pc.info).(*streamingpb.PChannelInfo)
}

// Assign allocates a new term for the channel, update assigned serverID and return the term.
func (pc *physicalChannelImpl) Assign(ctx context.Context, serverID int64) (*streamingpb.PChannelInfo, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.dropped {
		return nil, ErrNotExists
	}

	// Allocate new term.
	// Just increase the term by 1 and save.
	newChannel := proto.Clone(pc.info).(*streamingpb.PChannelInfo)
	newChannel.Term++
	newChannel.ServerId = serverID
	if err := pc.catalog.SavePChannel(ctx, newChannel); err != nil {
		return nil, err
	}
	pc.info = newChannel
	return proto.Clone(newChannel).(*streamingpb.PChannelInfo), nil
}

// Drop the channel.
func (pc *physicalChannelImpl) Drop(ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if err := pc.catalog.DropPChannel(ctx, pc.info.Name); err != nil {
		return err
	}
	pc.dropped = true
	return nil
}
