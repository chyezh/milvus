package assignment

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

var _ Watcher = (*watcherImpl)(nil)

// Watcher is the interface for the channel assignment.
type Watcher interface {
	// Get gets the channel assignment.
	Get(ctx context.Context, channel string) *types.PChannelInfoAssigned

	// GetWALReplica gets the WAL replica assignment.
	GetWALReplica(ctx context.Context, channelID types.ChannelID) *types.PChannelInfoAssigned

	// Watch watches the channel assignment.
	// Block until new term is coming.
	Watch(ctx context.Context, channel string, previous *types.PChannelInfoAssigned) error

	// WatchWALReplica watches the WAL replica assignment.
	// Block until assignment changes.
	WatchWALReplica(ctx context.Context, channelID types.ChannelID, previous *types.PChannelInfoAssigned) error

	// Close stop the watcher.
	Close()
}
