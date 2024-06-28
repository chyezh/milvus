package manager

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/layout"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

type ManagerClient interface {
	// WatchNodeChanged returns a channel that receive a node change.
	WatchNodeChanged(ctx context.Context) <-chan map[int64]*sessionutil.Session

	// CollectStatus collects status of all wal instances in all streamingnode.
	CollectAllStatus(ctx context.Context) (map[int64]*layout.NodeStatus, error)

	// Assign a wal instance for the channel on log node of given server id.
	Assign(ctx context.Context, serverID int64, channel types.PChannelInfo) error

	// Remove the wal instance for the channel on log node of given server id.
	Remove(ctx context.Context, serverID int64, channel types.PChannelInfo) error

	// Close closes the manager client.
	Close()
}
