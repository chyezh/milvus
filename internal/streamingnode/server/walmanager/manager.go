package walmanager

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ Manager = (*managerImpl)(nil)

type SyncRequest struct {
	States   []types.PChannelAssignState
	Reporter Reporter
}

type AssignWithError struct {
	Channel types.PChannelAssignState
	Err     error
}

type SyncResponse struct {
	States []AssignWithError
}

// Reporter is the interface for reporting the wal status.
type Reporter interface {
	Report(resp SyncResponse)
}

// Manager is the interface for managing the wal instances.
type Manager interface {
	// Sync sync up the wal status from coordinator.
	// The sync operation result will be reported by EventChan.
	// Only one replica of a channel can only be assigned to one wal manager.
	// E.g. If there's a ro channel in manager, then the sync operation unassign these ro channel, then assign a rw channel,
	// The underlying state will be shared between the ro channel and the rw channel, then th ro channel is gone, the rw channel will serve.
	// If the SyncRequest is try to assign two replica on the same wal manager，the operation will report an error.
	Sync(req SyncRequest)

	// GetAvailableWAL returns a available wal instance for the channel.
	// Return nil if the wal instance is not found.
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)

	// GetAllAvailableWALInfo returns all available channel info.
	GetAllAvailableChannels() ([]types.PChannelInfo, error)

	// Close these manager and release all managed WAL.
	Close()
}
