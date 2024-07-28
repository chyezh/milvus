package inspector

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

type TimeTickSyncOperator interface {
	// Channel returns the pchannel info.
	Channel() types.PChannelInfo

	// SetNotifier sets the sync notifier.
	SetNotifier(notifier *SyncNotifier)

	// Sync trigger a sync operation.
	// Try to the timetick message into wal.
	Sync(ctx context.Context)
}

// TimeTickSyncInspector is the inspector to sync time tick.
type TimeTickSyncInspector interface {
	// RegisterSyncOperator registers a sync operator.
	RegisterSyncOperator(operator TimeTickSyncOperator)

	// UnregisterSyncOperator unregisters a sync operator.
	UnregisterSyncOperator(operator TimeTickSyncOperator)

	// Close closes the inspector.
	Close()
}
