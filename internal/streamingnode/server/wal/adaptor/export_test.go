package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// RecoveryStorageForTest lets external component tests observe the real store
// without patching the recovery constructor while it starts background work.
func RecoveryStorageForTest(w wal.WAL) recovery.RecoveryStorage {
	return wal.Unwrap(w).(*walAdaptorImpl).param.RecoveryStorage
}

func NewReplicaFollowerForTest(ctx context.Context, cfg recovery.ReadOnlyConfig, snapshot *streamingpb.WALReplicaSnapshot, replay recovery.ReplicaReplay, client streamingpb.StreamingNodeReplicaServiceClient) (wal.WAL, *recovery.ReadOnlyRecoveryStorage, error) {
	r, err := newReadOnlyReplica(ctx, cfg, snapshot, replay, nil, nil)
	if err != nil {
		return nil, nil, err
	}
	r.startFollowing(client, cfg.Channel)
	return r, r.storage, nil
}
