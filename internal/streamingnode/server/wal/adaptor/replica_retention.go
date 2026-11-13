package adaptor

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type replicaRuntimeRetention struct {
	catalog metastore.WALReplicaRuntimeCatalog
	runtime *viewpb.StreamingWorkNode
}

func createReplicaRuntimeRetention(ctx context.Context, opt *wal.OpenOption) (*replicaRuntimeRetention, error) {
	member := opt.Replica
	if member.GetNode().GetServerId() <= 0 || member.GetReplicaId() == 0 || member.GetReplicaRevision() == 0 ||
		member.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY || opt.RuntimeID == 0 {
		return nil, merr.WrapErrServiceInternalMsg("read-only WAL requires its reserved replica runtime identity")
	}
	catalog, ok := resource.Resource().StreamingNodeCatalog().(metastore.WALReplicaRuntimeCatalog)
	if !ok {
		return nil, merr.WrapErrServiceUnavailable("WAL replica runtime retention catalog is unavailable")
	}
	r := &replicaRuntimeRetention{catalog: catalog, runtime: &viewpb.StreamingWorkNode{
		Pchannel: opt.Channel.Name, WalTerm: opt.Channel.Term, ReadOnly: true,
		NodeId: member.Node.ServerId, RuntimeId: opt.RuntimeID,
		WalReplicaId: member.ReplicaId, WalReplicaRevision: member.ReplicaRevision,
	}}
	if err := retry.Do(ctx, func() error { return catalog.CreateWALReplicaRuntime(ctx, r.runtime) }, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
		// The create response may have been lost after committing. Fence that
		// identity even when no bootstrap request was sent.
		r.Close()
		return nil, err
	}
	return r, nil
}

func (r *replicaRuntimeRetention) install(ctx context.Context, cursor *streamingpb.WALReplicaCursor) error {
	return retry.Do(ctx, func() error {
		return r.catalog.InstallWALReplicaRuntime(ctx, r.runtime, cursor)
	}, retry.Attempts(3), retry.Sleep(10*time.Millisecond))
}

// Called only after private construction fails or all installed query/storage
// resources have drained. The canceled Open/transport context must not prevent
// the release fence from reaching the catalog. A failed cleanup stays protected
// durably and is visible to retention reconciliation.
func (r *replicaRuntimeRetention) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := retry.Do(ctx, func() error {
		return r.catalog.ReleaseWALReplicaRuntime(ctx, r.runtime)
	}, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
		mlog.Warn(ctx, "failed to release WAL replica runtime retention", mlog.String("pchannel", r.runtime.Pchannel), mlog.Int64("nodeID", r.runtime.NodeId), mlog.Uint64("runtimeID", r.runtime.RuntimeId), mlog.Err(err))
	}
}
