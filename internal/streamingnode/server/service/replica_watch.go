package service

import (
	"context"

	"github.com/cockroachdb/errors"

	replicahandler "github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func serveReplicaWatch(manager walmanager.Manager, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	err := acquireAndServeReplicaWatch(manager, rpc)
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
}

func acquireAndServeReplicaWatch(manager walmanager.Manager, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer) error {
	req, err := contextutil.GetCreateReplicaWatchStream(rpc.Context())
	if err != nil {
		return status.NewInvalidArgument("create replica watch stream request is required")
	}
	channel := types.NewPChannelInfoFromProto(req.GetPchannel())
	target, err := worknodehandler.DecodeQueryReplicaFromIncomingContext(rpc.Context())
	if err != nil {
		return err
	}
	var raw wal.WAL
	if target == nil {
		raw, err = manager.GetAvailableWAL(channel)
	} else {
		if target.Pchannel != channel.Name || target.WalTerm != channel.Term || target.ReadOnly != (channel.AccessMode == types.AccessModeRO) {
			return status.NewChannelFenced(channel.Name)
		}
		replicas, ok := manager.(wal.QueryReplicaManager)
		if !ok {
			return status.NewOnShutdownError("query replica directory is unavailable")
		}
		raw, err = replicas.GetAvailableQueryReplica(target)
	}
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(wal.ReplicaWatchProvider)
	if !ok {
		return status.NewOnShutdownError("replica directory unavailable")
	}
	stream, err := provider.AcquireReplicaWatchStream(rpc.Context())
	if err != nil {
		return err
	}
	defer stream.Close()
	return replicahandler.ServeStream(rpc, req.GetPchannel().GetName(), stream)
}
