package service

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type replicaServiceImpl struct {
	streamingpb.UnimplementedStreamingNodeReplicaServiceServer
	walManager walmanager.Manager
}

func NewReplicaService(manager walmanager.Manager) streamingpb.StreamingNodeReplicaServiceServer {
	return &replicaServiceImpl{walManager: manager}
}

func (hs *replicaServiceImpl) ReconcileWALReplicas(ctx context.Context, req *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error) {
	manager, ok := hs.walManager.(walmanager.ReplicaPlanManager)
	if !ok {
		return nil, status.NewGRPCStatusFromStreamingError(status.NewOnShutdownError("WAL replica plan manager is unavailable")).Err()
	}
	response, err := manager.ReconcileWALReplicas(ctx, req)
	if err != nil {
		return nil, status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
	return response, nil
}

func (hs *replicaServiceImpl) SubscribeWALEvents(req *streamingpb.WALReplicaSubscribeRequest, rpc streamingpb.StreamingNodeReplicaService_SubscribeWALEventsServer) error {
	err := hs.serveWALEvents(req, rpc)
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	var reset *walevent.ResetError
	if errors.As(err, &reset) {
		// Resume failure belongs to the transport handshake, never to the
		// replicated business stream. Include the current primary's cut.
		st := status.NewGRPCStatusFromStreamingError(status.NewInvalidRequestSeq("WAL event history requires a metadata baseline"))
		st, detailErr := st.WithDetails(reset.Applied.IntoProto())
		if detailErr != nil {
			return detailErr
		}
		return st.Err()
	}
	return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
}

func (hs *replicaServiceImpl) serveWALEvents(req *streamingpb.WALReplicaSubscribeRequest, rpc streamingpb.StreamingNodeReplicaService_SubscribeWALEventsServer) error {
	if req.GetPchannel().GetName() == "" || req.GetPchannel().GetTerm() <= 0 || req.GetAfter().GetTerm() <= 0 || req.GetPchannel().GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
		return status.NewInvalidArgument("WAL replica subscription requires a channel and cursor")
	}
	raw, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.Pchannel))
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(raw).(wal.ReplicaEventProvider)
	if !ok {
		return status.NewOnShutdownError("WAL replica event source is unavailable")
	}
	reader, err := provider.SubscribeWALEvents(rpc.Context(), walevent.CursorFromProto(req.After))
	if err != nil {
		return err
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(rpc.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		for {
			event, err := reader.Next(ctx)
			if err != nil {
				finished <- err
				return
			}
			if err := rpc.Send(event.IntoProto()); err != nil {
				finished <- err
				return
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-reader.SourceDone():
		return status.NewOnShutdownError("WAL replica event source closed")
	case err := <-finished:
		return err
	}
}
