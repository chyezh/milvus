package service

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type replicaServiceImpl struct {
	streamingpb.UnimplementedStreamingNodeReplicaServiceServer
	walManager     walmanager.Manager
	bootstrapSlots chan struct{}
}

func NewReplicaService(manager walmanager.Manager) streamingpb.StreamingNodeReplicaServiceServer {
	return &replicaServiceImpl{walManager: manager, bootstrapSlots: make(chan struct{}, 32)}
}

func (hs *replicaServiceImpl) GetWALReplicaEndpoint(ctx context.Context, req *streamingpb.WALReplicaEndpointRequest) (*viewpb.StreamingWorkNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider, ok := hs.walManager.(wal.QueryReplicaEndpointProvider)
	if !ok {
		return nil, status.NewGRPCStatusFromStreamingError(status.NewOnShutdownError("WAL replica directory is unavailable")).Err()
	}
	endpoint, err := provider.QueryReplicaEndpoint(req)
	if err != nil {
		return nil, status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	}
	return endpoint, nil
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
	return serveReplicaEventReader(rpc.Context(), reader, 30*time.Second, func(_ context.Context, event walevent.Event) error {
		return rpc.Send(event.IntoProto())
	})
}

// This function takes ownership of the reader. One worker performs ordered sends.
// Only a send in progress consumes the
// timeout budget; an idle WAL is allowed to wait indefinitely for new events.
// Returning from the gRPC handler cancels transport flow control and releases
// its worker, even if the remote peer stopped reading without disconnecting.
func serveReplicaEventReader(parent context.Context, reader *walevent.Reader, sendTimeout time.Duration, send func(context.Context, walevent.Event) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	finished := make(chan error, 1)
	sending := make(chan bool)
	go func() {
		// Keep its admission slot until the actual transport worker exits,
		// including the interval between handler return and gRPC cancellation.
		defer reader.Close()
		for {
			event, err := reader.Next(ctx)
			if err != nil {
				finished <- err
				return
			}
			select {
			case sending <- true:
			case <-ctx.Done():
				return
			}
			if err := send(ctx, event); err != nil {
				finished <- err
				return
			}
			select {
			case sending <- false:
			case <-ctx.Done():
				return
			}
		}
	}()
	timer := time.NewTimer(sendTimeout)
	timer.Stop()
	defer timer.Stop()
	var deadline <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reader.SourceDone():
			return status.NewOnShutdownError("WAL replica event source closed")
		case err := <-finished:
			return err
		case active := <-sending:
			if active {
				timer.Reset(sendTimeout)
				deadline = timer.C
			} else {
				timer.Stop()
				deadline = nil
			}
		case <-deadline:
			return merr.WrapErrServiceUnavailable("WAL replica subscriber exceeded its send budget")
		}
	}
}
