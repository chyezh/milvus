package service

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

const (
	// defaultStateReportInterval is the default interval for reporting state.
	defaultStateReportInterval = 1 * time.Second
)

var _ ManagerService = (*managerServiceImpl)(nil)

// NewManagerService create a streamingnode manager service.
func NewManagerService(m walmanager.Manager) ManagerService {
	return &managerServiceImpl{
		walManager: m,
	}
}

type ManagerService interface {
	streamingpb.StreamingNodeManagerServiceServer
}

// managerServiceImpl implements ManagerService.
// managerServiceImpl is just a rpc level to handle incoming grpc.
// all manager logic should be done in wal.Manager.
type managerServiceImpl struct {
	streamingpb.UnimplementedStreamingNodeManagerServiceServer
	walManager walmanager.Manager
}

// Remove removes the wal instance for the channel.
// After remove returns, the wal instance is removed and all underlying read write operation should be rejected.
func (ms *managerServiceImpl) Remove(ctx context.Context, req *streamingpb.StreamingNodeManagerRemoveRequest) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
	pchannelInfo := types.NewPChannelInfoFromProto(req.GetPchannel())
	if err := ms.walManager.Remove(ctx, pchannelInfo); err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerRemoveResponse{}, nil
}

// CollectStatus collects the status of all wal instances in these streamingnode.
func (ms *managerServiceImpl) CollectStatus(ctx context.Context, req *streamingpb.StreamingNodeManagerCollectStatusRequest) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
	metrics, err := ms.walManager.Metrics()
	if err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerCollectStatusResponse{
		Metrics: types.NewProtoFromStreamingNodeMetrics(*metrics),
	}, nil
}

// AssignWithStateReport assigns a wal instance for the channel on this Manager with state reporting.
// Unlike Assign, this method uses server streaming to report progress during recovery.
func (ms *managerServiceImpl) AssignWithStateReport(
	req *streamingpb.StreamingNodeManagerAssignRequest,
	stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer,
) error {
	pchannelInfo := types.NewPChannelInfoFromProto(req.GetPchannel())
	logger := log.With(zap.String("channel", pchannelInfo.Name), zap.Int64("term", pchannelInfo.Term))

	// Start async open - returns a StateProgressStore immediately
	stateStore := ms.walManager.AsyncOpen(stream.Context(), pchannelInfo)

	// Watch the state store and report progress to the gRPC stream
	return watchAndReportState(stream.Context(), stateStore, stream, logger)
}

// watchAndReportState watches the state store and reports progress to the gRPC stream.
// It blocks until the state becomes terminal (Ready or Error) or the context is canceled.
func watchAndReportState(
	ctx context.Context,
	store *utility.StateProgressStore,
	stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer,
	logger *log.MLogger,
) error {
	ticker := time.NewTicker(defaultStateReportInterval)
	defer ticker.Stop()

	progress := store.Get()
	lastVersion := progress.Version

	// Send initial state
	if err := sendProgress(stream, progress); err != nil {
		logger.Warn("failed to send initial progress", zap.Error(err))
		return nil // Stream error, close gracefully
	}

	for {
		// Check if we've reached a terminal state
		if progress.Ready {
			_ = sendReady(stream)
			logger.Info("assignment ready")
			return nil
		}
		if progress.Error != nil {
			_ = sendError(stream, progress.Error)
			logger.Info("assignment error", zap.Error(progress.Error))
			return nil
		}

		// Wait for state change or timeout
		select {
		case <-ctx.Done():
			// Context canceled, close stream
			logger.Info("context canceled during state watching")
			return nil
		case <-ticker.C:
			// Periodic check - get current state and send if changed
			progress = store.Get()
			if progress.Version != lastVersion {
				lastVersion = progress.Version
				if err := sendProgress(stream, progress); err != nil {
					logger.Warn("failed to send progress", zap.Error(err))
					return nil
				}
			}
		}
	}
}

// sendProgress sends a progress update to the gRPC stream.
func sendProgress(stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer, progress utility.StateProgress) error {
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Progress{
			Progress: &streamingpb.AssignmentProgress{
				State:                    progress.State,
				StreamRecoveringProgress: progress.GetProtoProgress(),
			},
		},
	}
	return stream.Send(resp)
}

// sendReady sends a ready signal to the gRPC stream.
func sendReady(stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer) error {
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Ready{
			Ready: &streamingpb.AssignmentReady{},
		},
	}
	return stream.Send(resp)
}

// sendError sends an error to the gRPC stream.
func sendError(stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer, err error) error {
	streamingErr := status.AsStreamingError(err)
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Error{
			Error: streamingErr.AsPBError(),
		},
	}
	return stream.Send(resp)
}
