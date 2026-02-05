package service

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	// defaultStateReportInterval is the default interval for reporting state.
	defaultStateReportInterval = 1 * time.Second
)

// AssignmentStateReporter reports assignment state to the client via gRPC stream.
type AssignmentStateReporter struct {
	stream   streamingpb.StreamingNodeManagerService_AssignWithStateReportServer
	interval time.Duration
	logger   *log.MLogger
}

// NewAssignmentStateReporter creates a new AssignmentStateReporter.
func NewAssignmentStateReporter(
	stream streamingpb.StreamingNodeManagerService_AssignWithStateReportServer,
) *AssignmentStateReporter {
	return &AssignmentStateReporter{
		stream:   stream,
		interval: defaultStateReportInterval,
		logger:   log.With(zap.String("component", "assignment-state-reporter")),
	}
}

// ReportProgress sends a progress update to the client.
func (r *AssignmentStateReporter) ReportProgress(state streamingpb.AssignmentState, progress *streamingpb.StreamRecoveringProgress) error {
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Progress{
			Progress: &streamingpb.AssignmentProgress{
				State:                    state,
				StreamRecoveringProgress: progress,
			},
		},
	}
	if err := r.stream.Send(resp); err != nil {
		r.logger.Warn("failed to send progress", zap.Error(err), zap.Stringer("state", state))
		return err
	}
	r.logger.Debug("sent progress", zap.Stringer("state", state))
	return nil
}

// ReportReady sends a ready signal to the client and returns nil.
// The caller should return nil after calling this method to close the stream normally.
func (r *AssignmentStateReporter) ReportReady() error {
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Ready{
			Ready: &streamingpb.AssignmentReady{},
		},
	}
	if err := r.stream.Send(resp); err != nil {
		r.logger.Warn("failed to send ready", zap.Error(err))
		return err
	}
	r.logger.Info("sent ready")
	return nil
}

// ReportError sends an error to the client and returns nil.
// The caller should return nil after calling this method to close the stream normally.
func (r *AssignmentStateReporter) ReportError(err error) error {
	streamingErr := status.AsStreamingError(err)
	resp := &streamingpb.AssignmentStateResponse{
		Response: &streamingpb.AssignmentStateResponse_Error{
			Error: streamingErr.AsPBError(),
		},
	}
	if sendErr := r.stream.Send(resp); sendErr != nil {
		r.logger.Warn("failed to send error", zap.Error(sendErr), zap.Error(err))
		return sendErr
	}
	r.logger.Info("sent error", zap.Error(err))
	return nil
}

// Context returns the context of the stream.
func (r *AssignmentStateReporter) Context() context.Context {
	return r.stream.Context()
}

// Interval returns the reporting interval.
func (r *AssignmentStateReporter) Interval() time.Duration {
	return r.interval
}
