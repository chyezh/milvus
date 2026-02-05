package manager

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2
)

// assignHandler handles WAL assignment with automatic reconnection.
type assignHandler struct {
	client   streamingpb.StreamingNodeManagerServiceClient
	pchannel types.PChannelInfoAssigned
	logger   *log.MLogger
}

// newAssignHandler creates a new assign handler.
func newAssignHandler(
	client streamingpb.StreamingNodeManagerServiceClient,
	pchannel types.PChannelInfoAssigned,
) *assignHandler {
	return &assignHandler{
		client:   client,
		pchannel: pchannel,
		logger: log.With(
			zap.String("component", "assign-handler"),
			zap.String("channel", pchannel.Channel.Name),
			zap.Int64("serverID", pchannel.Node.ServerID),
		),
	}
}

// Execute executes the assignment with automatic reconnection.
// Returns nil on success, or error on failure.
func (h *assignHandler) Execute(ctx context.Context) error {
	backoff := initialBackoff

	for {
		err := h.executeOnce(ctx)
		if err == nil {
			return nil // Success
		}

		// Check if we should stop retrying
		if !h.shouldRetry(err) {
			return err
		}

		// Check context before sleep
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		h.logger.Info("assignment stream disconnected, will reconnect",
			zap.Error(err),
			zap.Duration("backoff", backoff),
		)

		// Sleep with exponential backoff
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		// Increase backoff for next iteration
		backoff = time.Duration(float64(backoff) * backoffFactor)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// executeOnce executes a single assignment attempt.
func (h *assignHandler) executeOnce(ctx context.Context) error {
	// Add server ID to context for load balancer
	ctx = contextutil.WithPickServerID(ctx, h.pchannel.Node.ServerID)

	// Create the stream
	stream, err := h.client.AssignWithStateReport(ctx, &streamingpb.StreamingNodeManagerAssignRequest{
		Pchannel: types.NewProtoFromPChannelInfo(h.pchannel.Channel),
	})
	if err != nil {
		return errors.Wrap(err, "failed to create assign stream")
	}

	// Process stream messages
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			// Stream closed without Ready/Error - treat as abnormal disconnect
			return errors.New("stream closed unexpectedly without Ready or Error")
		}
		if err != nil {
			return errors.Wrap(err, "failed to receive from stream")
		}

		switch r := resp.Response.(type) {
		case *streamingpb.AssignmentStateResponse_Progress:
			h.logger.Debug("received progress",
				zap.Stringer("state", r.Progress.State),
			)
			// Continue receiving

		case *streamingpb.AssignmentStateResponse_Ready:
			h.logger.Info("assignment completed successfully")
			return nil

		case *streamingpb.AssignmentStateResponse_Error:
			streamingErr := status.New(r.Error.Code, r.Error.Cause)
			h.logger.Warn("assignment failed with error", zap.Error(streamingErr))
			return streamingErr
		}
	}
}

// shouldRetry determines if the error is retryable.
func (h *assignHandler) shouldRetry(err error) bool {
	// Don't retry on context cancellation
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Don't retry on StreamingError (business logic error from server)
	var streamingErr *status.StreamingError
	if errors.As(err, &streamingErr) {
		return false
	}

	// Check gRPC status
	st, ok := grpcstatus.FromError(err)
	if ok {
		// Don't retry on Unimplemented - fallback to legacy
		if st.Code() == codes.Unimplemented {
			return false
		}
	}

	// Retry on other errors (network issues, stream disconnects, etc.)
	return true
}

// isUnimplemented checks if the error indicates the RPC is not implemented.
func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	st, ok := grpcstatus.FromError(errors.Cause(err))
	if !ok {
		// Try unwrapping
		var unwrapped error = err
		for unwrapped != nil {
			st, ok = grpcstatus.FromError(unwrapped)
			if ok {
				break
			}
			unwrapped = errors.Unwrap(unwrapped)
		}
	}
	return ok && st.Code() == codes.Unimplemented
}
