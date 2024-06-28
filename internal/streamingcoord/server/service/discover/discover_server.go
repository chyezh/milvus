package discover

import (
	"context"
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/collector"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

var errClosedByUser = errors.New("closed by user")

func NewAssignmentDiscoverServer(
	balancer balancer.Balancer,
	collector *collector.Collector,
	streamServer streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer,
) *AssignmentDiscoverServer {
	ctx, cancel := context.WithCancelCause(streamServer.Context())
	return &AssignmentDiscoverServer{
		ctx:       ctx,
		cancel:    cancel,
		balancer:  balancer,
		collector: collector,
		streamServer: discoverGrpcServerHelper{
			streamServer,
		},
		logger: log.With(),
	}
}

type AssignmentDiscoverServer struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	balancer     balancer.Balancer
	collector    *collector.Collector
	streamServer discoverGrpcServerHelper
	logger       *log.MLogger
}

func (s *AssignmentDiscoverServer) Execute() error {
	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = s.recvLoop()
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv closed and all response is sent.
	return s.sendLoop()
}

// recvLoop receives the message from client.
func (s *AssignmentDiscoverServer) recvLoop() (err error) {
	defer func() {
		if err != nil {
			s.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			s.cancel(err)
		} else {
			s.logger.Info("recv arm of stream closed")
			s.cancel(errClosedByUser)
		}
	}()

	for {
		req, err := s.streamServer.Recv()
		if err == io.EOF {
			s.logger.Warn("stream closed by client unexpectedly")
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		switch req := req.Command.(type) {
		case *streamingpb.AssignmentDiscoverRequest_ReportError:
			// Trigger a collector immediately.
			s.collector.Trigger()
		case *streamingpb.AssignmentDiscoverRequest_Close:
			return nil
		default:
			s.logger.Warn("unknown command type", zap.Any("command", req))
		}
	}
}

// sendLoop sends the message to client.
func (s *AssignmentDiscoverServer) sendLoop() error {
	err := s.balancer.WatchBalanceResult(s.ctx, s.streamServer.SendFullAssignment)
	if errors.Is(err, errClosedByUser) {
		return nil
	}
	return err
}
