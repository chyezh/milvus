package manager

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"go.uber.org/zap"
)

func NewAssignmentSyncServer(
	ctx context.Context,
	syncServer streamingpb.StreamingNodeManagerService_SyncServer,
	walManager walmanager.Manager,
) *AssignmentSyncServer {
	s := &AssignmentSyncServer{
		ctx:                ctx,
		syncServer:         newAssignmentSyncGRPCServerHelper(syncServer),
		walManager:         walManager,
		assignmentReportCh: make(chan walmanager.SyncResponse),
		closeCh:            make(chan struct{}),
	}
	s.SetLogger(resource.Resource().Logger().With(log.FieldComponent("assignment-sync-server")))
	return s
}

// AssignmentSyncServer is a Sync Server for wal assignment.
type AssignmentSyncServer struct {
	log.Binder
	ctx                context.Context
	syncServer         *assignmentSyncGRPCServerHelper
	walManager         walmanager.Manager
	assignmentReportCh chan walmanager.SyncResponse
	closeCh            chan struct{}
}

// Execute executes the consumer.
func (c *AssignmentSyncServer) Execute() error {
	// recv loop will be blocked until the stream is closed.
	// 1. close by client.
	// 2. close by server context cancel by return of outside Execute.
	go c.recvLoop()

	// Start a send loop on current goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv close signal.
	// 3. scanner is quit with expected error.
	err := c.sendLoop()
	return err
}

// sendLoop sends assignment report to client.
func (s *AssignmentSyncServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			s.Logger().Warn("send arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.Logger().Info("send arm of stream closed")
	}()

	for {
		select {
		case <-s.ctx.Done():
			s.Logger().Info("sync server is going to stop at server side, streaming node may go to stop")
			return nil
		case <-s.syncServer.Context().Done():
			return errors.Wrap(s.syncServer.Context().Err(), "stream closed")
		case assignmentResp, ok := <-s.assignmentReportCh:
			if !ok {
				panic("unreachable: assignment report channel closed")
			}
			if err := s.syncServer.SendAssignResponse(assignmentResp); err != nil {
				return err
			}
		case <-s.closeCh:
			s.Logger().Info("close channel notified")
			if err := s.syncServer.SendClosed(); err != nil {
				s.Logger().Warn("send close response failed", zap.Error(err))
				return status.NewInner("send close response failed", err.Error())
			}
			return nil
		}
	}
}

// recvLoop receives assignment request from client.
func (s *AssignmentSyncServer) recvLoop() (err error) {
	defer func() {
		close(s.closeCh)
		if err != nil {
			s.Logger().Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.Logger().Info("recv arm of stream closed")
	}()

	for {
		req, err := s.syncServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *streamingpb.StreamingNodeManagerSyncRequest_Assignment:
			reporter := newReporter(s.closeCh, s.assignmentReportCh)
			s.walManager.Sync(newSyncRequestFromProto(req.Assignment, reporter))
		case *streamingpb.StreamingNodeManagerSyncRequest_Close:
			s.Logger().Info("recv arm of stream start to close...")
		default:
			s.Logger().Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// newSyncRequestFromProto converts protobuf SyncAssignmentRequest to SyncRequest.
func newSyncRequestFromProto(req *streamingpb.SyncAssignmentRequest, reporter walmanager.Reporter) walmanager.SyncRequest {
	syncReq := walmanager.SyncRequest{
		Assign:   make([]types.PChannelInfo, 0, len(req.Assigned)),
		Unassign: make([]types.PChannelInfo, 0, len(req.Unassigned)),
		Reporter: reporter,
	}
	for _, assigned := range req.Assigned {
		syncReq.Assign = append(syncReq.Assign, types.NewPChannelInfoFromProto(assigned))
	}
	for _, unassigned := range req.Unassigned {
		syncReq.Unassign = append(syncReq.Assign, types.NewPChannelInfoFromProto(unassigned))
	}
	return syncReq
}
