package service

import (
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// CreateSyncServer creates a sync stream rpc server.
func CreateSyncServer(s viewpb.QueryViewSyncService_SyncServer) (*SyncServer, error) {
	ss := &SyncServer{
		syncGrpcServer: syncGrpcServer{s},
		sendCh:         make(chan *viewpb.SyncQueryViewsResponse, 10),
		recvCh:         make(chan *viewpb.SyncQueryViewsRequest, 10),
		logger:         log.With(zap.String("component", "sync_server")),
		fencedCh:       make(chan struct{}),
		sendExitCh:     make(chan struct{}),
		recvExitCh:     make(chan struct{}),
		finishedCh:     make(chan struct{}),
	}
	return ss, nil
}

type SyncServer struct {
	syncGrpcServer syncGrpcServer
	sendCh         chan *viewpb.SyncQueryViewsResponse
	recvCh         chan *viewpb.SyncQueryViewsRequest
	logger         *log.MLogger
	fencedCh       chan struct{}
	sendExitCh     chan struct{}
	recvExitCh     chan struct{}
	finishedCh     chan struct{}
}

// SyncAtBackground creates a sync stream rpc client.
// This operation doesn't promise the sync operation is done at server-side.
func (s *SyncServer) SyncAtBackground(view *viewpb.SyncQueryViewsResponse) {
	select {
	case s.sendCh <- view:
	case <-s.sendExitCh:
	case <-s.fencedCh:
	}
}

// Fence stops the sync server.
// Use Fence operation to keep the globally sync server uniquely at worknode.
func (s *SyncServer) Fence() {
	close(s.fencedCh)
	<-s.sendExitCh
}

// SyncChan returns the channel to receive from sync client.
func (s *SyncServer) SyncChan() <-chan *viewpb.SyncQueryViewsRequest {
	return s.recvCh
}

// Execute starts the producer.
func (p *SyncServer) Execute() error {
	go func() {
		_ = p.recvLoop()
	}()
	err := p.sendLoop()
	return err
}

// sendLoop sends the view to client.
func (s *SyncServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			s.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			s.logger.Info("send arm of stream closed")
		}
		close(s.sendExitCh)
	}()

	for {
		select {
		case <-s.recvExitCh:
			// When the recv arm exit, send the close response to the client.
			return s.syncGrpcServer.SendClose()
		case <-s.fencedCh:
			return s.syncGrpcServer.SendClose()
		case req := <-s.sendCh:
			if err := s.syncGrpcServer.SendViews(req); err != nil {
				return err
			}
		}
	}
}

// recvLoop receives the view from client.
func (s *SyncServer) recvLoop() (err error) {
	defer func() {
		if err != nil {
			s.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.logger.Info("recv arm of stream closed")
		close(s.recvCh)
		close(s.recvExitCh)
	}()

	for {
		resp, err := s.syncGrpcServer.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Request.(type) {
		case *viewpb.SyncRequest_Close:
			// recv io.EOF after this message.
		case *viewpb.SyncRequest_Views:
			s.recvCh <- resp.Views
		default:
			// skip message here.
			s.logger.Error("unknown response type", zap.Any("response", resp))
		}
	}
}
