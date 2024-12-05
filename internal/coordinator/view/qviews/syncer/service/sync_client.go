package service

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type SyncResponseFromNode struct {
	WorkNode qviews.WorkNode
	Response *viewpb.SyncQueryViewsResponse
}

// CreateSyncClient creates a sync stream rpc client.
func CreateSyncClient(
	ctx context.Context,
	workNode qviews.WorkNode,
	c viewpb.QueryViewSyncServiceClient,
	recvChan chan SyncResponseFromNode,
) (*SyncClient, error) {
	streamClient, err := c.Sync(ctx)
	if err != nil {
		return nil, err
	}
	sc := &SyncClient{
		syncGrpcClient: syncGrpcClient{streamClient},
		sendCh:         make(chan *viewpb.SyncQueryViewsRequest, 10),
		recvCh:         recvChan,
		logger:         log.With(zap.String("component", "sync_client")),
		sendExitCh:     make(chan struct{}),
		recvExitCh:     make(chan struct{}),
		finishedCh:     make(chan struct{}),
	}
	return sc, nil
}

// SyncClient is the client wrapper of sync stream rpc for sync service.
type SyncClient struct {
	syncGrpcClient syncGrpcClient
	workNode       qviews.WorkNode
	sendCh         chan *viewpb.SyncQueryViewsRequest
	recvCh         chan SyncResponseFromNode
	logger         *log.MLogger
	sendExitCh     chan struct{}
	recvExitCh     chan struct{}
	finishedCh     chan struct{}
}

// Execute starts the sync client.
func (c *SyncClient) Execute() {
	defer close(c.finishedCh)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.sendLoop()
	}()
	go func() {
		defer wg.Done()
		c.recvLoop()
	}()
	wg.Wait()
}

// SyncAtBackground creates a sync stream rpc client.
// This operation doesn't promise the sync operation is done at server-side.
// Make sure the sync operation is done by the Receiving message.
func (c *SyncClient) SyncAtBackground(req *viewpb.SyncQueryViewsRequest) {
	select {
	case c.sendCh <- req:
	case <-c.sendExitCh:
	}
}

// Close close the client.
func (c *SyncClient) Close() {
	// Wait for all message has been sent.
	close(c.sendCh)

	// Wait for send and recv arm to exit.
	<-c.finishedCh
}

// sendLoop sends the produce message to server.
func (c *SyncClient) sendLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.logger.Info("send arm of stream closed")
		}
		close(c.sendExitCh)
		if err := c.syncGrpcClient.CloseSend(); err != nil {
			c.logger.Warn("failed to close send", zap.Error(err))
		}
	}()

	for {
		select {
		case <-c.recvExitCh:
			return errors.New("recv arm of stream closed")
		case req, ok := <-c.sendCh:
			if !ok {
				// all message has been sent, sent close response.
				return c.syncGrpcClient.SendClose()
			}
			if err := c.syncGrpcClient.SendViews(req); err != nil {
				return err
			}
		}
	}
}

func (c *SyncClient) recvLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		c.logger.Info("recv arm of stream closed")
		close(c.recvExitCh)
	}()

	for {
		resp, err := c.syncGrpcClient.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *viewpb.SyncResponse_Close:
			// recv io.EOF after this message.
		case *viewpb.SyncResponse_Views:
			c.recvCh <- SyncResponseFromNode{
				WorkNode: c.workNode,
				Response: resp.Views,
			}
		default:
			// skip message here.
			c.logger.Error("unknown response type", zap.Any("response", resp))
		}
	}
}
