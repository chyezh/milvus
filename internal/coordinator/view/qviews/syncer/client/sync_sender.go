package client

import (
	"errors"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

var _ QueryViewServiceSyncer = (*grpcSyncer)(nil)

// newGRPCSyncer creates a sync client.
func newGRPCSyncer(
	client *queryViewServiceClientImpl,
	req *SyncOption,
) *grpcSyncer {
	syncer := &grpcSyncer{
		notifier:   syncutil.NewAsyncTaskNotifier[struct{}](),
		workNode:   req.WorkNode,
		sendCh:     make(chan *viewpb.SyncQueryViewsRequest, 10),
		receiver:   req.Receiver,
		sendExitCh: make(chan struct{}),
		recvExitCh: make(chan struct{}),
		logger:     log.With(),
	}
	go syncer.execute(client)
	return syncer
}

// grpcSyncer is the client wrapper of sync stream rpc for sync service.
type grpcSyncer struct {
	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	service    lazygrpc.Service[viewpb.QueryViewSyncServiceClient]
	workNode   qviews.WorkNode
	sendCh     chan *viewpb.SyncQueryViewsRequest
	receiver   chan<- SyncMessage
	sendExitCh chan struct{}
	recvExitCh chan struct{}
	logger     *log.MLogger
}

// SyncAtBackground creates a sync stream rpc client.
// This operation doesn't promise the sync operation is done at server-side.
// Make sure the sync operation is done by the Receiving message.
func (c *grpcSyncer) SyncAtBackground(req *viewpb.SyncQueryViewsRequest) {
	select {
	case c.sendCh <- req:
	case <-c.sendExitCh:
	}
}

// Close close the client.
func (c *grpcSyncer) Close() {
	c.notifier.Cancel()
	c.notifier.BlockUntilFinish()
}

func (c *grpcSyncer) execute(qvsc *queryViewServiceClientImpl) (err error) {
	defer func() {
		c.notifier.Finish(struct{}{})
		c.receiver <- SyncErrorMessage{workNode: c.workNode, Error: err}
	}()

	// TODO: make error mark here.

	// Get the related service and target node id by worknode.
	// maybe a streamingnode or querynode.
	streamClient, err := qvsc.createNewSyncStreamClient(c.notifier.Context(), c.workNode)
	if err != nil {
		return err
	}
	newStreamClient := syncGrpcClient{streamClient}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	sendErrCh := make(chan error, 1)
	recvErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- c.sendLoop(newStreamClient)
	}()
	go func() {
		recvErrCh <- c.recvLoop(newStreamClient)
	}()

	recvErr := <-recvErrCh
	sendErr := <-sendErrCh
	if recvErr != nil {
		return recvErr
	}
	return sendErr
}

// sendLoop sends the produce message to server.
func (c *grpcSyncer) sendLoop(syncGrpcClient syncGrpcClient) (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.logger.Info("send arm of stream closed")
		}
		close(c.sendExitCh)
		if err := syncGrpcClient.CloseSend(); err != nil {
			c.logger.Warn("failed to close send", zap.Error(err))
		}
	}()

	for {
		select {
		case <-c.recvExitCh:
			return errors.New("recv arm of stream closed")
		case req, ok := <-c.sendCh:
			if !ok {
				panic("sendCh should never be closed")
			}
			if err := syncGrpcClient.SendViews(req); err != nil {
				return err
			}
		}
	}
}

// recvLoop receives the response from server.
func (c *grpcSyncer) recvLoop(syncGrpcClient syncGrpcClient) (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		c.logger.Info("recv arm of stream closed")
		close(c.recvExitCh)
	}()

	for {
		resp, err := syncGrpcClient.Recv()
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
			c.receiver <- SyncResponseMessage{
				workNode: c.workNode,
				Response: resp.Views,
			}
		default:
			// skip message here.
			c.logger.Error("unknown response type", zap.Any("response", resp))
		}
	}
}
