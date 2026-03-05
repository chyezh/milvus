package coordview

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// resumableSyncer manages a single gRPC bidirectional stream to a work node.
// It continuously tries to maintain the stream with exponential backoff,
// and pushes/re-pushes outstanding views.
//
// Modeled after producer_resuming.go.
type resumableSyncer struct {
	node        qviews.WorkNode
	client      NodeClient
	outstanding *outstanding
	sendCh      chan []*viewpb.QueryViewOfShard

	// Stream swap using ContextCond (producer_resuming pattern).
	cond   *syncutil.ContextCond
	stream viewpb.ViewSyncService_SyncQueryViewClient
	err    error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResumableSyncer(
	ctx context.Context,
	node qviews.WorkNode,
	client NodeClient,
	outstanding *outstanding,
	sendBufferSize int,
) *resumableSyncer {
	ctx, cancel := context.WithCancel(ctx)
	rs := &resumableSyncer{
		node:        node,
		client:      client,
		outstanding: outstanding,
		sendCh:      make(chan []*viewpb.QueryViewOfShard, sendBufferSize),
		cond:        syncutil.NewContextCond(&sync.Mutex{}),
		ctx:         ctx,
		cancel:      cancel,
	}
	rs.wg.Add(2)
	go rs.streamCreatorLoop()
	go rs.sendRecvLoop()
	return rs
}

// Enqueue enqueues protos for sending to the node.
func (rs *resumableSyncer) Enqueue(protos []*viewpb.QueryViewOfShard) {
	select {
	case rs.sendCh <- protos:
	case <-rs.ctx.Done():
	}
}

// Close stops the ResumableSyncer and waits for goroutines to exit.
func (rs *resumableSyncer) Close() {
	rs.cancel()
	// Broadcast to wake up any goroutine waiting on cond.
	rs.swapStream(nil, context.Canceled)
	rs.wg.Wait()
}

// streamCreatorLoop continuously creates gRPC streams with exponential backoff.
func (rs *resumableSyncer) streamCreatorLoop() {
	defer rs.wg.Done()

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0 // retry forever until closed
	bo.Reset()

	for rs.ctx.Err() == nil {
		stream, err := rs.client.OpenSyncStream(rs.ctx, rs.node)
		if err != nil {
			if rs.ctx.Err() != nil {
				return
			}
			log.Warn("ResumableSyncer: failed to open stream",
				zap.String("node", rs.node.String()), zap.Error(err))

			nextBackoff := bo.NextBackOff()
			select {
			case <-time.After(nextBackoff):
			case <-rs.ctx.Done():
				return
			}
			continue
		}

		bo.Reset()
		rs.swapStream(stream, nil)

		// Wait until stream is broken (recv loop will swap it to nil).
		rs.waitForStreamBroken()
	}
}

// sendRecvLoop waits for a stream to become available, then runs send and recv loops.
func (rs *resumableSyncer) sendRecvLoop() {
	defer rs.wg.Done()

	for rs.ctx.Err() == nil {
		stream, err := rs.getStreamAfterAvailable()
		if err != nil {
			return
		}

		// Re-push all outstanding entries for this node.
		rs.rePushOutstanding(stream)

		// Run send and recv in parallel.
		var wg sync.WaitGroup
		streamCtx, streamCancel := context.WithCancel(rs.ctx)

		wg.Add(1)
		go func() {
			defer wg.Done()
			rs.sendLoop(streamCtx, stream)
		}()

		// Recv in current goroutine.
		rs.recvLoop(stream)

		// Stream broke, cancel send loop and swap stream to nil.
		streamCancel()
		wg.Wait()

		// Signal streamCreatorLoop that the stream is broken.
		rs.swapStream(nil, nil)
	}
}

// sendLoop reads from sendCh and sends to the stream.
func (rs *resumableSyncer) sendLoop(ctx context.Context, stream viewpb.ViewSyncService_SyncQueryViewClient) {
	for {
		select {
		case <-ctx.Done():
			return
		case protos := <-rs.sendCh:
			req := &viewpb.SyncRequest{
				Request: &viewpb.SyncRequest_Views{
					Views: &viewpb.SyncQueryViewsRequest{
						QueryViews: protos,
					},
				},
			}
			if err := stream.Send(req); err != nil {
				log.Warn("ResumableSyncer: stream send failed",
					zap.String("node", rs.node.String()), zap.Error(err))
				return
			}
		}
	}
}

// recvLoop receives responses and routes them to outstanding callbacks.
func (rs *resumableSyncer) recvLoop(stream viewpb.ViewSyncService_SyncQueryViewClient) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Warn("ResumableSyncer: stream recv failed",
				zap.String("node", rs.node.String()), zap.Error(err))
			return
		}

		viewsResp := resp.GetViews()
		if viewsResp == nil {
			continue
		}

		for _, pb := range viewsResp.QueryViews {
			rs.outstanding.MatchResponse(pb)
		}
	}
}

// rePushOutstanding sends all outstanding entries for this node through the stream.
func (rs *resumableSyncer) rePushOutstanding(stream viewpb.ViewSyncService_SyncQueryViewClient) {
	protos := rs.outstanding.CollectProtosForNode(rs.node)
	if len(protos) == 0 {
		return
	}

	req := &viewpb.SyncRequest{
		Request: &viewpb.SyncRequest_Views{
			Views: &viewpb.SyncQueryViewsRequest{
				QueryViews: protos,
			},
		},
	}
	if err := stream.Send(req); err != nil {
		log.Warn("ResumableSyncer: re-push outstanding failed",
			zap.String("node", rs.node.String()), zap.Error(err))
	}
}

// swapStream atomically replaces the stream.
func (rs *resumableSyncer) swapStream(stream viewpb.ViewSyncService_SyncQueryViewClient, err error) {
	rs.cond.LockAndBroadcast()
	rs.stream = stream
	rs.err = err
	rs.cond.L.Unlock()
}

// getStreamAfterAvailable waits until a non-nil stream is available.
func (rs *resumableSyncer) getStreamAfterAvailable() (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	rs.cond.L.Lock()
	for rs.err == nil && rs.stream == nil {
		if err := rs.cond.Wait(rs.ctx); err != nil {
			return nil, err
		}
	}
	stream := rs.stream
	err := rs.err
	rs.cond.L.Unlock()

	if err != nil {
		return nil, err
	}
	return stream, nil
}

// waitForStreamBroken waits until the current stream is set to nil (broken).
func (rs *resumableSyncer) waitForStreamBroken() {
	rs.cond.L.Lock()
	for rs.err == nil && rs.stream != nil {
		if err := rs.cond.Wait(rs.ctx); err != nil {
			return
		}
	}
	rs.cond.L.Unlock()
}
