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
)

// resumableSyncer manages a single gRPC bidirectional stream to a work node.
// It runs a single loop that creates a stream, pushes/re-pushes pending views,
// and on stream break decides to reconnect with exponential backoff.
type resumableSyncer struct {
	node    qviews.WorkNode
	client  NodeClient
	pending *pendingSyncQueryViews
	sendCh  chan []*viewpb.QueryViewOfShard

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResumableSyncer(
	ctx context.Context,
	node qviews.WorkNode,
	client NodeClient,
	pending *pendingSyncQueryViews,
	sendBufferSize int,
) *resumableSyncer {
	ctx, cancel := context.WithCancel(ctx)
	rs := &resumableSyncer{
		node:    node,
		client:  client,
		pending: pending,
		sendCh:  make(chan []*viewpb.QueryViewOfShard, sendBufferSize),
		ctx:     ctx,
		cancel:  cancel,
	}
	rs.wg.Add(1)
	go rs.loop()
	return rs
}

// Sync adds views to the pending queue and enqueues the resulting protos for sending.
// All views MUST target the same work node that this resumableSyncer manages.
func (rs *resumableSyncer) Sync(views []SyncView) {
	protos := make([]*viewpb.QueryViewOfShard, 0, len(views))
	for i := range views {
		protos = append(protos, rs.pending.Upsert(views[i]))
	}
	select {
	case rs.sendCh <- protos:
	case <-rs.ctx.Done():
	}
}

// Close stops the ResumableSyncer and waits for the goroutine to exit.
func (rs *resumableSyncer) Close() {
	rs.cancel()
	rs.wg.Wait()
}

// loop is the single goroutine that manages the stream lifecycle:
// create stream → re-push pending → send/recv → on break, backoff and retry.
func (rs *resumableSyncer) loop() {
	defer rs.wg.Done()

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 10 * time.Second
	bo.MaxElapsedTime = 0 // retry forever until closed
	bo.Reset()

	for rs.ctx.Err() == nil {
		// Create stream.
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

		// Re-push all pending entries for this node.
		rs.rePush(stream)

		// Run send and recv in parallel; recv drives the current goroutine.
		streamCtx, streamCancel := context.WithCancel(rs.ctx)

		var sendWg sync.WaitGroup
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()
			rs.sendLoop(streamCtx, stream)
		}()

		// Recv blocks until stream breaks.
		rs.recvLoop(stream)

		// Stream broke — cancel send loop and wait.
		streamCancel()
		sendWg.Wait()
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

// recvLoop receives responses and routes them to pending callbacks.
// Returns when the stream breaks.
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
			rs.pending.MatchResponse(pb)
		}
	}
}

// rePush sends all pending entries for this node through the stream.
func (rs *resumableSyncer) rePush(stream viewpb.ViewSyncService_SyncQueryViewClient) {
	protos := rs.pending.CollectProtosForNode(rs.node)
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
		log.Warn("ResumableSyncer: re-push pending failed",
			zap.String("node", rs.node.String()), zap.Error(err))
	}
}
