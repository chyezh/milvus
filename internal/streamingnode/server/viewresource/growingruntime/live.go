package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

var _ walview.VChannelLiveObserver = (*Runtime)(nil)

func (r *Runtime) ObserveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == stateClosed {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	default:
	}
	r.pendingEvents = append(r.pendingEvents, event)
	if r.state == stateReady {
		r.startDrainLocked()
	}
	return true
}

func (r *Runtime) markReady() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == stateClosed {
		return
	}
	r.state = stateReady
	if len(r.pendingEvents) == 0 {
		r.closePendingDrainedLocked()
	}
	r.startDrainLocked()
}

func (r *Runtime) startDrainLocked() {
	if r.drainRunning || len(r.pendingEvents) == 0 {
		return
	}
	r.drainRunning = true
	r.drainWG.Add(1)
	go func() {
		defer r.drainWG.Done()
		r.drainPending()
	}()
}

func (r *Runtime) drainPending() {
	for {
		r.mu.Lock()
		if r.state == stateClosed {
			r.drainRunning = false
			r.mu.Unlock()
			return
		}
		if len(r.pendingEvents) == 0 {
			r.drainRunning = false
			r.closePendingDrainedLocked()
			r.mu.Unlock()
			return
		}
		event := r.pendingEvents[0]
		copy(r.pendingEvents, r.pendingEvents[1:])
		r.pendingEvents[len(r.pendingEvents)-1] = walview.VChannelResourceEvent{}
		r.pendingEvents = r.pendingEvents[:len(r.pendingEvents)-1]
		r.mu.Unlock()

		if r.applyLiveEvent(context.Background(), event) && r.desc.OnApplied != nil {
			r.desc.OnApplied()
		}
	}
}

func (r *Runtime) applyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	if event.Message != nil {
		advanced := r.applyLiveMessage(ctx, event.Message)
		if bm25 := r.bm25Runtime(); bm25 != nil {
			if err := bm25.ApplyLiveEvent(ctx, event); err != nil {
				panic(errors.Wrap(err, "failed to apply live event to BM25 runtime"))
			}
		}
		return advanced
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
		if bm25 := r.bm25Runtime(); bm25 != nil {
			if err := bm25.ApplyLiveEvent(ctx, event); err != nil {
				panic(errors.Wrap(err, "failed to apply live event to BM25 runtime"))
			}
		}
		return true
	}
	return false
}

func (r *Runtime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) bool {
	if r == nil || msg == nil {
		return false
	}
	if err := r.dispatchMessage(ctx, msg); err != nil {
		panic(errors.Wrap(err, "failed to apply live message to growing runtime"))
	}
	timeTick := msg.TimeTick()
	advanced := advanceTimeTick(&r.appliedGrowingTimeTick, timeTick)
	if messageAdvancesTransformFrontier(msg) {
		advanced = advanceTimeTick(&r.appliedTransformTimeTick, timeTick) || advanced
	}
	return advanced
}

func messageAdvancesTransformFrontier(msg message.ImmutableMessage) bool {
	if msg == nil {
		return false
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		return true
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return false
		}
		containsDelete := false
		_ = txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() == message.MessageTypeDelete {
				containsDelete = true
			}
			return nil
		})
		return containsDelete
	default:
		return false
	}
}

func advanceTimeTick(value interface {
	Load() uint64
	CompareAndSwap(old uint64, new uint64) bool
}, next uint64) bool {
	for {
		current := value.Load()
		if next <= current {
			return false
		}
		if value.CompareAndSwap(current, next) {
			return true
		}
	}
}
