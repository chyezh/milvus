package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (r *Runtime) startLiveApply(ctx context.Context, done <-chan struct{}, onApplied func()) {
	if r == nil || r.LiveEvents == nil {
		return
	}
	if done == nil {
		done = neverDone()
	}
	r.mu.Lock()
	if r.liveStopCh == nil {
		r.liveStopCh = make(chan struct{})
	}
	if r.liveDoneCh == nil {
		r.liveDoneCh = make(chan struct{})
	}
	stopCh := r.liveStopCh
	doneCh := r.liveDoneCh
	r.mu.Unlock()
	go func() {
		defer close(doneCh)
		for {
			select {
			case event, ok := <-r.LiveEvents:
				if !ok {
					return
				}
				if r.applyLiveEvent(ctx, event) && onApplied != nil {
					onApplied()
				}
			case <-done:
				return
			case <-stopCh:
				return
			}
		}
	}()
}

func (r *Runtime) applyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	if event.Message != nil {
		return r.applyLiveMessage(ctx, event.Message)
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
		if bm25 := r.bm25Runtime(); bm25 != nil {
			bm25.ApplySegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
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
	if bm25 := r.bm25Runtime(); bm25 != nil {
		if err := bm25.ApplyLiveMessage(ctx, msg); err != nil {
			panic(errors.Wrap(err, "failed to apply live message to BM25 runtime"))
		}
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

func neverDone() <-chan struct{} {
	return make(chan struct{})
}
