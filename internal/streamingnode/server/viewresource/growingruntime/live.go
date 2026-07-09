package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if r == nil {
		return
	}
	if event.Message != nil {
		r.applyLiveMessage(ctx, event.Message)
		return
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
	}
}

func (r *Runtime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) {
	if r == nil || msg == nil {
		return
	}
	if err := r.dispatchMessage(ctx, msg); err != nil {
		panic(errors.Wrap(err, "failed to apply live message to growing runtime"))
	}
	timeTick := msg.TimeTick()
	advancesTransform := messageAdvancesTransformFrontier(msg)
	r.markGrowingTimeTick(timeTick)
	if advancesTransform {
		r.markTransformTimeTick(timeTick)
	}
	mlog.Debug(ctx, "applied live message to growing runtime",
		mlog.FieldVChannel(msg.VChannel()),
		mlog.String("messageType", msg.MessageType().String()),
		mlog.Uint64("timeTick", timeTick),
		mlog.Bool("advancesTransform", advancesTransform),
		mlog.Uint64("appliedGrowingTimeTick", r.appliedGrowingTimeTick.Load()),
		mlog.Uint64("appliedTransformTimeTick", r.appliedTransformTimeTick.Load()),
	)
}

func messageAdvancesTransformFrontier(msg message.ImmutableMessage) bool {
	if msg == nil {
		return false
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete, message.MessageTypeRecoveryBarrier:
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
}, next uint64,
) bool {
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
