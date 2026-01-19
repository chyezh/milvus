package streaming

import (
	"context"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	AppendResponses = types.AppendResponses
	AppendResponse  = types.AppendResponse
)

// AppendMessagesToWAL appends messages to the wal.
// It it a helper utility function to append messages to the wal.
// If the messages is belong to one vchannel, it will be sent as a transaction.
// Otherwise, it will be sent as individual messages.
// !!! This function do not promise the atomicity and deliver order of the messages appending.
func (w *walAccesserImpl) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	assertValidMessage(msgs...)

	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		err := types.NewAppendResponseN(len(msgs))
		err.FillAllError(ErrWALAccesserClosed)
		return err
	}
	defer w.lifetime.Done()

	// dispatch the messages into different vchannel.
	dispatchedMessages, _ := w.dispatchMessages(msgs...)

	guards := make([]*producer.ProduceGuard, 0)
	resp := types.NewAppendResponseN(len(msgs))
	for vchannel, msgs := range dispatchedMessages {
		g, err := w.getProducer(vchannel).BeginProduce(ctx, msgs...)
		if err != nil {
			for _, guard := range guards {
				guard.Cancel()
			}
			resp.FillAllError(err)
			return resp
		}
		guards = append(guards, g)
	}

	return producer.BatchCommitProduce(ctx, guards...)
}

// dispatchMessages dispatches the messages into different vchannel.
func (w *walAccesserImpl) dispatchMessages(msgs ...message.MutableMessage) (map[string][]message.MutableMessage, map[string][]int) {
	dispatchedMessages := make(map[string][]message.MutableMessage, 0)
	indexes := make(map[string][]int, 0)
	for idx, msg := range msgs {
		vchannel := msg.VChannel()
		if _, ok := dispatchedMessages[vchannel]; !ok {
			dispatchedMessages[vchannel] = make([]message.MutableMessage, 0)
			indexes[vchannel] = make([]int, 0)
		}
		dispatchedMessages[vchannel] = append(dispatchedMessages[vchannel], msg)
		indexes[vchannel] = append(indexes[vchannel], idx)
	}
	return dispatchedMessages, indexes
}

// applyOpt applies the append options to the message.
func applyOpt(msg message.MutableMessage, opts ...AppendOption) message.MutableMessage {
	if len(opts) == 0 {
		return msg
	}
	if opts[0].BarrierTimeTick > 0 {
		msg = msg.WithBarrierTimeTick(opts[0].BarrierTimeTick)
	}
	return msg
}
