package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
)

type defaultMessageHandler chan message.ImmutableMessage

func (h defaultMessageHandler) Handle(param wal.HandleParam) wal.HandleResult {
	var sendingCh chan message.ImmutableMessage
	if param.Message != nil {
		sendingCh = h
	}
	select {
	case <-param.Ctx.Done():
		return wal.HandleResult{Error: param.Ctx.Err()}
	case msg, ok := <-param.Upstream:
		if !ok {
			return wal.HandleResult{Error: wal.ErrUpstreamClosed}
		}
		return wal.HandleResult{Incoming: msg}
	case sendingCh <- param.Message:
		return wal.HandleResult{Messagehandled: true}
	case <-param.TimeTickChan:
		return wal.HandleResult{TimeTickUpdated: true}
	}
}

func (d defaultMessageHandler) Close() {
	close(d)
}

// NewMsgPackAdaptorHandler create a new message pack adaptor handler.
func NewMsgPackAdaptorHandler() *MsgPackAdaptorHandler {
	return &MsgPackAdaptorHandler{}
}

type MsgPackAdaptorHandler struct {
	base *adaptor.BaseMsgPackAdaptorHandler
}

// Chan is the channel for message.
func (m *MsgPackAdaptorHandler) Chan() <-chan *msgstream.MsgPack {
	return m.base.Channel
}

// Handle is the callback for handling message.
func (m *MsgPackAdaptorHandler) Handle(ctx context.Context, upstream <-chan message.ImmutableMessage, msg message.ImmutableMessage) (incoming message.ImmutableMessage, ok bool, err error) {
	// not handle new message if there are pending msgPack.
	if msg != nil && m.base.PendingMsgPack.Len() == 0 {
		m.base.GenerateMsgPack(msg)
		ok = true
	}

	for {
		var sendCh chan<- *msgstream.MsgPack
		if m.base.PendingMsgPack.Len() != 0 {
			sendCh = m.base.Channel
		}

		select {
		case <-ctx.Done():
			return nil, ok, ctx.Err()
		case msg, notClose := <-upstream:
			if !notClose {
				return nil, ok, wal.ErrUpstreamClosed
			}
			return msg, ok, nil
		case sendCh <- m.base.PendingMsgPack.Next():
			m.base.PendingMsgPack.UnsafeAdvance()
			if m.base.PendingMsgPack.Len() > 0 {
				continue
			}
			return nil, ok, nil
		}
	}
}

// Close close the handler.
func (m *MsgPackAdaptorHandler) Close() {
	close(m.base.Channel)
}
