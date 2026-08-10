package messageack

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

type Message struct {
	message.ImmutableMessage
	record Record
}

func NewMessage(msg message.ImmutableMessage, record Record) Message {
	if record == nil {
		panic("message ack record is nil")
	}
	return Message{
		ImmutableMessage: msg,
		record:           record,
	}
}

func NewMetaMessage(msg message.ImmutableMessage) Message {
	return Message{ImmutableMessage: msg}
}

func (m Message) Ack() Record {
	return m.record
}

func (m Message) Message() message.ImmutableMessage {
	return m.ImmutableMessage
}

var _ message.ImmutableMessage = Message{}
