package message

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/nmq"
)

func TestMessage(t *testing.T) {
	b := NewBuilder()
	mutableMessage := b.WithMessageType(MessageTypeTimeTick).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{"key": "value"}).
		BuildMutable()

	assert.Equal(t, "payload", string(mutableMessage.Payload()))
	assert.True(t, mutableMessage.Properties().Exist("key"))
	v, ok := mutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, MessageTypeTimeTick, mutableMessage.MessageType())
	assert.Equal(t, 21, mutableMessage.EstimateSize())
	mutableMessage.WithTimeTick(123)
	v, ok = mutableMessage.Properties().Get("_tt")
	assert.True(t, ok)
	assert.Equal(t, "123", v)

	b = NewBuilder()
	immutableMessage := b.WithMessageID(nmq.NewNmqID(1)).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{
			"key": "value",
			"_t":  "1",
			"_tt": "456",
			"_v":  "1",
		}).
		BuildImmutable()

	assert.True(t, immutableMessage.MessageID().EQ(nmq.NewNmqID(1)))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 27, immutableMessage.EstimateSize())
	assert.Equal(t, Version(1), immutableMessage.Version())
	assert.Equal(t, uint64(456), immutableMessage.TimeTick())

	b = NewBuilder()
	immutableMessage = b.WithMessageID(nmq.NewNmqID(1)).
		WithPayload([]byte("payload")).
		WithProperty("key", "value").
		WithProperty("_t", "1").
		BuildImmutable()

	assert.True(t, immutableMessage.MessageID().EQ(nmq.NewNmqID(1)))
	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	assert.True(t, immutableMessage.Properties().Exist("key"))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.Equal(t, MessageTypeTimeTick, immutableMessage.MessageType())
	assert.Equal(t, 18, immutableMessage.EstimateSize())
	assert.Equal(t, Version(0), immutableMessage.Version())
	assert.Panics(t, func() {
		immutableMessage.TimeTick()
	})

	assert.Panics(t, func() {
		NewBuilder().WithMessageID(nmq.NewNmqID(1)).BuildMutable()
	})
	assert.Panics(t, func() {
		NewBuilder().BuildImmutable()
	})
}

func TestMessageType(t *testing.T) {
	s := MessageTypeUnknown.marshal()
	assert.Equal(t, "0", s)
	typ := unmarshalMessageType("0")
	assert.Equal(t, MessageTypeUnknown, typ)

	typ = unmarshalMessageType("882s9")
	assert.Equal(t, MessageTypeUnknown, typ)
}

func TestVersion(t *testing.T) {
	v := newMessageVersionFromString("")
	assert.Equal(t, VersionOld, v)
	assert.Panics(t, func() {
		newMessageVersionFromString("s1")
	})
	v = newMessageVersionFromString("1")
	assert.Equal(t, VersionV1, v)
}

func TestNewer(t *testing.T) {
	b := NewBuilder()
	mutableMessage := b.WithMessageType(MessageTypeTimeTick).
		WithPayload([]byte("payload")).
		WithProperties(map[string]string{"key": "value"}).
		BuildMutable()

	producerMessage := NewMQProducerMessageFromMutableMessage(mutableMessage)
	assert.True(t,
		bytes.Equal(
			producerMessage.Payload,
			mutableMessage.Payload(),
		))
	assert.Equal(t,
		producerMessage.Properties["key"],
		"value",
	)

	mutableMessage = NewMessageFromPBMessage(&logpb.Message{
		Payload:    []byte("payload"),
		Properties: map[string]string{"key": "value"},
	})
	assert.Equal(t, "payload", string(mutableMessage.Payload()))
	v, ok := mutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)

	immutableMessage := NewImmutableMessageFromPBMessage(&logpb.MessageID{
		Id: &logpb.MessageID_Nmq{
			Nmq: &logpb.MessageIDNmq{
				Offset: 1,
			},
		},
	}, &logpb.Message{
		Payload:    []byte("payload"),
		Properties: map[string]string{"key": "value"},
	})

	assert.Equal(t, "payload", string(immutableMessage.Payload()))
	v, ok = immutableMessage.Properties().Get("key")
	assert.Equal(t, "value", v)
	assert.True(t, ok)
	assert.True(t, immutableMessage.MessageID().EQ(nmq.NewNmqID(1)))
}
