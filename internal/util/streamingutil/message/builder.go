package message

import "github.com/milvus-io/milvus/internal/proto/streamingpb"

// NewImmutableMessage creates a new immutable message.
func NewImmutableMesasge(
	id MessageID,
	payload []byte,
	properties map[string]string,
) ImmutableMessage {
	return &immutableMessageImpl{
		id: id,
		messageImpl: messageImpl{
			payload:    payload,
			properties: properties,
		},
	}
}

// NewMutableMessage creates a new mutable message.
// Should only used at server side.
func NewMutableMessage(m *streamingpb.Message) MutableMessage {
	return &messageImpl{
		payload:    m.GetPayload(),
		properties: m.GetProperties(),
	}
}

// NewMutableMessageBuilder creates a new builder.
// Should only used at client side.
func NewMutableMessageBuilder() *MutableMesasgeBuilder {
	return &MutableMesasgeBuilder{
		payload:    nil,
		properties: make(propertiesImpl),
	}
}

// MutableMesasgeBuilder is the builder for message.
type MutableMesasgeBuilder struct {
	payload    []byte
	properties propertiesImpl
}

func (b *MutableMesasgeBuilder) WithMessageType(t MessageType) *MutableMesasgeBuilder {
	b.properties.Set(messageTypeKey, t.marshal())
	return b
}

// WithPayload creates a new builder with message payload.
// The MessageType is required to indicate which message type payload is.
func (b *MutableMesasgeBuilder) WithPayload(payload []byte) *MutableMesasgeBuilder {
	b.payload = payload
	return b
}

// WithProperty creates a new builder with message property.
// A key started with '_' is reserved for log system, should never used at user of client.
func (b *MutableMesasgeBuilder) WithProperty(key string, val string) *MutableMesasgeBuilder {
	b.properties.Set(key, val)
	return b
}

// WithProperties creates a new builder with message properties.
// A key started with '_' is reserved for log system, should never used at user of client.
func (b *MutableMesasgeBuilder) WithProperties(kvs map[string]string) *MutableMesasgeBuilder {
	for key, val := range kvs {
		b.properties.Set(key, val)
	}
	return b
}

// BuildMutable builds a mutable message.
// Panic if not set payload and message type.
// should only used at client side.
func (b *MutableMesasgeBuilder) BuildMutable() MutableMessage {
	if b.payload == nil {
		panic("message builder not ready for payload field")
	}
	if !b.properties.Exist(messageTypeKey) {
		panic("message builder not ready for message type field")
	}
	// Set message version.
	b.properties.Set(messageVersion, VersionV1.String())
	return &messageImpl{
		payload:    b.payload,
		properties: b.properties,
	}
}
