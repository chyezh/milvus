package message

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// mustAsSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func mustAsSpecializedMutableMessage[H proto.Message, B proto.Message](msg BasicMessage) specializedMutableMessage[H, B] {
	smsg, err := asSpecializedMutableMessage[H, B](msg)
	if err != nil {
		panic(
			fmt.Sprintf("failed to parse mutable message: %s @ %s, %d, %d",
				err.Error(),
				msg.MessageType(),
				msg.TimeTick(),
				msg.Version(),
			))
	}
	return smsg
}

// asSpecializedMutableMessage converts a MutableMessage to a specialized MutableMessage.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return specializedMutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedMutableMessage[H proto.Message, B proto.Message](msg BasicMessage) (specializedMutableMessage[H, B], error) {
	if already, ok := msg.(specializedMutableMessage[H, B]); ok {
		return already, nil
	}
	underlying := msg.(*messageImpl)

	var header H
	msgType := MustGetMessageTypeWithVersion[H, B]()
	if underlying.MessageType() != msgType.MessageType {
		// The message type do not match the specialized header.
		return nil, merr.WrapErrParameterInvalidMsg("message type do not match specialized header")
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageHeader)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("lost specialized header, %s", msgType.String())
	}

	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(header)
	t.Elem()
	header = reflect.New(t.Elem()).Interface().(H)

	// must be a pointer to a proto message
	if err := DecodeProto(val, header); err != nil {
		return nil, errors.Wrap(err, "failed to decode specialized header")
	}
	return &specializedMutableMessageImpl[H, B]{
		header:      header,
		messageImpl: underlying,
	}, nil
}

// MustAsSpecializedImmutableMessage converts a ImmutableMutableMessage to a specialized ImmutableMutableMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func MustAsSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) SpecializedImmutableMessage[H, B] {
	smsg, err := asSpecializedImmutableMessage[H, B](msg)
	if err != nil {
		panic(
			fmt.Sprintf("failed to parse immutable message: %s @ %s, %s, %s, %d, %d",
				err.Error(),
				msg.MessageID(),
				msg.MessageType(),
				msg.LastConfirmedMessageID(),
				msg.TimeTick(),
				msg.Version(),
			))
	}
	return smsg
}

// MustAsRefCountedSpecializedImmutableMessage specializes a ref-counted
// immutable message and panics if the message does not expose ref counting.
func MustAsRefCountedSpecializedImmutableMessage[H proto.Message, B proto.Message](
	msg ImmutableMessage,
) RefCountedSpecializedImmutableMessage[H, B] {
	specialized := MustAsSpecializedImmutableMessage[H, B](msg)
	refCounted, ok := specialized.(RefCountedSpecializedImmutableMessage[H, B])
	if !ok {
		panic("specialized immutable message is not ref-counted")
	}
	return refCounted
}

// MustAsRetainedSpecializedImmutableMessage specializes a retained immutable
// message and panics if the message does not own a retained reference.
func MustAsRetainedSpecializedImmutableMessage[H proto.Message, B proto.Message](
	msg ImmutableMessage,
) RetainedSpecializedImmutableMessage[H, B] {
	specialized := MustAsSpecializedImmutableMessage[H, B](msg)
	retained, ok := specialized.(RetainedSpecializedImmutableMessage[H, B])
	if !ok {
		panic("specialized immutable message is not retained")
	}
	return retained
}

// asSpecializedImmutableMessage converts a ImmutableMessage to a specialized ImmutableMessage.
// Return nil, error if the message is the target specialized message but failed to decode the specialized header.
// Return asSpecializedImmutableMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedImmutableMessage[H proto.Message, B proto.Message](msg ImmutableMessage) (SpecializedImmutableMessage[H, B], error) {
	if already, ok := msg.(SpecializedImmutableMessage[H, B]); ok {
		return already, nil
	}
	underlying, ok := unwrapImmutableMessage(msg).(*immutableMessageImpl)
	if !ok {
		// maybe a txn message.
		return nil, merr.WrapErrParameterInvalidMsg("not a specialized immutable message, txn message maybe")
	}

	var header H
	msgType := MustGetMessageTypeWithVersion[H, B]()
	if underlying.MessageType() != msgType.MessageType {
		// The message type do not match the specialized header.
		return nil, merr.WrapErrParameterInvalidMsg("message type do not match specialized header")
	}

	// Get the specialized header from the message.
	val, ok := underlying.properties.Get(messageHeader)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("lost specialized header, %s", msgType.String())
	}

	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(header)
	header = reflect.New(t.Elem()).Interface().(H)

	// must be a pointer to a proto message
	if err := DecodeProto(val, header); err != nil {
		return nil, errors.Wrap(err, "failed to decode specialized header")
	}
	if _, ok := msg.(wrappedImmutableMessage); ok {
		return newSpecializedImmutableMessageView[H, B](msg, header), nil
	}
	return &specializedImmutableMessageImpl[H, B]{
		header:               header,
		immutableMessageImpl: underlying,
	}, nil
}

// asSpecializedBroadcastMessage converts a BasicMessage to a specialized BroadcastMessage.
// Return nil, error if the message is not the target specialized message or failed to decode the specialized header.
// Return specializedBroadcastMessage, nil if the message is the target specialized message and successfully decoded the specialized header.
func asSpecializedBroadcastMessage[H proto.Message, B proto.Message](msg BasicMessage) (SpecializedBroadcastMessage[H, B], error) {
	if already, ok := msg.(SpecializedBroadcastMessage[H, B]); ok {
		return already, nil
	}
	sm, err := asSpecializedMutableMessage[H, B](msg)
	if err != nil {
		return nil, err
	}
	return sm.(*specializedMutableMessageImpl[H, B]), nil
}

// MustAsSpecializedBroadcastMessage converts a BasicMessage to a specialized BroadcastMessage.
// It will panic if the message is not the target specialized message or failed to decode the specialized header.
func MustAsSpecializedBroadcastMessage[H proto.Message, B proto.Message](msg BasicMessage) SpecializedBroadcastMessage[H, B] {
	smsg, err := asSpecializedBroadcastMessage[H, B](msg)
	if err != nil {
		panic(err)
	}
	return smsg
}

// specializedMutableMessageImpl is the specialized mutable message implementation.
type specializedMutableMessageImpl[H proto.Message, B proto.Message] struct {
	header H
	*messageImpl
}

// MessageHeader returns the message header.
func (m *specializedMutableMessageImpl[H, B]) Header() H {
	return m.header
}

// Body returns the message body.
func (m *specializedMutableMessageImpl[H, B]) Body() (B, error) {
	return unmarshalProtoB[B](m.Payload())
}

// MustBody returns the message body.
func (m *specializedMutableMessageImpl[H, B]) MustBody() B {
	b, err := m.Body()
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal specialized body,%s", err.Error()))
	}
	return b
}

// OverwriteMessageHeader overwrites the message header.
func (m *specializedMutableMessageImpl[H, B]) OverwriteHeader(header H) {
	m.header = header
	newHeader, err := EncodeProto(m.header)
	if err != nil {
		panic(fmt.Sprintf("failed to encode insert header, there's a bug, %+v, %s", m.header, err.Error()))
	}
	m.properties.Set(messageHeader, newHeader)
}

// OverwriteBody overwrites the message body.
func (m *specializedMutableMessageImpl[H, B]) OverwriteBody(body B) {
	payload, err := proto.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal specialized body, %s", err.Error()))
	}
	if ch := m.cipherHeader(); ch != nil {
		cipher := mustGetCipher()
		encryptor, safeKey, err := cipher.GetEncryptor(ch.EzId, ch.CollectionId)
		if err != nil {
			panic(fmt.Sprintf("failed to get encryptor when overwriting specialized body, %s", err.Error()))
		}
		payloadBytes := len(payload)
		payload, err = encryptor.Encrypt(payload)
		if err != nil {
			panic(fmt.Sprintf("failed to encrypt overwritten specialized body, %s", err.Error()))
		}
		cipherHeader, err := EncodeProto(&messagespb.CipherHeader{
			EzId:         ch.EzId,
			CollectionId: ch.CollectionId,
			SafeKey:      safeKey,
			PayloadBytes: int64(payloadBytes),
		})
		if err != nil {
			panic(fmt.Sprintf("failed to encode overwritten specialized body cipher header, %s", err.Error()))
		}
		m.properties.Set(messageCipherHeader, cipherHeader)
	}
	m.payload = payload
}

// BroadcastMessage returns the broadcast message.
func (m *specializedMutableMessageImpl[H, B]) BroadcastMessage() BroadcastMutableMessage {
	return m.messageImpl
}

// specializedImmutableMessageImpl is the specialized immmutable message implementation.
type specializedImmutableMessageImpl[H proto.Message, B proto.Message] struct {
	header H
	*immutableMessageImpl
}

func (m *specializedImmutableMessageImpl[H, B]) unwrapImmutableMessage() ImmutableMessage {
	return m.immutableMessageImpl
}

// Header returns the message header.
func (m *specializedImmutableMessageImpl[H, B]) Header() H {
	return m.header
}

// Body returns the message body.
func (m *specializedImmutableMessageImpl[H, B]) Body() (B, error) {
	return unmarshalProtoB[B](m.Payload())
}

// Must Body returns the message body.
func (m *specializedImmutableMessageImpl[H, B]) MustBody() B {
	b, err := m.Body()
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal specialized body, %s, %s", m.MessageID().String(), err.Error()))
	}
	return b
}

type specializedImmutableMessageView[H proto.Message, B proto.Message] struct {
	ImmutableMessage
	header H
}

func newSpecializedImmutableMessageView[H proto.Message, B proto.Message](
	msg ImmutableMessage,
	header H,
) SpecializedImmutableMessage[H, B] {
	view := &specializedImmutableMessageView[H, B]{
		ImmutableMessage: msg,
		header:           header,
	}
	if retained, ok := msg.(RetainedImmutableMessage); ok {
		return &retainedSpecializedImmutableMessage[H, B]{
			specializedImmutableMessageView: view,
			retained:                        retained,
		}
	}
	if refCounted, ok := msg.(RefCountedImmutableMessage); ok {
		return &refCountedSpecializedImmutableMessage[H, B]{
			specializedImmutableMessageView: view,
			refCounted:                      refCounted,
		}
	}
	return view
}

func (m *specializedImmutableMessageView[H, B]) unwrapImmutableMessage() ImmutableMessage {
	return m.ImmutableMessage
}

func (m *specializedImmutableMessageView[H, B]) Header() H {
	_ = m.MessageType()
	return m.header
}

func (m *specializedImmutableMessageView[H, B]) Body() (B, error) {
	return unmarshalProtoB[B](m.Payload())
}

func (m *specializedImmutableMessageView[H, B]) MustBody() B {
	body, err := m.Body()
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal specialized body, %s, %s", m.MessageID().String(), err.Error()))
	}
	return body
}

type refCountedSpecializedImmutableMessage[H proto.Message, B proto.Message] struct {
	*specializedImmutableMessageView[H, B]
	refCounted RefCountedImmutableMessage
}

func (m *refCountedSpecializedImmutableMessage[H, B]) Retain() RetainedImmutableMessage {
	retained := m.refCounted.Retain()
	specialized, err := asSpecializedImmutableMessage[H, B](retained)
	if err != nil {
		panic(err)
	}
	return specialized.(RetainedImmutableMessage)
}

type retainedSpecializedImmutableMessage[H proto.Message, B proto.Message] struct {
	*specializedImmutableMessageView[H, B]
	retained RetainedImmutableMessage
}

func (m *retainedSpecializedImmutableMessage[H, B]) Sealed() bool {
	return m.retained.Sealed()
}

func (m *retainedSpecializedImmutableMessage[H, B]) IsExclusive() bool {
	return m.retained.IsExclusive()
}

func (m *retainedSpecializedImmutableMessage[H, B]) Release() {
	m.retained.Release()
}

func unmarshalProtoB[B proto.Message](data []byte) (B, error) {
	var nilBody B
	// Decode the specialized header.
	// Must be pointer type.
	t := reflect.TypeOf(nilBody)
	t.Elem()
	body := reflect.New(t.Elem()).Interface().(B)

	err := proto.Unmarshal(data, body)
	if err != nil {
		return nilBody, err
	}
	return body, nil
}
