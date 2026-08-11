package message

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// AsImmutableTxnMessage converts an ImmutableMessage to ImmutableTxnMessage
var AsImmutableTxnMessage = func(msg ImmutableMessage) ImmutableTxnMessage {
	if txn, ok := msg.(ImmutableTxnMessage); ok {
		return txn
	}
	if _, ok := msg.(wrappedImmutableMessage); ok {
		if _, ok := unwrapImmutableMessage(msg).(*immutableTxnMessageImpl); ok {
			return newImmutableTxnMessageView(msg)
		}
	}
	return nil
}

type immutableTxnMessageView struct {
	ImmutableMessage
}

func newImmutableTxnMessageView(msg ImmutableMessage) ImmutableTxnMessage {
	view := &immutableTxnMessageView{ImmutableMessage: msg}
	if retained, ok := msg.(RetainedImmutableMessage); ok {
		return &retainedImmutableTxnMessage{
			immutableTxnMessageView: view,
			retained:                retained,
		}
	}
	if refCounted, ok := msg.(RefCountedImmutableMessage); ok {
		return &refCountedImmutableTxnMessage{
			immutableTxnMessageView: view,
			refCounted:              refCounted,
		}
	}
	return view
}

func (m *immutableTxnMessageView) unwrapImmutableMessage() ImmutableMessage {
	return m.ImmutableMessage
}

func (m *immutableTxnMessageView) Begin() ImmutableMessage {
	return newBorrowedTxnChildMessage(m.ImmutableMessage, txnChildBegin, 0)
}

func (m *immutableTxnMessageView) Commit() ImmutableMessage {
	return newBorrowedTxnChildMessage(m.ImmutableMessage, txnChildCommit, 0)
}

func (m *immutableTxnMessageView) RangeOver(visitor func(ImmutableMessage) error) error {
	size := m.Size()
	for idx := 0; idx < size; idx++ {
		if err := visitor(newBorrowedTxnChildMessage(m.ImmutableMessage, txnChildBody, idx)); err != nil {
			return err
		}
	}
	return nil
}

func (m *immutableTxnMessageView) Size() int {
	return m.rawTxn().Size()
}

func (m *immutableTxnMessageView) rawTxn() ImmutableTxnMessage {
	raw, ok := unwrapImmutableMessage(m.ImmutableMessage).(*immutableTxnMessageImpl)
	if !ok {
		panic("immutable transaction message accessed after release")
	}
	return raw
}

type refCountedImmutableTxnMessage struct {
	*immutableTxnMessageView
	refCounted RefCountedImmutableMessage
}

func (m *refCountedImmutableTxnMessage) Retain() RetainedImmutableMessage {
	retained := m.refCounted.Retain()
	return AsImmutableTxnMessage(retained).(RetainedImmutableMessage)
}

type retainedImmutableTxnMessage struct {
	*immutableTxnMessageView
	retained RetainedImmutableMessage
}

func (m *retainedImmutableTxnMessage) Sealed() bool {
	return m.retained.Sealed()
}

func (m *retainedImmutableTxnMessage) IsExclusive() bool {
	return m.retained.IsExclusive()
}

func (m *retainedImmutableTxnMessage) Release() {
	m.retained.Release()
}

type txnChildKind uint8

const (
	txnChildBegin txnChildKind = iota
	txnChildBody
	txnChildCommit
)

type borrowedTxnChildProvider struct {
	parent ImmutableMessage
	kind   txnChildKind
	index  int
}

func newBorrowedTxnChildMessage(parent ImmutableMessage, kind txnChildKind, index int) ImmutableMessage {
	provider := &borrowedTxnChildProvider{
		parent: parent,
		kind:   kind,
		index:  index,
	}
	return &borrowedTxnChildMessage{immutableMessageView: newImmutableMessageView(provider)}
}

func (p *borrowedTxnChildProvider) immutableMessage() ImmutableMessage {
	raw, ok := unwrapImmutableMessage(p.parent).(*immutableTxnMessageImpl)
	if !ok {
		panic("borrowed transaction child accessed after parent release")
	}
	switch p.kind {
	case txnChildBegin:
		return raw.Begin()
	case txnChildCommit:
		return raw.Commit()
	case txnChildBody:
		var child ImmutableMessage
		idx := 0
		_ = raw.RangeOver(func(msg ImmutableMessage) error {
			if idx == p.index {
				child = msg
			}
			idx++
			return nil
		})
		if child == nil {
			panic("borrowed transaction child index out of range")
		}
		return child
	default:
		panic("unknown borrowed transaction child kind")
	}
}

type borrowedTxnChildMessage struct {
	*immutableMessageView
}

// NewMessageTypeWithVersion creates a new MessageTypeWithVersion.
func NewMessageTypeWithVersion(t MessageType, v Version) MessageTypeWithVersion {
	return MessageTypeWithVersion{MessageType: t, Version: v}
}

// GetSerializeType returns the specialized message type for the given message type and version.
func GetSerializeType(mv MessageTypeWithVersion) (MessageSpecializedType, bool) {
	if mv.Version == VersionOld {
		// There's some old messages that is coming from old arch of msgstream.
		// We need to convert them to versionV1 to find the specialized type.
		mv.Version = VersionV1
	}
	typ, ok := messageTypeVersionSpecializedMap[mv]
	return typ, ok
}

// GetMessageTypeWithVersion returns the message type with version for the given message type and version.
func GetMessageTypeWithVersion[H proto.Message, B proto.Message]() (MessageTypeWithVersion, bool) {
	var h H
	var b B
	styp := MessageSpecializedType{
		HeaderType: reflect.TypeOf(h),
		BodyType:   reflect.TypeOf(b),
	}
	mv, ok := messageSpecializedTypeVersionMap[styp]
	return mv, ok
}

// MustGetMessageTypeWithVersion returns the message type with version for the given message type and version, panics on error.
func MustGetMessageTypeWithVersion[H proto.Message, B proto.Message]() MessageTypeWithVersion {
	mv, ok := GetMessageTypeWithVersion[H, B]()
	if !ok {
		panic("message type not found")
	}
	return mv
}

// ReplicateHeader is the header of replicate message.
type ReplicateHeader struct {
	ClusterID              string
	MessageID              MessageID
	LastConfirmedMessageID MessageID
	TimeTick               uint64
	VChannel               string
}

// ClearReplicateHeader removes replicate header from a mutable message.
// Used during force promote fix to re-append as primary messages.
func ClearReplicateHeader(msg MutableMessage) MutableMessage {
	if msg == nil {
		return nil
	}
	if impl, ok := msg.(*messageImpl); ok {
		impl.properties.Delete(messageReplicateMesssageHeader)
		return impl
	}
	raw := msg.Properties().ToRawMap()
	delete(raw, messageReplicateMesssageHeader)
	return NewMutableMessageBeforeAppend(msg.Payload(), raw)
}
