package message

import "google.golang.org/protobuf/proto"

var (
	_ BasicMessage        = (*messageImpl)(nil)
	_ MutableMessage      = (*messageImpl)(nil)
	_ ImmutableMessage    = (*immutableMessageImpl)(nil)
	_ ImmutableTxnMessage = (*immutableTxnMessageImpl)(nil)
)

// BasicMessage is the basic interface of message.
type BasicMessage interface {
	// MessageType returns the type of message.
	MessageType() MessageType

	// Version returns the message version.
	// 0: old version before streamingnode.
	// from 1: new version after streamingnode.
	Version() Version

	// Message payload.
	Payload() []byte

	// EstimateSize returns the estimated size of message.
	EstimateSize() int

	// Properties returns the message properties.
	// Should be used with read-only promise.
	Properties() RProperties

	// VChannel returns the virtual channel of current message.
	// Available only when the message's version greater than 0.
	// Return "" if message is broadcasted.
	VChannel() string

	// TimeTick returns the time tick of current message.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	TimeTick() uint64

	// BarrierTimeTick returns the barrier time tick of current message.
	// 0 by default, no fence.
	BarrierTimeTick() uint64

	// TxnContext returns the transaction context of current message.
	TxnContext() *TxnContext
}

// MutableMessage is the mutable message interface.
// Message can be modified before it is persistent by wal.
type MutableMessage interface {
	BasicMessage

	// WithBarrierTimeTick sets the barrier time tick of current message.
	// these time tick is used to promised the message will be sent after that time tick.
	// and the message which timetick is less than it will never concurrent append with it.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithBarrierTimeTick(tt uint64) MutableMessage

	// WithWALTerm sets the wal term of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithWALTerm(term int64) MutableMessage

	// WithLastConfirmed sets the last confirmed message id of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithLastConfirmed(id MessageID) MutableMessage

	// WithLastConfirmedUseMessageID sets the last confirmed message id of current message to be the same as message id.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithLastConfirmedUseMessageID() MutableMessage

	// WithTimeTick sets the time tick of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithTimeTick(tt uint64) MutableMessage

	// WithTxnContext sets the transaction context of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of streaming system.
	WithTxnContext(txnCtx TxnContext) MutableMessage

	// IntoImmutableMessage converts the mutable message to immutable message.
	IntoImmutableMessage(msgID MessageID) ImmutableMessage
}

// ImmutableMessage is the read-only message interface.
// Once a message is persistent by wal or temporary generated by wal, it will be immutable.
type ImmutableMessage interface {
	BasicMessage

	// WALName returns the name of message related wal.
	WALName() string

	// MessageID returns the message id of current message.
	MessageID() MessageID

	// LastConfirmedMessageID returns the last confirmed message id of current message.
	// last confirmed message is always a timetick message.
	// Read from this message id will guarantee the time tick greater than this message is consumed.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	LastConfirmedMessageID() MessageID

	// EvictPayload returns the evict payload of current message.
	EvictPayload() ImmutableMessage
}

// ImmutableTxnMessage is the read-only transaction message interface.
// Once a transaction is committed, the wal will generate a transaction message.
// The MessageType() is always return MessageTypeTransaction if it's a transaction message.
type ImmutableTxnMessage interface {
	ImmutableMessage

	// Begin returns the begin message of the transaction.
	Begin() ImmutableMessage

	// Commit returns the commit message of the transaction.
	Commit() ImmutableMessage

	// RangeOver iterates over the underlying messages in the transaction.
	// If visitor return not nil, the iteration will be stopped.
	RangeOver(visitor func(ImmutableMessage) error) error

	// Size returns the number of messages in the transaction.
	Size() int
}

// specializedMutableMessage is the specialized mutable message interface.
type specializedMutableMessage[H proto.Message, B proto.Message] interface {
	BasicMessage

	// MessageHeader returns the message header.
	// Modifications to the returned header will be reflected in the message.
	Header() H

	// Body returns the message body.
	// !!! Do these will trigger a unmarshal operation, so it should be used with caution.
	Body() (B, error)

	// OverwriteHeader overwrites the message header.
	OverwriteHeader(header H)
}

// specializedImmutableMessage is the specialized immutable message interface.
type specializedImmutableMessage[H proto.Message, B proto.Message] interface {
	ImmutableMessage

	// Header returns the message header.
	// Modifications to the returned header will be reflected in the message.
	Header() H

	// Body returns the message body.
	// !!! Do these will trigger a unmarshal operation, so it should be used with caution.
	Body() (B, error)
}
