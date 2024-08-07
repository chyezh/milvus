package message

import "github.com/golang/protobuf/proto"

var (
	_ BasicMessage     = (*messageImpl)(nil)
	_ MutableMessage   = (*messageImpl)(nil)
	_ ImmutableMessage = (*immutableMessageImpl)(nil)
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
}

// MutableMessage is the mutable message interface.
// Message can be modified before it is persistent by wal.
type MutableMessage interface {
	BasicMessage

	// WithLastConfirmed sets the last confirmed message id of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of log system.
	WithLastConfirmed(id MessageID) MutableMessage

	// WithTimeTick sets the time tick of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of log system.
	WithTimeTick(tt uint64) MutableMessage

	// WithVChannel sets the virtual channel of current message.
	// !!! preserved for streaming system internal usage, don't call it outside of log system.
	WithVChannel(vChannel string) MutableMessage

	// IntoImmutableMessage converts the mutable message to immutable message.
	IntoImmutableMessage(msgID MessageID) ImmutableMessage
}

// ImmutableMessage is the read-only message interface.
// Once a message is persistent by wal or temporary generated by wal, it will be immutable.
type ImmutableMessage interface {
	BasicMessage

	// WALName returns the name of message related wal.
	WALName() string

	// VChannel returns the virtual channel of current message.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	VChannel() string

	// TimeTick returns the time tick of current message.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	TimeTick() uint64

	// LastConfirmedMessageID returns the last confirmed message id of current message.
	// last confirmed message is always a timetick message.
	// Read from this message id will guarantee the time tick greater than this message is consumed.
	// Available only when the message's version greater than 0.
	// Otherwise, it will panic.
	LastConfirmedMessageID() MessageID

	// MessageID returns the message id of current message.
	MessageID() MessageID
}

// specializedMutableMessage is the specialized mutable message interface.
type specializedMutableMessage[H proto.Message] interface {
	BasicMessage

	// VChannel returns the vchannel of the message.
	VChannel() string

	// TimeTick returns the time tick of the message.
	TimeTick() uint64

	// MessageHeader returns the message header.
	// Modifications to the returned header will be reflected in the message.
	MessageHeader() H

	// OverwriteMessageHeader overwrites the message header.
	OverwriteMessageHeader(header H)
}

// specializedImmutableMessage is the specialized immutable message interface.
type specializedImmutableMessage[H proto.Message] interface {
	ImmutableMessage

	// MessageHeader returns the message header.
	// Modifications to the returned header will be reflected in the message.
	MessageHeader() H
}
