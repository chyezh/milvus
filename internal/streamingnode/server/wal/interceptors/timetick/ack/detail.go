package ack

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

type AckDetailControl int

const (
	controlNone AckDetailControl = 0
	controlSync AckDetailControl = 1 << iota
	controlRefresh
)

func (c AckDetailControl) IsControl() bool {
	return c != controlNone
}

func (c AckDetailControl) MetricString() string {
	switch c {
	case controlNone:
		return "none"
	case controlSync:
		return "sync"
	case controlRefresh:
		return "refresh"
	default:
		panic(fmt.Sprintf("unknown control %d", c))
	}
}

// newAckDetail creates a new default acker detail.
func newAckDetail(ts uint64, lastConfirmedMessageID message.MessageID) *AckDetail {
	if ts <= 0 {
		panic(fmt.Sprintf("ts should never less than 0 %d", ts))
	}
	return &AckDetail{
		AckDetailControl:       controlNone,
		BeginTimestamp:         ts,
		LastConfirmedMessageID: lastConfirmedMessageID,
		Err:                    nil,
	}
}

// AckDetail records the information of acker.
type AckDetail struct {
	AckDetailControl // The control ack is used to make some control operation in timetick manager.

	BeginTimestamp uint64 // the timestamp when acker is allocated.
	EndTimestamp   uint64 // the timestamp when acker is acknowledged.
	// for avoiding allocation of timestamp failure, the timestamp will use the ack manager last allocated timestamp.
	LastConfirmedMessageID message.MessageID
	MessageID              message.MessageID
	TxnSession             *txn.TxnSession
	Err                    error
}

// AckOption is the option for acker.
type AckOption func(*AckDetail)

// optSync marks the acker is sync message.
func optSync() AckOption {
	return func(detail *AckDetail) {
		detail.AckDetailControl |= controlSync
	}
}

func optRefresh() AckOption {
	return func(detail *AckDetail) {
		detail.AckDetailControl |= controlRefresh
	}
}

// OptError marks the timestamp ack with error info.
func OptError(err error) AckOption {
	return func(detail *AckDetail) {
		detail.Err = err
	}
}

// OptMessageID marks the message id for acker.
func OptMessageID(messageID message.MessageID) AckOption {
	return func(detail *AckDetail) {
		detail.MessageID = messageID
	}
}

// OptTxnSession marks the session for acker.
func OptTxnSession(session *txn.TxnSession) AckOption {
	return func(detail *AckDetail) {
		detail.TxnSession = session
	}
}
