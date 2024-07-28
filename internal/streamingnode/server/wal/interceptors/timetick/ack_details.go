package timetick

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// ackDetails records the information of AckDetail.
// Used to analyze the ack details.
// TODO: add more analysis methods. e.g. such as counter function with filter.
type ackDetails struct {
	detail []*ack.AckDetail
}

// AddDetails adds details to AckDetails.
func (ad *ackDetails) AddDetails(details []*ack.AckDetail) {
	if len(details) == 0 {
		return
	}
	if len(ad.detail) == 0 {
		ad.detail = details
		return
	}
	ad.detail = append(ad.detail, details...)
}

// Empty returns true if the AckDetails is empty.
func (ad *ackDetails) Empty() bool {
	return len(ad.detail) == 0
}

// Len returns the count of AckDetail.
func (ad *ackDetails) Len() int {
	return len(ad.detail)
}

// IsNoPersistedMessage returns true if no persisted message.
func (ad *ackDetails) IsNoPersistedMessage() bool {
	for _, detail := range ad.detail {
		// only sync message do not persist.
		// it just sync up the timetick with rootcoord
		if !detail.IsSync {
			return false
		}
	}
	return true
}

// LastAllAcknowledgedTimestamp returns the last timestamp which all timestamps before it have been acknowledged.
// panic if no timestamp has been acknowledged.
func (ad *ackDetails) LastAllAcknowledgedTimestamp() uint64 {
	return ad.detail[len(ad.detail)-1].Timestamp
}

// LastConfirmedMessageID returns the last confirmed message id.
func (ad *ackDetails) LastConfirmedMessageID() message.MessageID {
	// use the earliest last confirmed message id.
	var msgID message.MessageID
	for _, detail := range ad.detail {
		if msgID == nil {
			msgID = detail.LastConfirmedMessageID
			continue
		}
		if detail.LastConfirmedMessageID != nil && detail.LastConfirmedMessageID.LT(msgID) {
			msgID = detail.LastConfirmedMessageID
		}
	}
	return msgID
}

// Clear clears the AckDetails.
func (ad *ackDetails) Clear() {
	ad.detail = nil
}
