package block

import "github.com/milvus-io/milvus/internal/util/logserviceutil/message"

// MessageIDRange represents a range of message IDs.
type MessageIDRange struct {
	Begin message.MessageID
	Last  message.MessageID
}

// In checks if the given message ID is in the range.
func (r MessageIDRange) In(msgID message.MessageID) bool {
	if r.Begin == nil {
		return false
	}
	return r.Begin.LTE(msgID) && msgID.LTE(r.Last)
}
