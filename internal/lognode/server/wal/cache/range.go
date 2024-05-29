package cache

import "github.com/milvus-io/milvus/internal/util/logserviceutil/message"

type MessageIDRange struct {
	Begin message.MessageID
	Last  message.MessageID // represented inf it's nil
}

func (r MessageIDRange) In(msgID message.MessageID) bool {
	return r.Begin.LTE(msgID) && (r.Last == nil || msgID.LTE(r.Last))
}
