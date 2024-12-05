package events

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

type SyncerEventBalanceAttrUpdate struct {
	EventBase
	BalanceAttr qviews.BalanceAttrAtWorkNode
}

func (s SyncerEventBalanceAttrUpdate) EventType() EventType {
	return EventTypeBalanceAttrUpdate
}
