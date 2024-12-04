package events

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// NewRecoveryEventBaseFromQVMeta creates a new event base from the query view meta.
func NewRecoveryEventBaseFromQVMeta(meta *viewpb.QueryViewMeta) RecoveryEventBase {
	return RecoveryEventBase{NewEventBase(qviews.NewShardIDFromQVMeta(meta))}
}

type RecoveryEventBase struct {
	EventBase
}

func (r RecoveryEventBase) isRecoveryEvent() {
}

type EventRecoverySwap struct {
	RecoveryEventBase
	OldVersion *QueryViewVersion
	NewView    *viewpb.QueryViewOfShard
}

func (EventRecoverySwap) EventType() EventType {
	return EventTypeRecoverySwap
}

type EventRecoverySaveNewUp struct {
	RecoveryEventBase
	Version QueryViewVersion
}

func (EventRecoverySaveNewUp) EventType() EventType {
	return EventTypeRecoverySaveNewUp
}

type EventRecoverySave struct {
	RecoveryEventBase
	Version QueryViewVersion
	State   QueryViewState
}

func (EventRecoverySave) EventType() EventType {
	return EventTypeRecoverySave
}

type EventRecoveryDelete struct {
	RecoveryEventBase
	Version QueryViewVersion
}

func (EventRecoveryDelete) EventType() EventType {
	return EventTypeRecoveryDelete
}
