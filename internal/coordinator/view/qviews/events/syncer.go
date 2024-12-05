package events

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

var (
	_ SyncerViewEvent = SyncerEventSent{}
	_ SyncerViewEvent = SyncerEventOverwrite{}
	_ SyncerViewEvent = SyncerEventAck{}
	_ SyncerEvent     = SyncerEventBalanceAttrUpdate{}
)

func NewSyncerViewEventBase(shardID ShardID, version qviews.QueryViewVersion, state qviews.QueryViewState) SyncerViewEventBase {
	return SyncerViewEventBase{
		EventBase: NewEventBase(shardID),
		version:   version,
		state:     state,
	}
}

type SyncerViewEventBase struct {
	EventBase
	version QueryViewVersion
	state   QueryViewState
}

func (s SyncerViewEventBase) isSyncerEvent() {}

func (s SyncerViewEventBase) Version() QueryViewVersion {
	return s.version
}

func (s SyncerViewEventBase) State() QueryViewState {
	return s.state
}

type SyncerEventSent struct {
	SyncerViewEventBase
}

func (s SyncerEventSent) EventType() EventType {
	return EventTypeSyncSent
}

type SyncerEventOverwrite struct {
	SyncerViewEventBase
	PreviousState QueryViewState
}

func (s SyncerEventOverwrite) EventType() EventType {
	return EventTypeSyncOverwrite
}

type SyncerEventAck struct {
	SyncerViewEventBase
	AcknowledgedView qviews.QueryViewAtWorkNode
}

func (s SyncerEventAck) EventType() EventType {
	return EventTypeSyncAck
}

type SyncerEventBalanceAttrUpdate struct {
	EventBase
	BalanceAttr qviews.BalanceAttrAtWorkNode
}

func (s SyncerEventBalanceAttrUpdate) isSyncerEvent() {}

func (s SyncerEventBalanceAttrUpdate) EventType() EventType {
	return EventTypeBalanceAttrUpdate
}
