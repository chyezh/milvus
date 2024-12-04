package events

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

type SyncerEventBase struct {
	EventBase
	version QueryViewVersion
	state   QueryViewState
}

func (s SyncerEventBase) isSyncerEvent() {}

func (s SyncerEventBase) Version() QueryViewVersion {
	return s.version
}

func (s SyncerEventBase) State() QueryViewState {
	return s.state
}

type SyncerEventSent struct {
	SyncerEventBase
}

func (s SyncerEventSent) EventType() EventType {
	return EventTypeSyncSent
}

type SyncerEventOverwrite struct {
	SyncerEventBase
	PreviousState QueryViewState
}

func (s SyncerEventOverwrite) EventType() EventType {
	return EventTypeSyncOverwrite
}

type SyncerEventAck struct {
	SyncerEventBase
	AcknowledgedView qviews.QueryViewAtWorkNode
}

func (s SyncerEventAck) EventType() EventType {
	return EventTypeSyncAck
}
