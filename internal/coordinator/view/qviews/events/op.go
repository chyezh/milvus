package events

type EventShardJoin struct {
	EventBase
}

func (EventShardJoin) EventType() EventType {
	return EventTypeShardJoin
}

type EventShardReady struct {
	EventBase
}

func (EventShardReady) EventType() EventType {
	return EventTypeShardReady
}

type EventShardRequestRelease struct {
	EventBase
}

func (EventShardRequestRelease) EventType() EventType {
	return EventTypeShardRequestRelease
}

type EventShardReleaseDone struct {
	EventBase
}

func (EventShardReleaseDone) EventType() EventType {
	return EventTypeShardReleaseDone
}

type EventQVApply struct {
	EventBase
	Version QueryViewVersion
}

func (EventQVApply) EventType() EventType {
	return EventTypeQVApply
}
