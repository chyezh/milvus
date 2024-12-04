package qviews

type EventType int

const (
	EventTypeJoin            EventType = iota // The event for a new shard join the queryview.
	EventTypeRelease                          // The event for the shard leaving the queryview.
	EventTypeRecoveryWrite                    // The event for recovery write.
	EventTypeStateTransition                  // The event for state transition.
	EventTypeSync                             // The event for sync.
)

// Event is the common interface for all events that should be seen by the watcher.
type Event interface {
	EventType() EventType

	GetShardID()
}

// EventTypeJoin marks there's a new shard join.
type EventJoin struct {
	ShardID ShardID
}

// EventTypeUp marks the previous applied query view is up.
type EventTypeUp struct {
	ShardID         ShardID
	LatestUpVersion QueryViewVersion
}

// EventTypeUnrecoverable marks the previous applied query view is unrecoverable.
type EventTypeUnrecoverable struct {
	ShardID         ShardID
	LatestUpVersion QueryViewVersion
}
