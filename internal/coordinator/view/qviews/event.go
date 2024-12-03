package qviews

type EventType int

const (
	EventTypeJoin EventType = iota
)

type Event interface{}

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
