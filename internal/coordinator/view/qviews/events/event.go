package events

import (
	"time"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

type (
	EventType        int
	ShardID          = qviews.ShardID
	QueryViewVersion = qviews.QueryViewVersion
	QueryViewState   = qviews.QueryViewState
	StateTransition  = qviews.StateTransition
)

const (
	EventTypeShardJoin           EventType = iota // The event for a new shard join.
	EventTypeShardReady                           // The event for the first view of shard up.
	EventTypeShardRequestRelease                  // The event for shard request release.
	EventTypeShardReleaseDone                     // The event for shard release done.
	EventTypeQVApply                              // The event for query view apply.

	EventTypeRecoverySwap      // The event for recovery swap.
	EventTypeRecoverySaveNewUp // The event for recovery save new up.
	EventTypeRecoverySave      // The event for recovery save.
	EventTypeRecoveryDelete    // The event for recovery delete.

	EventTypeStateTransition // The event for state transition.

	EventTypeSyncSent      // The event for sync, when sync operation is sent.
	EventTypeSyncOverwrite // The event for overwrite, when the acknowledge or sync is not done but the state is transitted.
	EventTypeSyncAck       // The event for acked, when first acknowledge returned.
	EventTypeReport        // The event for report, when the query view is reported.
)

var (
	_ Event = StateTransitionEvent{}

	_ RecoveryEvent = EventRecoverySwap{}
	_ RecoveryEvent = EventRecoverySaveNewUp{}
	_ RecoveryEvent = EventRecoverySave{}
	_ RecoveryEvent = EventRecoveryDelete{}

	_ SyncerEvent = SyncerEventSent{}
	_ SyncerEvent = SyncerEventOverwrite{}
	_ SyncerEvent = SyncerEventAck{}
)

// Event is the common interface for all events that can be watched.
type Event interface {
	// ShardID returns the related shard id of the event.
	ShardID() ShardID

	// EventType returns the type of the event.
	EventType() EventType

	// The instant that the event happens.
	Instant() time.Time
}

// SyncerEvent is the interface for syncer events.
type SyncerEvent interface {
	Event

	isSyncerEvent()

	// Version returns the version of the event.
	Version() QueryViewVersion

	// State returns the state of the event.
	State() QueryViewState
}

// RecoveryEvent is the interface for recovery events.
type RecoveryEvent interface {
	Event

	isRecoveryEvent()
}

func (t EventType) String() string {
	switch t {
	case EventTypeShardJoin:
		return "ShardJoin"
	case EventTypeShardReady:
		return "ShardReady"
	case EventTypeShardRequestRelease:
		return "ShardRequestRelease"
	case EventTypeShardReleaseDone:
		return "ShardReleaseDone"
	case EventTypeRecoverySwap:
		return "RecoverySwap"
	case EventTypeRecoverySaveNewUp:
		return "RecoverySaveNewUp"
	case EventTypeRecoverySave:
		return "RecoverySave"
	case EventTypeRecoveryDelete:
		return "RecoveryDelete"
	case EventTypeStateTransition:
		return "StateTransition"
	case EventTypeSyncSent:
		return "SyncSent"
	case EventTypeSyncOverwrite:
		return "SyncOverwrite"
	case EventTypeSyncAck:
		return "SyncAck"
	default:
		panic("unknown event type")
	}
}

// NewEventBaseFromQVMeta creates a new event base from the query view meta.
func NewEventBaseFromQVMeta(meta *viewpb.QueryViewMeta) EventBase {
	return NewEventBase(qviews.NewShardIDFromQVMeta(meta))
}

// NewEventBase creates a new event base.
func NewEventBase(shardID ShardID) EventBase {
	return EventBase{
		shardID: shardID,
		instant: time.Now(),
	}
}

// EventBase is the basic event implementation.
type EventBase struct {
	shardID ShardID
	instant time.Time
}

// ShardID returns the shard id of the event.
func (eb EventBase) ShardID() ShardID {
	return eb.shardID
}

// Instant returns the instant that the event happens.
func (eb EventBase) Instant() time.Time {
	return eb.instant
}
