package events

// StateTransitionEvent is the event for state transition.
type StateTransitionEvent struct {
	EventBase
	Transition StateTransition
	Version    QueryViewVersion
}

// EventType returns the event type.
func (StateTransitionEvent) EventType() EventType {
	return EventTypeStateTransition
}
