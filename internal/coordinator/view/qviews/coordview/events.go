package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// EventObserver is the observer to observe the event.
// All Event in QueryViewManager can be seen by the observer sequentially.
type EventObserver interface {
	// Observe will be called when the event happens.
	// When observe return false, the observer will be removed from the observer list.
	Observe(e ...events.Event) bool
}

// newEventObservers creates a new EventObservers.
func newEventObservers() *eventObservers {
	return &eventObservers{
		observers: map[EventObserver]struct{}{
			logAndMetricObserver{}: {},
		},
	}
}

// eventObservers is the registration of the observer.
type eventObservers struct {
	observers map[EventObserver]struct{}
}

// Register is the method to register the observer.
func (l *eventObservers) Register(o EventObserver) {
	l.observers[o] = struct{}{}
}

// Observe is the method to observe the event.
func (l *eventObservers) Observe(e ...events.Event) {
	for o := range l.observers {
		if !o.Observe(e...) {
			delete(l.observers, o)
		}
	}
}

// logAndMetricObserver is the observer to log and metric the event.
type logAndMetricObserver struct{}

// Observe is the method to log and metric the event.
func (l logAndMetricObserver) Observe(evs ...events.Event) bool {
	// TOOD: log and metric the event.
	for _, ev := range evs {
		log.Info("QueryViewManager event: %v", zap.Any("event", ev))
	}
	return true
}
