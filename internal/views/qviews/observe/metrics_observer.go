package observe

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

type MetricsObserver struct {
	mu     sync.Mutex
	states map[metricViewKey]qviews.QueryViewState
}

type metricViewKey struct {
	component string
	view      qviews.QueryViewKey
}

func NewMetricsObserver() *MetricsObserver {
	return &MetricsObserver{
		states: make(map[metricViewKey]qviews.QueryViewState),
	}
}

func (o *MetricsObserver) Observe(_ context.Context, event Event) {
	component := event.ComponentInfo()
	if component == "" {
		return
	}
	if created, ok := event.(CoordViewCreatedEvent); ok {
		o.setState(component, created.View, created.State)
		return
	}
	if persisted, ok := event.(CoordPersistViewEvent); ok && persisted.State == qviews.QueryViewStateDropped {
		o.deleteState(component, persisted.View)
	}

	transition, ok := metricTransition(event)
	if !ok {
		return
	}
	trigger := event.TriggerInfo()
	if trigger == "" {
		return
	}
	metrics.QVViewTransitionTotal.WithLabelValues(
		component,
		transition.From.String(),
		transition.To.String(),
		trigger,
	).Inc()
	o.moveState(component, transition.View, transition.To)
}

func (o *MetricsObserver) setState(component string, view qviews.QueryViewKey, state qviews.QueryViewState) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	if old, ok := o.states[key]; ok {
		metrics.QVViewStateTotal.WithLabelValues(component, old.String()).Dec()
	}
	o.states[key] = state
	metrics.QVViewStateTotal.WithLabelValues(component, state.String()).Inc()
}

func (o *MetricsObserver) moveState(component string, view qviews.QueryViewKey, to qviews.QueryViewState) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	if old, ok := o.states[key]; ok {
		metrics.QVViewStateTotal.WithLabelValues(component, old.String()).Dec()
	}
	o.states[key] = to
	metrics.QVViewStateTotal.WithLabelValues(component, to.String()).Inc()
}

func (o *MetricsObserver) deleteState(component string, view qviews.QueryViewKey) {
	o.mu.Lock()
	defer o.mu.Unlock()

	key := metricViewKey{component: component, view: view}
	old, ok := o.states[key]
	if !ok {
		return
	}
	delete(o.states, key)
	metrics.QVViewStateTotal.WithLabelValues(component, old.String()).Dec()
}

func metricTransition(event Event) (ViewStateTransition, bool) {
	switch e := event.(type) {
	case CoordViewPreemptedEvent:
		return e.ViewStateTransition, true
	case CoordViewAdvancedFromUnrecoverableEvent:
		return e.ViewStateTransition, true
	case CoordViewReleaseRequestedEvent:
		return e.ViewStateTransition, true
	case CoordViewHandoffToNewUpEvent:
		return e.ViewStateTransition, true
	case CoordViewReportAppliedEvent:
		return e.ViewStateTransition, true
	case CoordViewQueryNodeLostAppliedEvent:
		return e.ViewStateTransition, true
	case QueryNodeSegmentsReadyEvent:
		return e.ViewStateTransition, true
	case QueryNodeSegmentUnrecoverableEvent:
		return e.ViewStateTransition, true
	case QueryNodeReleaseDoneEvent:
		return e.ViewStateTransition, true
	case StreamingNodeResourceReadyEvent:
		return e.ViewStateTransition, true
	case StreamingNodeRecoveringDoneEvent:
		return e.ViewStateTransition, true
	case StreamingNodeReleaseDoneEvent:
		return e.ViewStateTransition, true
	default:
		return ViewStateTransition{}, false
	}
}
