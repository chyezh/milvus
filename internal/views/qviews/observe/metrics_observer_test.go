package observe

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestMetricsObserverTracksCoordViewStateTotal(t *testing.T) {
	metrics.QVViewStateTotal.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		View:  view,
		State: qviews.QueryViewStatePreparing,
	})

	assertGaugeValue(t, metrics.QVViewStateTotal, 1, "coord", qviews.QueryViewStatePreparing.String())

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})

	assertGaugeValue(t, metrics.QVViewStateTotal, 0, "coord", qviews.QueryViewStatePreparing.String())
	assertGaugeValue(t, metrics.QVViewStateTotal, 1, "coord", qviews.QueryViewStateReady.String())
}

func TestMetricsObserverCountsCoordViewTransitions(t *testing.T) {
	metrics.QVViewStateTotal.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewReportAppliedEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReportedState:        qviews.QueryViewStateReady,
		ResourceReadyPercent: 100,
	})

	assertCounterValue(
		t,
		metrics.QVViewTransitionTotal,
		1,
		"coord",
		qviews.QueryViewStatePreparing.String(),
		qviews.QueryViewStateReady.String(),
		"reportReady",
	)
}

func TestMetricsObserverSeparatesStateTotalByComponent(t *testing.T) {
	metrics.QVViewStateTotal.Reset()
	metrics.QVViewTransitionTotal.Reset()
	observer := NewMetricsObserver()
	view := testQueryViewKey()

	observer.Observe(context.Background(), CoordViewCreatedEvent{
		View:  view,
		State: qviews.QueryViewStatePreparing,
	})
	observer.Observe(context.Background(), QueryNodeSegmentsReadyEvent{
		ViewStateTransition: ViewStateTransition{
			View: view,
			From: qviews.QueryViewStatePreparing,
			To:   qviews.QueryViewStateReady,
		},
		ReadySegmentCount: 10,
	})

	assertGaugeValue(t, metrics.QVViewStateTotal, 1, "coord", qviews.QueryViewStatePreparing.String())
	assertGaugeValue(t, metrics.QVViewStateTotal, 1, "queryNode", qviews.QueryViewStateReady.String())
}

func assertGaugeValue(t *testing.T, collector *prometheus.GaugeVec, expected float64, labels ...string) {
	t.Helper()
	got := testutil.ToFloat64(collector.WithLabelValues(labels...))
	if got != expected {
		t.Fatalf("gauge labels %v = %v, want %v", labels, got, expected)
	}
}

func assertCounterValue(t *testing.T, collector *prometheus.CounterVec, expected float64, labels ...string) {
	t.Helper()
	got := testutil.ToFloat64(collector.WithLabelValues(labels...))
	if got != expected {
		t.Fatalf("counter labels %v = %v, want %v", labels, got, expected)
	}
}
