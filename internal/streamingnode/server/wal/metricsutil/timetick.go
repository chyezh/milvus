package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

// TimeTickMetrics is the metrics for time tick.
type TimeTickMetrics struct {
	mu                               syncutil.ClosableLock
	constLabel                       prometheus.Labels
	allocatedTimeTickCounter         prometheus.Counter
	acknowledgedTimeTickCounter      *prometheus.CounterVec
	syncTimeTickCounter              *prometheus.CounterVec
	lastAllocatedTimeTick            prometheus.Gauge
	lastConfirmedTimeTick            prometheus.Gauge
	persistentTimeTickSyncCounter    prometheus.Counter
	persistentTimeTickSync           prometheus.Gauge
	nonPersistentTimeTickSyncCounter prometheus.Counter
	nonPersistentTimeTickSync        prometheus.Gauge
}

// NewTimeTickMetrics creates a new time tick metrics.
func NewTimeTickMetrics(pchannel string) *TimeTickMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	return &TimeTickMetrics{
		mu:                               syncutil.ClosableLock{},
		constLabel:                       constLabel,
		allocatedTimeTickCounter:         metrics.WALAllocateTimeTickTotal.With(constLabel),
		acknowledgedTimeTickCounter:      metrics.WALAcknowledgeTimeTickTotal.MustCurryWith(constLabel),
		syncTimeTickCounter:              metrics.WALSyncTimeTickTotal.MustCurryWith(constLabel),
		lastAllocatedTimeTick:            metrics.WALLastAllocatedTimeTick.With(constLabel),
		lastConfirmedTimeTick:            metrics.WALLastConfirmedTimeTick.With(constLabel),
		persistentTimeTickSyncCounter:    metrics.WALTimeTickSyncTotal.MustCurryWith(constLabel).WithLabelValues("persistent"),
		persistentTimeTickSync:           metrics.WALTimeTickSyncTimeTick.MustCurryWith(constLabel).WithLabelValues("persistent"),
		nonPersistentTimeTickSyncCounter: metrics.WALTimeTickSyncTotal.MustCurryWith(constLabel).WithLabelValues("memory"),
		nonPersistentTimeTickSync:        metrics.WALTimeTickSyncTimeTick.MustCurryWith(constLabel).WithLabelValues("memory"),
	}
}

func (m *TimeTickMetrics) CountAllocateTimeTick(ts uint64) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.allocatedTimeTickCounter.Inc()
	m.lastAllocatedTimeTick.Set(tsoutil.PhysicalTimeSeconds(ts))
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountAcknowledgeTimeTick(control string) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.acknowledgedTimeTickCounter.WithLabelValues(control).Inc()
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountSyncTimeTick(control string) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.syncTimeTickCounter.WithLabelValues(control).Inc()
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountMemoryTimeTickSync(ts uint64) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.nonPersistentTimeTickSyncCounter.Inc()
	m.nonPersistentTimeTickSync.Set(tsoutil.PhysicalTimeSeconds(ts))
	m.mu.Unlock()
}

func (m *TimeTickMetrics) CountPersistentTimeTickSync(ts uint64) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.persistentTimeTickSyncCounter.Inc()
	m.persistentTimeTickSync.Set(tsoutil.PhysicalTimeSeconds(ts))
	m.mu.Unlock()
}

func (m *TimeTickMetrics) UpdateLastConfirmedTimeTick(ts uint64) {
	if !m.mu.LockIfNotClosed() {
		return
	}
	m.lastConfirmedTimeTick.Set(tsoutil.PhysicalTimeSeconds(ts))
	m.mu.Unlock()
}

func (m *TimeTickMetrics) Close() {
	// mark as closed and delete all labeled metrics
	m.mu.Close()
	metrics.WALAllocateTimeTickTotal.Delete(m.constLabel)
	metrics.WALLastAllocatedTimeTick.Delete(m.constLabel)
	metrics.WALLastConfirmedTimeTick.Delete(m.constLabel)
	metrics.WALAcknowledgeTimeTickTotal.DeletePartialMatch(m.constLabel)
	metrics.WALSyncTimeTickTotal.DeletePartialMatch(m.constLabel)
	metrics.WALTimeTickSyncTimeTick.DeletePartialMatch(m.constLabel)
	metrics.WALTimeTickSyncTotal.DeletePartialMatch(m.constLabel)
}
