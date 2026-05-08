package recovery

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const (
	scannerTaskStateRecovering scannerTaskState = "in_recovery"
	scannerTaskStateWorking    scannerTaskState = "working"
	scannerTaskStateClosing    scannerTaskState = "closing"
)

type scannerTaskState = string

func newRecoveryStorageMetrics(channelInfo types.PChannelInfo) *recoveryMetrics {
	constLabels := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     channelInfo.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(channelInfo.Term, 10),
	}
	return &recoveryMetrics{
		constLabels:            constLabels,
		info:                   metrics.WALRecoveryInfo.MustCurryWith(constLabels),
		inconsistentEventTotal: metrics.WALRecoveryInconsistentEventTotal.With(constLabels),
		isOnPersisting:         metrics.WALRecoveryIsOnPersisting.With(constLabels),
		inMemTimeTick:          metrics.WALRecoveryInMemTimeTick.With(constLabels),
		persistedTimeTick:      metrics.WALRecoveryPersistedTimeTick.With(constLabels),
	}
}

func newScannerTaskMetrics(channelInfo types.PChannelInfo) *scannerTaskMetrics {
	constLabels := prometheus.Labels{
		metrics.NodeIDLabelName:         paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName:     channelInfo.Name,
		metrics.WALChannelTermLabelName: strconv.FormatInt(channelInfo.Term, 10),
	}
	m := &scannerTaskMetrics{
		constLabels: constLabels,
		// Keep existing Prometheus metric names for compatibility; this task used to be
		// implemented by the WAL flusher.
		info:     metrics.WALFlusherInfo.MustCurryWith(constLabels),
		timetick: metrics.WALFlusherTimeTick.With(constLabels),
		state:    scannerTaskStateRecovering,
	}
	m.info.WithLabelValues(scannerTaskStateRecovering).Set(1)
	return m
}

type recoveryMetrics struct {
	constLabels            prometheus.Labels
	info                   *prometheus.GaugeVec
	inconsistentEventTotal prometheus.Counter
	isOnPersisting         prometheus.Gauge
	inMemTimeTick          prometheus.Gauge
	persistedTimeTick      prometheus.Gauge
}

type scannerTaskMetrics struct {
	constLabels prometheus.Labels
	info        *prometheus.GaugeVec
	timetick    prometheus.Gauge
	state       scannerTaskState
}

// ObserveStateChange sets the state of the recovery storage metrics.
func (m *recoveryMetrics) ObserveStateChange(state string) {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	m.info.WithLabelValues(state).Set(1)
}

func (m *recoveryMetrics) ObServeInMemMetrics(tickTime uint64) {
	m.inMemTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObServePersistedMetrics(tickTime uint64) {
	m.persistedTimeTick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *recoveryMetrics) ObserveInconsitentEvent() {
	m.inconsistentEventTotal.Inc()
}

func (m *recoveryMetrics) ObserveIsOnPersisting(onPersisting bool) {
	if onPersisting {
		m.isOnPersisting.Set(1)
	} else {
		m.isOnPersisting.Set(0)
	}
}

func (m *scannerTaskMetrics) IntoState(state scannerTaskState) {
	metrics.WALFlusherInfo.DeletePartialMatch(m.constLabels)
	m.state = state
	m.info.WithLabelValues(m.state).Set(1)
}

func (m *scannerTaskMetrics) ObserveMetrics(tickTime uint64) {
	m.timetick.Set(tsoutil.PhysicalTimeSeconds(tickTime))
}

func (m *scannerTaskMetrics) Close() {
	metrics.WALFlusherInfo.DeletePartialMatch(m.constLabels)
	metrics.WALFlusherTimeTick.DeletePartialMatch(m.constLabels)
}

func (m *recoveryMetrics) Close() {
	metrics.WALRecoveryInfo.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInconsistentEventTotal.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryIsOnPersisting.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryInMemTimeTick.DeletePartialMatch(m.constLabels)
	metrics.WALRecoveryPersistedTimeTick.DeletePartialMatch(m.constLabels)
}
