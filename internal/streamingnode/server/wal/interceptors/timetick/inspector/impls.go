package inspector

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NewTimeTickSyncInspector creates a new time tick sync inspector.
func NewTimeTickSyncInspector() TimeTickSyncInspector {
	inspector := &timeTickSyncInspectorImpl{
		taskNotifier:      syncutil.NewAsyncTaskNotifier[struct{}](),
		syncNotifier:      newSyncNotifier(),
		operators:         typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
		lastPersistedSync: typeutil.NewConcurrentMap[string, time.Time](),
	}
	go inspector.background()
	return inspector
}

type timeTickSyncInspectorImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	syncNotifier *syncNotifier
	operators    *typeutil.ConcurrentMap[string, TimeTickSyncOperator]
	wg           sync.WaitGroup
	working      typeutil.ConcurrentSet[string]
	// lastPersistedSync records the last time a persisted time tick sync was
	// triggered for each pchannel.
	lastPersistedSync *typeutil.ConcurrentMap[string, time.Time]
}

func (s *timeTickSyncInspectorImpl) TriggerSync(pChannelInfo types.PChannelInfo, persisted bool) {
	s.syncNotifier.AddAndNotify(pChannelInfo, persisted)
}

func (s *timeTickSyncInspectorImpl) GetOperator(pChannelInfo types.PChannelInfo) (TimeTickSyncOperator, bool) {
	return s.operators.Get(pChannelInfo.Name)
}

// MustGetOperator gets the operator by pchannel info.
func (s *timeTickSyncInspectorImpl) MustGetOperator(pChannelInfo types.PChannelInfo) TimeTickSyncOperator {
	operator, ok := s.GetOperator(pChannelInfo)
	if !ok {
		panic("sync operator not found, critical bug in code")
	}
	return operator
}

// RegisterSyncOperator registers a sync operator.
func (s *timeTickSyncInspectorImpl) RegisterSyncOperator(operator TimeTickSyncOperator) {
	mlog.Info(context.TODO(), "RegisterSyncOperator", mlog.String("channel", operator.Channel().Name))
	_, loaded := s.operators.GetOrInsert(operator.Channel().Name, operator)
	if loaded {
		panic("sync operator already exists, critical bug in code")
	}
}

// UnregisterSyncOperator unregisters a sync operator.
func (s *timeTickSyncInspectorImpl) UnregisterSyncOperator(operator TimeTickSyncOperator) {
	mlog.Info(context.TODO(), "UnregisterSyncOperator", mlog.String("channel", operator.Channel().Name))
	_, loaded := s.operators.GetAndRemove(operator.Channel().Name)
	if !loaded {
		panic("sync operator not found, critical bug in code")
	}
}

// background executes the time tick sync inspector.
func (s *timeTickSyncInspectorImpl) background() {
	defer s.taskNotifier.Finish(struct{}{})

	interval := paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond)
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-s.taskNotifier.Context().Done():
			return
		case <-ticker.C:
			// Sync a non-persisted heartbeat for every pchannel on each tick;
			// on top of that, emit a persisted time tick at least every
			// dataNode.segment.syncPeriod so an idle pchannel still refreshes
			// the persisted recovery checkpoint on a bounded cadence (the
			// recovery storage skips non-persisted heartbeats entirely).
			now := time.Now()
			s.operators.Range(func(name string, _ TimeTickSyncOperator) bool {
				s.asyncSync(name, false)
				s.maybeForcePersistedSync(name, now)
				return true
			})
		case <-s.syncNotifier.WaitChan():
			signals := s.syncNotifier.Get()
			for pchannel, persisted := range signals {
				s.asyncSync(pchannel.Name, persisted)
			}
		}
	}
}

// maybeForcePersistedSync emits a persisted time tick sync for a pchannel if
// it has not had one within dataNode.segment.syncPeriod. The recorded time is
// the trigger time; a failed sync is retried on the next tick after the
// interval elapses again. The interval is read from the (refreshable) config on
// every call so a config change takes effect without a restart.
func (s *timeTickSyncInspectorImpl) maybeForcePersistedSync(name string, now time.Time) {
	interval := paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
	if last, ok := s.lastPersistedSync.Get(name); ok && now.Sub(last) < interval {
		return
	}
	s.lastPersistedSync.Insert(name, now)
	s.asyncSync(name, true)
}

// asyncSync syncs the pchannel in a goroutine.
func (s *timeTickSyncInspectorImpl) asyncSync(pchannelName string, persisted bool) {
	if !s.working.Insert(pchannelName) {
		// Check if the sync operation of pchannel is working, if so, skip it.
		return
	}

	s.wg.Add(1)
	go func() {
		defer func() {
			s.wg.Done()
			s.working.Remove(pchannelName)
		}()
		if operator, ok := s.operators.Get(pchannelName); ok {
			operator.Sync(s.taskNotifier.Context(), persisted)
		}
	}()
}

func (s *timeTickSyncInspectorImpl) Close() {
	s.taskNotifier.Cancel()
	s.taskNotifier.BlockUntilFinish()
	s.wg.Wait()
}
