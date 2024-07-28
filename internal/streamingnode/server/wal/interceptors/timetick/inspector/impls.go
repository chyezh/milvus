package inspector

import (
	"time"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// NewTimeTickSyncInspector creates a new time tick sync inspector.
func NewTimeTickSyncInspector() TimeTickSyncInspector {
	return &timeTickSyncInspectorImpl{
		taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		syncNotifier: NewSyncNotifier(),
		operators:    typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
	}
}

type timeTickSyncInspectorImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	syncNotifier *SyncNotifier
	operators    *typeutil.ConcurrentMap[string, TimeTickSyncOperator]
}

func (s *timeTickSyncInspectorImpl) RegisterSyncOperator(operator TimeTickSyncOperator) {
	operator.SetNotifier(s.syncNotifier)
	_, loaded := s.operators.GetOrInsert(operator.Channel().Name, operator)
	if loaded {
		panic("sync operator already exists, critical bug in code")
	}
}

func (s *timeTickSyncInspectorImpl) UnregisterSyncOperator(operator TimeTickSyncOperator) {
	_, loaded := s.operators.GetAndRemove(operator.Channel().Name)
	if !loaded {
		panic("sync operator not found, critical bug in code")
	}
}

// execute executes the time tick sync inspector.
func (s *timeTickSyncInspectorImpl) execute() {
	defer s.taskNotifier.Finish(struct{}{})

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-s.taskNotifier.Context().Done():
			return
		case <-ticker.C:
			s.operators.Range(func(_ string, operator TimeTickSyncOperator) bool {
				operator.Sync(s.taskNotifier.Context())
				return true
			})
		case <-s.syncNotifier.WaitChan():
			s.syncNotifier.Get().Range(func(pchannel types.PChannelInfo) bool {
				if operator, ok := s.operators.Get(pchannel.Name); ok {
					operator.Sync(s.taskNotifier.Context())
				}
				return true
			})
		}
	}
}

func (s *timeTickSyncInspectorImpl) Close() {
	s.taskNotifier.Cancel()
	s.taskNotifier.BlockUntilFinish()
}
