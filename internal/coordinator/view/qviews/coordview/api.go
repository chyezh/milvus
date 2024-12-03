package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

type api struct {
	applier   func(*QueryViewManager)
	condition func(*QueryViewManager) bool
	result    *syncutil.Future[error]
}

// newIncomingQueryViewAPI creates a new incoming query view API.
func newIncomingQueryViewAPI() *api {
	return &api{
		applier: func(qvm *QueryViewManager) {
		},
		condition: func(qvm *QueryViewManager) bool {
			return true
		},
		result: syncutil.NewFuture[error](),
	}
}

// newReplicaAPI creates a new replica API.
func newReplicaAPI() *api {
	return &api{
		applier: func(qvm *QueryViewManager) {
			qvm.replicas = make(map[int64]qviews.ShardID)
		},
		condition: func(qvm *QueryViewManager) bool {
			return true
		},
		result: syncutil.NewFuture[error](),
	}
}
