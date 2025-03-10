package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// apiImplementation is the interface to implement the api operation.
// When new api is sent to QueryViewManager, the QueryViewManager will call the Apply method to apply the operation.
// If Apply operation is successful, the QueryViewManager will put the api into the event observer.
// Otherwise, the apply error will returned by Result Future.
type apiImplementation interface {
	EventObserver

	// Apply make a apply opeartion into the QueryViewManager.
	// If returns nil, the QueryViewManager will not put the api into the event watcher.
	Apply(*queryViewManagerImpl) error
}

// newApplyAPI creates a new incomingQueryViewAPI.
func newApplyAPI(newIncomingQV *QueryViewAtCoordBuilder) (apiImplementation, *syncutil.Future[ApplyResult]) {
	result := syncutil.NewFuture[ApplyResult]()
	return &applyAPI{
		shardID:       newIncomingQV.ShardID(),
		newIncomingQV: newIncomingQV,
		result:        result,
	}, result
}

// applyAPI is the api to apply the new incoming query view.
type applyAPI struct {
	// request field
	shardID       qviews.ShardID
	newIncomingQV *QueryViewAtCoordBuilder

	// result field
	version *qviews.QueryViewVersion
	result  *syncutil.Future[ApplyResult]
}

// Apply applies the new incoming query view to the QueryViewManager.
func (api *applyAPI) Apply(qvm *queryViewManagerImpl) error {
	version, err := qvm.apply(api.newIncomingQV)
	api.newIncomingQV = nil
	if err != nil {
		api.result.Set(ApplyResult{Err: err})
		return err
	}
	api.version = version
	return nil
}

// Observe blocks until the newIncomingQueryView is persisted into recovery info.
func (api *applyAPI) Observe(evs ...events.Event) bool {
	if api.version == nil {
		panic("the version should never be nil when doing the event check")
	}
	for _, ev := range evs {
		ev, ok := ev.(events.RecoveryEventPersisted)
		if !ok {
			continue
		}
		for _, item := range ev.Persisted {
			if item.ShardID == api.shardID && item.Version == *api.version {
				api.result.Set(ApplyResult{Version: api.version})
				return false
			}
		}
	}
	return true
}

// newReleaseShardsAPI creates a new releaseShardsAPI.
func newReleaseShardsAPI(req ReleaseShardsRequest) (apiImplementation, *syncutil.Future[ReleaseShardsResult]) {
	result := syncutil.NewFuture[ReleaseShardsResult]()
	return &apiReleaseReplicas{
		req:    req,
		result: result,
	}, result
}

// releaseAPI is the api to release the query view.
type apiReleaseReplicas struct {
	req       ReleaseShardsRequest
	releasing []qviews.ShardID
	result    *syncutil.Future[ReleaseShardsResult]
}

func (api *apiReleaseReplicas) Apply(qvm *queryViewManagerImpl) error {
	api.releasing = qvm.releaseReplicas(api.req)
	if api.checkIfAllShardReleased() {
		api.result.Set(ReleaseShardsResult{})
		api.result = nil
		return nil
	}
	return nil
}

func (api *apiReleaseReplicas) Observe(evs ...events.Event) bool {
	if api.result == nil {
		return false
	}
	for _, ev := range evs {
		ev, ok := ev.(events.RecoveryEventPersisted)
		if !ok {
			continue
		}
		for _, item := range ev.Persisted {
			if item.State != qviews.QueryViewStateDropped {
				continue
			}
			for idx, shard := range api.releasing {
				if shard == item.ShardID && item.Version == *api.version {
					api.releasing = append(api.releasing[:idx], api.releasing[idx+1:]...)
					continue
				}
			}

			if item.ShardID == api.shardID && item.Version == *api.version {
				api.result.Set(ApplyResult{Version: api.version})
				return false
			}
		}

		ev := ev.(events.EventStateTransition)
		for idx, shard := range api.releasing {
			if shard == ev.ShardID && ev.Transition.To == qviews.QueryViewStateDropped {
			}
		}
		if api.checkIfAllShardReleased() {
			api.result.Set(ReleaseShardsResult{})
			return false
		}
	}
	return true
}

func (api *apiReleaseReplicas) checkIfAllShardReleased() bool {
	return len(api.releasing) == 0
}
