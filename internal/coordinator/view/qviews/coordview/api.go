package coordview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// apiImplementation is the interface to implement the api operation.
// When new api is sent to QueryViewManager, the QueryViewManager will call the Apply method to apply the operation.
// If Apply operation is successful, the QueryViewManager will put the api into the event observer.
// Otherwise, the apply error will returned by Result Future.
type apiImplementation interface {
	EventObserver

	// Apply make a apply opeartion into the QueryViewManager.
	// If returns nil, the QueryViewManager will not put the api into the event watcher.
	Apply(*QueryViewManager) error
}

// newApplyAPI creates a new incomingQueryViewAPI.
func newApplyAPI(newIncomingQV *QueryViewAtCoordBuilder) (apiImplementation, *syncutil.Future[ApplyResult]) {
	result := syncutil.NewFuture[ApplyResult]()
	return &applyAPI{
		newIncomingQV: newIncomingQV,
		result:        result,
	}, result
}

// ApplyResult is the result of the apply operation.
type ApplyResult struct {
	Version *qviews.QueryViewVersion
	Err     error
}

// applyAPI is the api to apply the new incoming query view.
type applyAPI struct {
	shardID       qviews.ShardID
	newIncomingQV *QueryViewAtCoordBuilder
	version       *qviews.QueryViewVersion
	result        *syncutil.Future[ApplyResult]
}

// Apply applies the new incoming query view to the QueryViewManager.
func (api *applyAPI) Apply(qvm *QueryViewManager) error {
	api.shardID = api.newIncomingQV.ShardID()
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
		ev, ok := ev.(events.EventRecoverySwap)
		if !ok {
			continue
		}
		if ev.ShardID() != api.shardID {
			continue
		}
		api.result.Set(ApplyResult{Version: api.version})
		return false
	}
	return true
}

// newAddReplicasAPI creates a new AddReplicasAPI.
func newAddReplicasAPI(req AddReplicasRequest) (apiImplementation, *syncutil.Future[AddReplicasResult]) {
	result := syncutil.NewFuture[AddReplicasResult]()
	return &apiAddReplicas{
		req:    req,
		result: result,
	}, result
}

// AddReplicasRequest is the request to add replicas to the QueryViewManager.
type AddReplicasRequest struct {
	Shards []AddShardRequest
}

// AddShardRequest is the settings for the replica.
type AddShardRequest struct {
	ShardID  qviews.ShardID
	Settings *viewpb.QueryViewSettings
}

// AddReplicasResult is the result of the add replicas operation.
type AddReplicasResult struct {
	Results []AddShardResult
}

// AddShardResult is the result of the add replica operation.
type AddShardResult struct {
	UpVersions *qviews.QueryViewVersion
	Err        error
}

// apiAddReplicas is the api to add replicas to the QueryViewManager.
type apiAddReplicas struct {
	req        AddReplicasRequest
	upVersions []*qviews.QueryViewVersion
	readys     int
	result     *syncutil.Future[AddReplicasResult]
}

func (api *apiAddReplicas) Apply(qvm *QueryViewManager) error {
	api.upVersions = qvm.addShards(api.req)
	for _, version := range api.upVersions {
		if version != nil {
			api.readys++
		}
	}
	if api.checkIfAllShardUp() {
		api.result.Set(AddReplicasResult{Results: api.getResults()})
		api.result = nil
	}
	return nil
}

func (api *apiAddReplicas) Observe(evs ...events.Event) bool {
	if api.result == nil {
		// The result may be setup by the Apply method.
		return false
	}
	for _, ev := range evs {
		if ev.EventType() != events.EventTypeStateTransition {
			continue
		}

		ev := ev.(events.StateTransitionEvent)
		for idx, shard := range api.req.Shards {
			if api.upVersions[idx] == nil && shard.ShardID == ev.ShardID() && ev.Transition.To == qviews.QueryViewStateUp {
				api.upVersions[idx] = &ev.Version
				api.readys++
			}
		}
		if api.checkIfAllShardUp() {
			api.result.Set(AddReplicasResult{Results: api.getResults()})
			return false
		}
	}
	return true
}

// checkIfAllShardUp checks if all the shards are up.
func (api *apiAddReplicas) checkIfAllShardUp() bool {
	return api.readys == len(api.upVersions)
}

func (api *apiAddReplicas) getResults() []AddShardResult {
	results := make([]AddShardResult, 0, len(api.req.Shards))
	for _, version := range api.upVersions {
		results = append(results, AddShardResult{
			UpVersions: version,
		})
	}
	return results
}

func newReleaseReplicasAPI(req ReleaseReplicasRequest) (apiImplementation, *syncutil.Future[ReleaseReplicasResult]) {
	result := syncutil.NewFuture[ReleaseReplicasResult]()
	return &apiReleaseReplicas{
		req:    req,
		result: result,
	}, result
}

type ReleaseReplicasRequest struct {
	ReplicaIDs []int64
}

type ReleaseReplicasResult struct{}

type apiReleaseReplicas struct {
	req       ReleaseReplicasRequest
	releasing []qviews.ShardID
	result    *syncutil.Future[ReleaseReplicasResult]
}

func (api *apiReleaseReplicas) Apply(qvm *QueryViewManager) error {
	api.releasing = qvm.releaseReplicas(api.req)
	if api.checkIfAllShardReleased() {
		api.result.Set(ReleaseReplicasResult{})
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
		if ev.EventType() != events.EventTypeStateTransition {
			continue
		}

		ev := ev.(events.StateTransitionEvent)
		for idx, shard := range api.releasing {
			if shard == ev.ShardID() && ev.Transition.To == qviews.QueryViewStateDropped {
				api.releasing = append(api.releasing[:idx], api.releasing[idx+1:]...)
				continue
			}
		}
		if api.checkIfAllShardReleased() {
			api.result.Set(ReleaseReplicasResult{})
			return false
		}
	}
	return true
}

func (api *apiReleaseReplicas) checkIfAllShardReleased() bool {
	return len(api.releasing) == 0
}
