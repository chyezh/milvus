package recovery

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// pendingTask is the request to execute the task.
type pendingTask struct {
	saves    map[string]string
	removals []string
	event    events.RecoveryEvent
}

func (r *RecoveryImpl) newSwapPreparingTask(shardID qviews.ShardID, old *viewpb.QueryViewOfShard, new *viewpb.QueryViewOfShard) *pendingTask {
	saves := make(map[string]string, 2)
	removals := make([]string, 0, 1)
	if old != nil {
		// If old is not nil, it must be persisted into the history path.
		if old.Meta.State != viewpb.QueryViewState_QueryViewStateUnrecoverable {
			panic("old view state must be unrecoverable")
		}
		saves[r.getHistoryPath(old.Meta)] = mustMarshal(old)
	}
	if new != nil {
		// If new is not nil, it must be persisted into the preparing path.
		if new.Meta.State != viewpb.QueryViewState_QueryViewStatePreparing {
			panic("new view state must be preparing")
		}
		saves[r.getPreparingPath(shardID)] = mustMarshal(new)
	} else {
		// Otherwise, the preparing view should be removed.
		removals = append(removals, r.getPreparingPath(shardID))
	}
	return &pendingTask{
		saves:    saves,
		removals: removals,
		event: events.EventRecoverySwap{
			RecoveryEventBase: events.NewRecoveryEventBaseFromQVMeta(new.Meta),
			OldVersion:        nil,
			NewView:           new,
		},
	}
}

func (r *RecoveryImpl) newUpNewPreparingView(newUp *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		saves: map[string]string{
			r.getHistoryPath(newUp.Meta): mustMarshal(newUp),
		},
		removals: []string{
			r.getPreparingPath(qviews.NewShardIDFromQVMeta(newUp.Meta)),
		},
		event: events.EventRecoverySaveNewUp{
			RecoveryEventBase: events.NewRecoveryEventBaseFromQVMeta(newUp.Meta),
			Version:           qviews.FromProtoQueryViewVersion(newUp.Meta.Version),
		},
	}
}

func (r *RecoveryImpl) newSave(newSave *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		saves: map[string]string{
			r.getHistoryPath(newSave.Meta): mustMarshal(newSave),
		},
		event: events.EventRecoverySave{
			RecoveryEventBase: events.NewRecoveryEventBaseFromQVMeta(newSave.Meta),
			Version:           qviews.FromProtoQueryViewVersion(newSave.Meta.Version),
			State:             qviews.QueryViewState(newSave.Meta.State),
		},
	}
}

func (r *RecoveryImpl) newDelete(newDelete *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		removals: []string{
			r.getHistoryPath(newDelete.Meta),
		},
		event: events.EventRecoveryDelete{
			RecoveryEventBase: events.NewRecoveryEventBaseFromQVMeta(newDelete.Meta),
			Version:           qviews.FromProtoQueryViewVersion(newDelete.Meta.Version),
		},
	}
}
