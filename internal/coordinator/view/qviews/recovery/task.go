package recovery

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// pendingTask is the request to execute the task.
type pendingTask struct {
	saves    map[string]string
	removals []string
	event    qviews.RecoveryEvent
}

func (r *RecoveryImpl) newSwapPreparingTask(old *viewpb.QueryViewOfShard, new *viewpb.QueryViewOfShard) *pendingTask {
	if new.Meta.ReplicaId != old.Meta.ReplicaId {
		panic("replica id is not matched")
	}
	if new.Meta.Vchannel != old.Meta.Vchannel {
		panic("vchannel is not matched")
	}
	return &pendingTask{
		saves: map[string]string{
			r.getHistoryPath(old.Meta):   mustMarshal(old),
			r.getPreparingPath(new.Meta): mustMarshal(new),
		},
		event: qviews.RecoveryEventSwapPreparing{
			ShardID:    qviews.NewShardIDFromQVMeta(old.Meta),
			OldVersion: qviews.FromProtoQueryViewVersion(old.Meta.Version),
			NewVersion: qviews.FromProtoQueryViewVersion(new.Meta.Version),
		},
	}
}

func (r *RecoveryImpl) newUpNewPreparingView(newUp *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		saves: map[string]string{
			r.getHistoryPath(newUp.Meta): mustMarshal(newUp),
		},
		removals: []string{
			r.getPreparingPath(newUp.Meta),
		},
		event: qviews.RecoveryEventUpNewPreparingView{
			ShardID: qviews.NewShardIDFromQVMeta(newUp.Meta),
			Version: qviews.FromProtoQueryViewVersion(newUp.Meta.Version),
		},
	}
}

func (r *RecoveryImpl) newSave(newSave *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		saves: map[string]string{
			r.getHistoryPath(newSave.Meta): mustMarshal(newSave),
		},
		event: qviews.RecoveryEventSave{
			ShardID: qviews.NewShardIDFromQVMeta(newSave.Meta),
			Version: qviews.FromProtoQueryViewVersion(newSave.Meta.Version),
		},
	}
}

func (r *RecoveryImpl) newDelete(newDelete *viewpb.QueryViewOfShard) *pendingTask {
	return &pendingTask{
		removals: []string{
			r.getHistoryPath(newDelete.Meta),
		},
		event: qviews.RecoveryEventDelete{
			ShardID: qviews.NewShardIDFromQVMeta(newDelete.Meta),
			Version: qviews.FromProtoQueryViewVersion(newDelete.Meta.Version),
		},
	}
}
