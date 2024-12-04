package recovery

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/protobuf/proto"
)

var _ RecoveryStorage = (*RecoveryImpl)(nil)

// NewRecovery creates a new recovery instance.
func NewRecovery(metaKV kv.MetaKv) *RecoveryImpl {
	r := &RecoveryImpl{
		metaKV:        metaKV,
		taskChan:      make(chan *pendingTask, 100),
		eventNotifier: make(chan events.RecoveryEvent, 100),
		n:             syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	go r.backgroundTask()
	return r
}

// RecoveryImpl is the implementation of Recovery.
type RecoveryImpl struct {
	metaKV        kv.MetaKv
	taskChan      chan *pendingTask
	eventNotifier chan events.RecoveryEvent
	backoff       *typeutil.BackoffTimer
	n             *syncutil.AsyncTaskNotifier[struct{}]
}

// backgroundTask is the background task to handle the recovery write tasks.
func (r *RecoveryImpl) backgroundTask() {
	defer func() {
		close(r.eventNotifier)
		r.n.Finish(struct{}{})
	}()

	var pendingTasks []*pendingTask
	var backoffTimer <-chan time.Time

	for {
		select {
		case <-r.n.Context().Done():
			return
		case newIncomingTask := <-r.taskChan:
			pendingTasks = append(pendingTasks, newIncomingTask)
			pendingTasks = r.consumeTaskChan(pendingTasks)
		case <-backoffTimer:
		}
		pendingTasks = r.handlePendingEvents(pendingTasks)
		if len(pendingTasks) > 0 {
			r.backoff.EnableBackoff()
			backoffTimer, _ = r.backoff.NextTimer()
		} else {
			r.backoff.DisableBackoff()
			backoffTimer = nil
		}
	}
}

// consumeTaskChan consumes all the tasks in the task channel.
func (r *RecoveryImpl) consumeTaskChan(tasks []*pendingTask) []*pendingTask {
	for {
		select {
		case newIncomingTask := <-r.taskChan:
			tasks = append(tasks, newIncomingTask)
		default:
			return tasks
		}
	}
}

// handlePendingEvents handles the pending events.
func (r *RecoveryImpl) handlePendingEvents(tasks []*pendingTask) []*pendingTask {
	// TODO: do a task mergify here.
	failTasks := make([]*pendingTask, 0, len(tasks))
	for _, task := range tasks {
		if err := r.metaKV.MultiSaveAndRemove(task.saves, task.removals); err != nil {
			failTasks = append(failTasks, task)
			continue
		}
		r.eventNotifier <- task.event
	}
	return failTasks
}

// SwapPreparing swaps the old preparing view with new preparing view.
func (r *RecoveryImpl) SwapPreparing(ctx context.Context, shardID qviews.ShardID, old *viewpb.QueryViewOfShard, new *viewpb.QueryViewOfShard) {
	r.taskChan <- r.newSwapPreparingTask(shardID, old, new)
}

// UpNewPerparingView remove the preparing view and make it as a up view.
func (r *RecoveryImpl) UpNewPreparingView(ctx context.Context, newUp *viewpb.QueryViewOfShard) {
	r.taskChan <- r.newUpNewPreparingView(newUp)
}

// Save saves the recovery infos into underlying persist storage.
func (r *RecoveryImpl) Save(ctx context.Context, saved *viewpb.QueryViewOfShard) {
	r.taskChan <- r.newSave(saved)
}

func (r *RecoveryImpl) Delete(ctx context.Context, deleted *viewpb.QueryViewOfShard) {
	r.taskChan <- r.newDelete(deleted)
}

func (r *RecoveryImpl) Event() <-chan events.RecoveryEvent {
	return r.eventNotifier
}

func (r *RecoveryImpl) Close() {
	r.n.Cancel()
	r.n.BlockUntilFinish()
}

func (r *RecoveryImpl) getPreparingPath(shardID qviews.ShardID) (key string) {
	return ""
}

func (r *RecoveryImpl) getHistoryPath(metas *viewpb.QueryViewMeta) (key string) {
	return ""
}

func mustMarshal(m proto.Message) string {
	bytes, err := proto.Marshal(m)
	if err != nil {
		panic("marshal meta fields failed")
	}
	return string(bytes)
}
