package growing

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type transformLogView struct {
	mu    sync.Mutex
	log   waltransformlog.TransformLog
	tasks []scheduler.TaskHandle
}

func newTransformLogView(transformLog waltransformlog.TransformLog) *transformLogView {
	return &transformLogView{log: transformLog}
}

func (v *transformLogView) hasPendingTask() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, task := range v.tasks {
		if task != nil && !task.Done() {
			return true
		}
	}
	return false
}

func (v *transformLogView) startFlushTask(manager *Manager, vchannel string, timetick uint64) scheduler.Task {
	v.mu.Lock()
	defer v.mu.Unlock()
	task := &flushTransformLogBufferTask{
		manager:      manager,
		vchannelName: vchannel,
		timetick:     timetick,
		precondition: v.taskPreconditionLocked(),
	}
	v.tasks = append(v.tasks, task)
	return task
}

func (v *transformLogView) taskPreconditionLocked() scheduler.Precondition {
	pending := v.tasks[:0]
	preconditions := make([]scheduler.Precondition, 0, len(v.tasks))
	for _, task := range v.tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
		preconditions = append(preconditions, scheduler.After(task))
	}
	v.tasks = pending
	return scheduler.All(preconditions...)
}

type flushTransformLogBufferTask struct {
	manager      *Manager
	vchannelName string
	timetick     uint64
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *flushTransformLogBufferTask) Name() string {
	return "growing-flush-transform-log-buffer"
}

func (t *flushTransformLogBufferTask) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *flushTransformLogBufferTask) Done() bool {
	return t.done.Load()
}

func (t *flushTransformLogBufferTask) Run(ctx context.Context) error {
	err := t.run(ctx)
	if err == nil {
		t.done.Store(true)
	}
	return err
}

func (t *flushTransformLogBufferTask) run(ctx context.Context) error {
	vchannel := t.manager.retainedVChannel(t.vchannelName)
	transformLog := t.manager.transformLog(t.vchannelName)
	if vchannel == nil || transformLog == nil {
		return nil
	}
	currentDurableTimeTick := vchannel.DataCheckpointTimeTick()
	result, err := transformLog.log.Flush(ctx, waltransformlog.FlushOption{
		TargetTimeTick:         t.timetick,
		CurrentDurableTimeTick: currentDurableTimeTick,
	})
	if err != nil {
		return err
	}
	if !result.Started {
		return nil
	}

	var nextTask scheduler.Task
	vchannel.mu.Lock()
	if result.DurableTimeTick > 0 {
		vchannel.MarkDeleteDataDurable(result.DurableTimeTick)
	}
	if result.NextTargetTimeTick > 0 {
		nextTask = t.manager.startFlushTransformLogBufferTask(t.vchannelName, result.NextTargetTimeTick)
	}
	runtime := vchannel.runtime
	vchannel.mu.Unlock()

	vchannel.NotifyDataUpdated()
	if nextTask != nil {
		runtime.Scheduler.Submit(nextTask)
	}
	return nil
}
