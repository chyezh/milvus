package vchannel

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type transformTaskBase struct {
	module       *VChannelRecoveryModule
	name         string
	timetick     uint64
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *transformTaskBase) Name() string {
	return "vchan-transformlog-" + t.name
}

func (t *transformTaskBase) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *transformTaskBase) Done() bool {
	return t.done.Load()
}

type transformFlushTask struct {
	transformTaskBase
}

func (t *transformFlushTask) Run(ctx context.Context) error {
	result, err := t.module.transformLog.Flush(ctx, transformlog.FlushOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.done.Store(true)
	if result.NextTargetTimeTick > 0 {
		t.module.submitTransformFlushTask(result.NextTargetTimeTick)
	}
	if t.module.transformLog.ShouldMaterialize() && !t.module.hasPendingTransformMaterializeTask() {
		t.module.submitTransformMaterializeTask(t.module.transformLog.DataCheckpointTimeTick())
	}
	t.module.notifyTransformLogUpdated()
	return nil
}

type transformMaterializeTask struct {
	transformTaskBase
}

func (t *transformMaterializeTask) Run(ctx context.Context) error {
	_, err := t.module.transformLog.Materialize(ctx, transformlog.MaterializeOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.done.Store(true)
	t.module.notifyTransformLogUpdated()
	return nil
}

func (m *VChannelRecoveryModule) newTransformFlushTask(timetick uint64) scheduler.Task {
	return &transformFlushTask{
		transformTaskBase: transformTaskBase{
			module:       m,
			name:         "flush",
			timetick:     timetick,
			precondition: m.transformTaskPrecondition(),
		},
	}
}

func (m *VChannelRecoveryModule) newTransformMaterializeTask(timetick uint64) scheduler.Task {
	return &transformMaterializeTask{
		transformTaskBase: transformTaskBase{
			module:   m,
			name:     "materialize",
			timetick: timetick,
			precondition: scheduler.All(m.transformTaskPrecondition(), scheduler.PreconditionFunc(func() bool {
				return m.transformLog.DataBarrierTimeTick() >= timetick
			})),
		},
	}
}

func (m *VChannelRecoveryModule) transformTaskPrecondition() scheduler.Precondition {
	m.flushTasks = compactPendingTasks(m.flushTasks)
	m.materializeTasks = compactPendingTasks(m.materializeTasks)
	preconditions := make([]scheduler.Precondition, 0, len(m.flushTasks)+len(m.materializeTasks))
	for _, task := range m.flushTasks {
		if task == nil || task.Done() {
			continue
		}
		preconditions = append(preconditions, scheduler.After(task))
	}
	for _, task := range m.materializeTasks {
		if task == nil || task.Done() {
			continue
		}
		preconditions = append(preconditions, scheduler.After(task))
	}
	return scheduler.All(preconditions...)
}

func (m *VChannelRecoveryModule) notifyTransformLogUpdated() {
	if m.runtime.Notifier == nil {
		return
	}
	m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
	m.runtime.Notifier.NotifyBarrierUpdated()
}

var _ scheduler.Task = (*transformFlushTask)(nil)
var _ scheduler.Task = (*transformMaterializeTask)(nil)
