package recovery

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type moduleMode int

const (
	moduleModeMetaOnly moduleMode = iota
	moduleModeMetaAndData
)

type broadcastAckModule struct {
	runtime     moduleapi.Runtime
	mode        moduleMode
	ackTaskMu   sync.Mutex
	ackTaskHead *broadcastAckTask
	ackTaskTail *broadcastAckTask
	ack         func(context.Context, message.ImmutableMessage) error
}

func newBroadcastAckModule(runtime moduleapi.Runtime) *broadcastAckModule {
	return &broadcastAckModule{
		runtime: runtime,
		mode:    moduleModeMetaOnly,
		ack: func(ctx context.Context, msg message.ImmutableMessage) error {
			return streaming.WAL().Broadcast().Ack(ctx, msg)
		},
	}
}

func (m *broadcastAckModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameAck
}

func (m *broadcastAckModule) ObserveMessage(_ context.Context, msg messageack.Message) {
	if msg.ImmutableMessage == nil || msg.BroadcastHeader() == nil || m.mode != moduleModeMetaAndData {
		return
	}
	record := msg.Ack()
	if record == nil {
		panic("broadcast ack observed data message without ack record")
	}
	task := &broadcastAckTask{
		module: m,
		msg:    msg.Message(),
		record: record,
		ref:    record.Retain(),
	}
	m.enqueueTask(task)
}

func (m *broadcastAckModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	m.mode = moduleModeMetaAndData
	return nil
}

func (m *broadcastAckModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	return nil
}

func (m *broadcastAckModule) enqueueTask(task *broadcastAckTask) {
	m.ackTaskMu.Lock()
	shouldSubmit := m.ackTaskTail == nil
	if shouldSubmit {
		m.ackTaskHead = task
	} else {
		m.ackTaskTail.next = task
	}
	m.ackTaskTail = task
	m.ackTaskMu.Unlock()

	if shouldSubmit && m.runtime.Scheduler != nil {
		m.runtime.Scheduler.Submit(task)
	}
}

func (m *broadcastAckModule) finishTask(task *broadcastAckTask) {
	m.ackTaskMu.Lock()
	if m.ackTaskHead != task {
		m.ackTaskMu.Unlock()
		return
	}
	next := task.next
	task.next = nil
	m.ackTaskHead = next
	if next == nil {
		m.ackTaskTail = nil
	}
	m.ackTaskMu.Unlock()

	if next != nil && m.runtime.Scheduler != nil {
		m.runtime.Scheduler.Submit(next)
	}
}

type broadcastAckTask struct {
	module *broadcastAckModule
	msg    message.ImmutableMessage
	record messageack.Record
	ref    messageack.Ref
	next   *broadcastAckTask
}

func (t *broadcastAckTask) Execute(ctx context.Context) error {
	if !t.record.Sealed() || t.record.RefCount() != 1 {
		return nodescheduler.ErrDelay
	}
	if err := t.module.ack(ctx, t.msg); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	t.ref.Done()
	t.module.finishTask(t)
	return nil
}

var (
	_ moduleapi.Module   = (*broadcastAckModule)(nil)
	_ nodescheduler.Task = (*broadcastAckTask)(nil)
)
