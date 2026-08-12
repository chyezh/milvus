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

type broadcastAckModule struct {
	runtime     moduleapi.Runtime
	ackTaskMu   sync.Mutex
	ackTaskHead *broadcastAckTask
	ackTaskTail *broadcastAckTask
	ack         func(context.Context, message.ImmutableMessage) error
}

func newBroadcastAckModule(runtime moduleapi.Runtime) *broadcastAckModule {
	return &broadcastAckModule{
		runtime: runtime,
		ack: func(ctx context.Context, msg message.ImmutableMessage) error {
			return streaming.WAL().Broadcast().Ack(ctx, msg)
		},
	}
}

func (m *broadcastAckModule) Accept(
	owner message.OwnedImmutableMessage,
	tracked *messageack.TrackedMessage,
) {
	owner.Release()
	if !tracked.RequiresBroadcastAck() {
		return
	}
	task := &broadcastAckTask{
		module:  m,
		tracked: tracked,
	}
	m.enqueueTask(task)
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
	module  *broadcastAckModule
	tracked *messageack.TrackedMessage
	next    *broadcastAckTask
}

func (t *broadcastAckTask) Execute(ctx context.Context) error {
	select {
	case <-t.tracked.ConsumersDone():
	default:
		return nodescheduler.ErrDelay
	}
	if err := t.module.ack(ctx, t.tracked.Message()); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	t.tracked.CompleteBroadcastAck()
	t.module.finishTask(t)
	return nil
}

var _ nodescheduler.Task = (*broadcastAckTask)(nil)
