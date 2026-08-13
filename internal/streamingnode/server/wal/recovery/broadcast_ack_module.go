package recovery

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

const broadcastAckRetryInterval = 200 * time.Millisecond

type broadcastAckModule struct {
	runtime     moduleapi.Runtime
	ctx         context.Context
	cancel      context.CancelFunc
	retryDelay  time.Duration
	closeOnce   sync.Once
	workerMu    sync.Mutex
	closed      bool
	workerWG    sync.WaitGroup
	ackTaskMu   sync.Mutex
	ackTaskHead *broadcastAckTask
	ackTaskTail *broadcastAckTask
	ack         func(context.Context, message.ImmutableMessage) error
}

func newBroadcastAckModule(runtime moduleapi.Runtime) *broadcastAckModule {
	ctx, cancel := context.WithCancel(context.Background())
	return &broadcastAckModule{
		runtime:    runtime,
		ctx:        ctx,
		cancel:     cancel,
		retryDelay: broadcastAckRetryInterval,
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
		m.submitWhenConsumersDone(task)
	}
}

func (m *broadcastAckModule) submitWhenConsumersDone(task *broadcastAckTask) {
	if !m.beginWorker() {
		return
	}
	go func() {
		defer m.workerWG.Done()
		select {
		case <-task.tracked.ConsumersDone():
			m.submit(task)
		case <-m.ctx.Done():
		}
	}()
}

func (m *broadcastAckModule) submit(task *broadcastAckTask) {
	if m.runtime.Scheduler == nil || m.ctx.Err() != nil {
		return
	}
	m.runtime.Scheduler.Submit(task)
}

func (m *broadcastAckModule) retry(task *broadcastAckTask) {
	if !m.beginWorker() {
		return
	}
	go func() {
		defer m.workerWG.Done()
		timer := time.NewTimer(m.retryDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			m.submit(task)
		case <-m.ctx.Done():
		}
	}()
}

func (m *broadcastAckModule) Close() {
	m.closeOnce.Do(func() {
		m.workerMu.Lock()
		m.closed = true
		m.cancel()
		m.workerMu.Unlock()
		m.workerWG.Wait()
	})
}

func (m *broadcastAckModule) beginWorker() bool {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if m.closed {
		return false
	}
	m.workerWG.Add(1)
	return true
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
		m.submitWhenConsumersDone(next)
	}
}

type broadcastAckTask struct {
	module  *broadcastAckModule
	tracked *messageack.TrackedMessage
	next    *broadcastAckTask
}

func (t *broadcastAckTask) Execute(ctx context.Context) error {
	if err := t.module.ack(ctx, t.tracked.Message()); err != nil {
		t.module.retry(t)
		return nil
	}
	t.tracked.CompleteBroadcastAck()
	t.module.finishTask(t)
	return nil
}

var _ nodescheduler.Task = (*broadcastAckTask)(nil)
