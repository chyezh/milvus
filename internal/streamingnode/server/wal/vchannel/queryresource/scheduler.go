package queryresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type resourceBuildTask struct {
	build func(context.Context) (*QueryRuntime, error)
	done  chan struct{}

	mu       sync.Mutex
	finished bool
	runtime  *QueryRuntime
	err      error
}

func newResourceBuildTask(build func(context.Context) (*QueryRuntime, error)) *resourceBuildTask {
	return &resourceBuildTask{
		build: build,
		done:  make(chan struct{}),
	}
}

func (t *resourceBuildTask) Execute(ctx context.Context) error {
	runtime, err := t.build(ctx)
	t.finish(runtime, err)
	return err
}

func (t *resourceBuildTask) Result() (*QueryRuntime, error) {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runtime, t.err
}

func (t *resourceBuildTask) finish(runtime *QueryRuntime, err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		closeRuntime(runtime)
		return
	}
	t.runtime = runtime
	t.err = err
	t.finished = true
	close(t.done)
	t.mu.Unlock()
}

type scheduledBuild struct {
	task   *resourceBuildTask
	handle nodescheduler.TaskHandle
}

func scheduleResourceBuild(scheduler nodescheduler.Scheduler, task *resourceBuildTask) *scheduledBuild {
	return &scheduledBuild{
		task:   task,
		handle: scheduler.Submit(task),
	}
}

func (t *scheduledBuild) Cancel() {
	t.handle.Cancel()
}

func (t *scheduledBuild) Result() (*QueryRuntime, error) {
	_ = t.handle.Wait(context.Background())
	t.task.finish(nil, context.Canceled)
	return t.task.Result()
}

var _ nodescheduler.Task = (*resourceBuildTask)(nil)
