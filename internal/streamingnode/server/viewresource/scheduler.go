package viewresource

import (
	"context"
	"errors"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

type Scheduler interface {
	Submit(task BuildTask)
	Close()
}

type BuildKey struct {
	CollectionID int64
	VChannel     string
	DataVersion  qviews.DataVersion
}

type BuildTask interface {
	Key() BuildKey
	Run()
	Done() <-chan struct{}
	Result() (*ViewRuntime, error)
	Cancel()
}

type defaultScheduler struct {
	sem    chan struct{}
	done   chan struct{}
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func NewScheduler(concurrency int) Scheduler {
	if concurrency <= 0 {
		concurrency = 1
	}
	return &defaultScheduler{
		sem:    make(chan struct{}, concurrency),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
}

func (s *defaultScheduler) Submit(task BuildTask) {
	select {
	case <-s.closed:
		task.Cancel()
		return
	default:
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-s.closed:
			task.Cancel()
			return
		}
		task.Run()
	}()
}

func (s *defaultScheduler) Close() {
	s.once.Do(func() {
		close(s.closed)
		s.wg.Wait()
		close(s.done)
	})
}

type resourceBuildTask struct {
	key    BuildKey
	ctx    context.Context
	cancel context.CancelFunc
	build  func(context.Context) (*ViewRuntime, error)

	done chan struct{}

	mu       sync.Mutex
	started  bool
	finished bool
	runtime  *ViewRuntime
	err      error
}

func newResourceBuildTask(parent context.Context, key BuildKey, build func(context.Context) (*ViewRuntime, error)) *resourceBuildTask {
	ctx, cancel := context.WithCancel(parent)
	return &resourceBuildTask{
		key:    key,
		ctx:    ctx,
		cancel: cancel,
		build:  build,
		done:   make(chan struct{}),
	}
}

func (t *resourceBuildTask) Key() BuildKey {
	return t.key
}

func (t *resourceBuildTask) Run() {
	t.mu.Lock()
	if t.started || t.finished {
		t.mu.Unlock()
		return
	}
	t.started = true
	t.mu.Unlock()

	runtime, err := t.build(t.ctx)
	t.finish(runtime, err)
}

func (t *resourceBuildTask) Done() <-chan struct{} {
	return t.done
}

func (t *resourceBuildTask) Result() (*ViewRuntime, error) {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runtime, t.err
}

func (t *resourceBuildTask) Cancel() {
	t.cancel()
	t.mu.Lock()
	if !t.started && !t.finished {
		t.err = t.ctx.Err()
		if t.err == nil {
			t.err = context.Canceled
		}
		t.finished = true
		close(t.done)
	}
	t.mu.Unlock()
}

func (t *resourceBuildTask) finish(runtime *ViewRuntime, err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		if runtime != nil {
			runtime.Close()
		}
		return
	}
	if err == nil && t.ctx.Err() != nil {
		err = t.ctx.Err()
		if err == nil {
			err = context.Canceled
		}
	}
	if err != nil && errors.Is(err, context.Canceled) && t.ctx.Err() != nil {
		err = t.ctx.Err()
	}
	t.runtime = runtime
	t.err = err
	t.finished = true
	close(t.done)
	t.mu.Unlock()
}
