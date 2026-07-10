package viewresource

import "sync"

const defaultLiveEventDispatchConcurrency = 4

type queryRuntimeDispatcher struct {
	tasks  chan *QueryRuntime
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func newQueryRuntimeDispatcher(concurrency int) *queryRuntimeDispatcher {
	if concurrency <= 0 {
		concurrency = 1
	}
	dispatcher := &queryRuntimeDispatcher{
		tasks:  make(chan *QueryRuntime, 1024),
		closed: make(chan struct{}),
	}
	for i := 0; i < concurrency; i++ {
		dispatcher.wg.Add(1)
		go func() {
			defer dispatcher.wg.Done()
			dispatcher.worker()
		}()
	}
	return dispatcher
}

func (d *queryRuntimeDispatcher) Submit(runtime *QueryRuntime) bool {
	if d == nil || runtime == nil {
		return false
	}
	select {
	case d.tasks <- runtime:
		return true
	case <-d.closed:
		return false
	}
}

func (d *queryRuntimeDispatcher) Close() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		close(d.closed)
		d.wg.Wait()
	})
}

func (d *queryRuntimeDispatcher) worker() {
	for {
		select {
		case runtime := <-d.tasks:
			runtime.drainReady()
		case <-d.closed:
			return
		}
	}
}
