package gsegment

import (
	"container/list"
	"context"
	"sync"

	"go.uber.org/atomic"
)

// SyncScheduler is a scheduler that can schedule tasks in a CPU-bounded and IO-bounded background.
type SyncScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cpuLimit *atomic.Int32
	ioLimit  *atomic.Int32

	cond         *sync.Cond
	incomingTask *list.List
	cpuTasks     *list.List
	ioTasks      *list.List
	cpuRunning   int32
	ioRunning    int32
}

// NewSyncScheduler creates a new SyncScheduler.
func NewSyncScheduler(cpuLimit, ioLimit int) *SyncScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &SyncScheduler{
		ctx:      ctx,
		cancel:   cancel,
		cpuLimit: atomic.NewInt32(int32(cpuLimit)),
		ioLimit:  atomic.NewInt32(int32(ioLimit)),
		cpuTasks: list.New(),
		ioTasks:  list.New(),
	}
	s.cond = sync.NewCond(&sync.Mutex{})
	s.wg.Add(1)
	go s.scheduleLoop()
	return s
}

// AddTask adds a task to the scheduler.
func (s *SyncScheduler) AddTask(task SyncChunkTask) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if task.CPUBound() {
		s.cpuTasks.PushBack(task)
	} else {
		s.ioTasks.PushBack(task)
	}
	s.cond.Signal()
}

// UpdateLimits updates the limits of the scheduler.
func (s *SyncScheduler) UpdateLimits(cpuLimit, ioLimit int) {
	s.cpuLimit.Store(int32(cpuLimit))
	s.ioLimit.Store(int32(ioLimit))

	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()
}

// scheduleLoop is the main loop of the scheduler.
func (s *SyncScheduler) scheduleLoop() {
	defer s.wg.Done()
	for {
		s.cond.L.Lock()
		for s.ctx.Err() == nil &&
			(s.cpuRunning >= s.cpuLimit.Load() || s.cpuTasks.Len() == 0) &&
			(s.ioRunning >= s.ioLimit.Load() || s.ioTasks.Len() == 0) {
			s.cond.Wait()
		}

		if s.ctx.Err() != nil {
			s.cond.L.Unlock()
			return
		}

		// Dispatch CPU tasks
		for s.cpuRunning < s.cpuLimit.Load() && s.cpuTasks.Len() > 0 {
			element := s.cpuTasks.Front()
			s.cpuTasks.Remove(element)
			task := element.Value.(SyncChunkTask)
			s.cpuRunning++
			s.wg.Add(1)
			go s.executeTask(task, true)
		}

		// Dispatch IO tasks
		for s.ioRunning < s.ioLimit.Load() && s.ioTasks.Len() > 0 {
			element := s.ioTasks.Front()
			s.ioTasks.Remove(element)
			task := element.Value.(SyncChunkTask)
			s.ioRunning++
			s.wg.Add(1)
			go s.executeTask(task, false)
		}
		s.cond.L.Unlock()
	}
}

// executeTask executes a task.
func (s *SyncScheduler) executeTask(task SyncChunkTask, wasCPU bool) {
	defer s.wg.Done()

	err := task.Poll(s.ctx)

	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if wasCPU {
		s.cpuRunning--
	} else {
		s.ioRunning--
	}

	if err != nil {
		// if the task is not finished, put it back to the head of the queue to continue polling.
		if task.CPUBound() {
			s.cpuTasks.PushFront(task)
		} else {
			s.ioTasks.PushFront(task)
		}
	}
	s.cond.Signal()
}

// Close closes the scheduler.
func (s *SyncScheduler) Close() {
	s.cancel()
	s.cond.L.Lock()
	s.cond.Broadcast()
	s.cond.L.Unlock()
	s.wg.Wait()
}
