// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodescheduler

import (
	"container/list"
	"context"
	"reflect"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type Scheduler interface {
	Submit(Task) TaskHandle
}

type Task interface {
	Execute(context.Context) error
}

type TaskHandle interface {
	Cancel()
	Wait(context.Context) error
}

type scheduleErrorKind int

const scheduleErrorKindDelay scheduleErrorKind = iota + 1

type ScheduleError struct {
	kind scheduleErrorKind
}

func (e *ScheduleError) Error() string {
	if e != nil && e.kind == scheduleErrorKindDelay {
		return "delay node scheduler task"
	}
	return "unknown node scheduler error"
}

func (e *ScheduleError) Is(target error) bool {
	other, ok := target.(*ScheduleError)
	return ok && e != nil && other != nil && e.kind == other.kind
}

var ErrDelay = &ScheduleError{kind: scheduleErrorKindDelay}

type nodeScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	cond   *sync.Cond
	queue  *list.List
	closed bool

	workers sync.WaitGroup
}

type taskEntry struct {
	task   Task
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	wakeup func()
}

func (e *taskEntry) finish() {
	e.once.Do(func() {
		e.cancel()
		close(e.done)
	})
}

type taskHandle struct {
	entry *taskEntry
}

func (h *taskHandle) Cancel() {
	h.entry.cancel()
	h.entry.wakeup()
}

func (h *taskHandle) Wait(ctx context.Context) error {
	select {
	case <-h.entry.done:
		return nil
	default:
	}

	select {
	case <-h.entry.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func New(concurrency int) *nodeScheduler {
	if concurrency <= 0 {
		panic("node scheduler concurrency must be greater than zero")
	}

	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &nodeScheduler{
		ctx:    ctx,
		cancel: cancel,
		queue:  list.New(),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	scheduler.workers.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go scheduler.runWorker()
	}
	return scheduler
}

func (s *nodeScheduler) Submit(task Task) TaskHandle {
	ctx, cancel := context.WithCancel(s.ctx)
	entry := &taskEntry{
		task:   task,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		wakeup: s.wakeup,
	}
	handle := &taskHandle{entry: entry}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		entry.finish()
		return handle
	}
	s.queue.PushBack(entry)
	s.cond.Signal()
	s.mu.Unlock()
	return handle
}

func (s *nodeScheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.workers.Wait()
		return
	}

	s.closed = true
	for element := s.queue.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*taskEntry)
		entry.finish()
	}
	s.queue.Init()
	s.cancel()
	s.cond.Broadcast()
	s.mu.Unlock()

	s.workers.Wait()
}

func (s *nodeScheduler) wakeup() {
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *nodeScheduler) runWorker() {
	defer s.workers.Done()
	for {
		entry := s.dequeue()
		if entry == nil {
			return
		}
		if entry.ctx.Err() != nil {
			entry.finish()
			continue
		}

		err := entry.task.Execute(entry.ctx)
		if entry.ctx.Err() != nil {
			entry.finish()
			continue
		}
		if errors.Is(err, ErrDelay) {
			if s.requeue(entry) {
				continue
			}
			entry.finish()
			continue
		}
		if err != nil {
			mlog.Error(entry.ctx, "node scheduler task failed",
				mlog.String("taskType", reflect.TypeOf(entry.task).String()),
				mlog.Err(err))
		}
		entry.finish()
	}
}

func (s *nodeScheduler) dequeue() *taskEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.queue.Len() == 0 && !s.closed {
		s.cond.Wait()
	}
	if s.closed {
		return nil
	}

	element := s.queue.Front()
	s.queue.Remove(element)
	return element.Value.(*taskEntry)
}

func (s *nodeScheduler) requeue(entry *taskEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || entry.ctx.Err() != nil {
		return false
	}
	s.queue.PushBack(entry)
	s.cond.Signal()
	return true
}

var global struct {
	sync.Mutex
	scheduler *nodeScheduler
}

func Init(concurrency int) {
	global.Lock()
	defer global.Unlock()
	if global.scheduler != nil {
		panic("node scheduler is already initialized")
	}
	global.scheduler = New(concurrency)
}

func Get() Scheduler {
	global.Lock()
	defer global.Unlock()
	if global.scheduler == nil {
		panic("node scheduler is not initialized")
	}
	return global.scheduler
}

func Close() {
	global.Lock()
	scheduler := global.scheduler
	global.scheduler = nil
	global.Unlock()
	if scheduler != nil {
		scheduler.Close()
	}
}
