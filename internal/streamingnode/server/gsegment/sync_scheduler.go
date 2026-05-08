// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
)

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 60 * time.Second
	retryMultiplier      = 2.0
)

// SyncScheduler decouples CPU-bound and IO-bound stages of chunk sync tasks
// into two independent worker pools. A task's Poll is called repeatedly; after
// each Poll it is re-dispatched to the pool matching its next stage, so CPU
// and IO slots are never blocked on each other.
//
// Failures:
//   - nil                 — terminal success; scheduler calls task.OnComplete(nil)
//   - ErrContinue         — yield; re-enqueue in CPU or IO pool per task.CPUBound()
//   - retryableError      — transient failure; push onto retry heap with exp backoff
//   - any other error     — terminal failure; scheduler calls task.OnComplete(err)
type SyncScheduler struct {
	log.Binder
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cpuLimit *atomic.Int32
	ioLimit  *atomic.Int32

	mu         sync.Mutex
	cond       *sync.Cond
	cpuTasks   *list.List
	ioTasks    *list.List
	retries    []retryEntry // unsorted; small N so linear scan is fine
	retryTries map[string]int
	retryWake  chan struct{}
	cpuRunning int32
	ioRunning  int32
	completing int32
	closed     bool
}

type retryEntry struct {
	task    SyncChunkTask
	dueAt   time.Time
	attempt int
}

// NewSyncScheduler creates a scheduler with the given pool sizes.
func NewSyncScheduler(cpuLimit, ioLimit int) *SyncScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &SyncScheduler{
		ctx:        ctx,
		cancel:     cancel,
		cpuLimit:   atomic.NewInt32(int32(cpuLimit)),
		ioLimit:    atomic.NewInt32(int32(ioLimit)),
		cpuTasks:   list.New(),
		ioTasks:    list.New(),
		retryTries: make(map[string]int),
		retryWake:  make(chan struct{}, 1),
	}
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(2)
	go s.scheduleLoop()
	go s.retryLoop()
	return s
}

// AddTask enqueues a task in the pool matching task.CPUBound().
func (s *SyncScheduler) AddTask(task SyncChunkTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Scheduler is shutting down; complete the task with ctx canceled so callers unblock.
		go task.OnComplete(context.Canceled)
		return
	}
	s.pushLocked(task)
}

// pushLocked places a task onto the correct pool queue. Caller must hold s.mu.
func (s *SyncScheduler) pushLocked(task SyncChunkTask) {
	if task.CPUBound() {
		s.cpuTasks.PushBack(task)
	} else {
		s.ioTasks.PushBack(task)
	}
	s.cond.Broadcast()
}

// UpdateLimits adjusts the pool sizes at runtime.
func (s *SyncScheduler) UpdateLimits(cpuLimit, ioLimit int) {
	s.cpuLimit.Store(int32(cpuLimit))
	s.ioLimit.Store(int32(ioLimit))
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()
}

// scheduleLoop dispatches runnable tasks from cpuTasks/ioTasks to worker goroutines.
func (s *SyncScheduler) scheduleLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for !s.closed &&
			(s.cpuRunning >= s.cpuLimit.Load() || s.cpuTasks.Len() == 0) &&
			(s.ioRunning >= s.ioLimit.Load() || s.ioTasks.Len() == 0) {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		for s.cpuRunning < s.cpuLimit.Load() && s.cpuTasks.Len() > 0 {
			elem := s.cpuTasks.Front()
			s.cpuTasks.Remove(elem)
			s.cpuRunning++
			s.wg.Add(1)
			go s.executeTask(elem.Value.(SyncChunkTask), true)
		}
		for s.ioRunning < s.ioLimit.Load() && s.ioTasks.Len() > 0 {
			elem := s.ioTasks.Front()
			s.ioTasks.Remove(elem)
			s.ioRunning++
			s.wg.Add(1)
			go s.executeTask(elem.Value.(SyncChunkTask), false)
		}
		s.mu.Unlock()
	}
}

// executeTask runs a single Poll and then either completes the task, yields
// it back to a pool, or puts it on the retry heap. If Close raced with this
// Poll, the task is failed with context.Canceled so callers never orphan.
func (s *SyncScheduler) executeTask(task SyncChunkTask, wasCPU bool) {
	defer s.wg.Done()
	err := task.Poll(s.ctx)

	s.mu.Lock()
	if wasCPU {
		s.cpuRunning--
	} else {
		s.ioRunning--
	}
	closed := s.closed

	if closed {
		s.completing++
		s.cond.Broadcast()
		s.mu.Unlock()
		task.OnComplete(context.Canceled)
		s.finishComplete()
		return
	}
	switch {
	case err == nil:
		s.forgetRetryLocked(task)
		s.completing++
		s.cond.Broadcast()
		s.mu.Unlock()
		task.OnComplete(nil)
		s.finishComplete()
	case errors.Is(err, ErrContinue):
		if s.closed {
			s.mu.Unlock()
			task.OnComplete(context.Canceled)
			return
		}
		s.pushLocked(task)
		s.mu.Unlock()
	case IsRetryable(err):
		if s.closed {
			s.mu.Unlock()
			task.OnComplete(context.Canceled)
			return
		}
		s.scheduleRetryLocked(task, err)
		s.mu.Unlock()
	default:
		s.forgetRetryLocked(task)
		s.Logger().Warn("sync task failed with unretryable error",
			zap.String("key", task.Key()), zap.Error(err))
		s.completing++
		s.cond.Broadcast()
		s.mu.Unlock()
		task.OnComplete(err)
		s.finishComplete()
	}
}

// scheduleRetryLocked registers a transient-failure task for a later wake-up.
// Caller must hold s.mu.
func (s *SyncScheduler) scheduleRetryLocked(task SyncChunkTask, cause error) {
	key := task.Key()
	attempt := s.retryTries[key] + 1
	s.retryTries[key] = attempt
	backoff := computeBackoff(attempt)
	s.retries = append(s.retries, retryEntry{
		task:    task,
		dueAt:   time.Now().Add(backoff),
		attempt: attempt,
	})
	s.Logger().Warn("sync task entered retry heap",
		zap.String("key", task.Key()),
		zap.Int("attempt", attempt),
		zap.Duration("backoff", backoff),
		zap.Error(cause))
	s.cond.Broadcast()
	select {
	case s.retryWake <- struct{}{}:
	default:
	}
}

func (s *SyncScheduler) forgetRetry(task SyncChunkTask) {
	s.mu.Lock()
	s.forgetRetryLocked(task)
	s.mu.Unlock()
}

func (s *SyncScheduler) forgetRetryLocked(task SyncChunkTask) {
	delete(s.retryTries, task.Key())
}

func (s *SyncScheduler) finishComplete() {
	s.mu.Lock()
	s.completing--
	s.cond.Broadcast()
	s.mu.Unlock()
}

// retryLoop wakes periodically (or on broadcast) to promote due retry entries
// back onto the active pool queues.
func (s *SyncScheduler) retryLoop() {
	defer s.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		s.mu.Lock()
		for !s.closed && len(s.retries) == 0 {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		now := time.Now()
		// Find earliest due time; simultaneously promote already-due entries.
		nextDue := time.Time{}
		dueTasks := make([]SyncChunkTask, 0, len(s.retries))
		remaining := s.retries[:0]
		for _, r := range s.retries {
			if !r.dueAt.After(now) {
				dueTasks = append(dueTasks, r.task)
				continue
			}
			if nextDue.IsZero() || r.dueAt.Before(nextDue) {
				nextDue = r.dueAt
			}
			remaining = append(remaining, r)
		}
		s.retries = remaining
		for _, t := range dueTasks {
			s.pushLocked(t)
		}
		s.mu.Unlock()

		if len(dueTasks) > 0 {
			continue
		}

		if nextDue.IsZero() {
			// No pending retries; sleep until signaled.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Hour)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Until(nextDue))
		}

		select {
		case <-s.ctx.Done():
			// Drain retries and fail them so callers unblock.
			s.mu.Lock()
			toFail := s.retries
			s.retries = nil
			s.mu.Unlock()
			for _, r := range toFail {
				r.task.OnComplete(context.Canceled)
			}
			return
		case <-timer.C:
		case <-s.retryWake:
		}
	}
}

// computeBackoff returns the delay before the Nth retry attempt (1-indexed),
// with exponential growth capped at retryMaxInterval.
func computeBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return retryInitialInterval
	}
	d := retryInitialInterval
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * retryMultiplier)
		if d >= retryMaxInterval {
			return retryMaxInterval
		}
	}
	return d
}

// Drain waits until all queued, retrying, running, and completion-callback work
// has finished. It does not close the scheduler; callers may continue to add
// tasks after Drain returns.
func (s *SyncScheduler) Drain(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		idle := !s.closed &&
			s.cpuTasks.Len() == 0 &&
			s.ioTasks.Len() == 0 &&
			len(s.retries) == 0 &&
			s.cpuRunning == 0 &&
			s.ioRunning == 0 &&
			s.completing == 0
		closed := s.closed
		s.mu.Unlock()
		if idle {
			return nil
		}
		if closed {
			return context.Canceled
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Close stops the scheduler; in-flight tasks finish their current Poll and
// then fail with context.Canceled. Queued and retrying tasks are also failed.
func (s *SyncScheduler) Close() {
	s.cancel()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	// Drain queues and fail everything; callers awaiting OnComplete unblock.
	var pending []SyncChunkTask
	for e := s.cpuTasks.Front(); e != nil; e = e.Next() {
		pending = append(pending, e.Value.(SyncChunkTask))
	}
	s.cpuTasks.Init()
	for e := s.ioTasks.Front(); e != nil; e = e.Next() {
		pending = append(pending, e.Value.(SyncChunkTask))
	}
	s.ioTasks.Init()
	for _, r := range s.retries {
		pending = append(pending, r.task)
	}
	s.retries = nil
	s.retryTries = make(map[string]int)
	s.cond.Broadcast()
	s.mu.Unlock()

	for _, t := range pending {
		t.OnComplete(context.Canceled)
	}
	s.wg.Wait()
}
