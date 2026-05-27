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

package delegator

import (
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	tsafeWaiterPending int32 = iota
	tsafeWaiterDone
)

const waitTSafeClosedLabel = "closed"

type tsafeWaiter struct {
	target uint64
	done   chan struct{}
	state  atomic.Int32
}

func stopTSafeWaitTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func newTSafeWaiter(target uint64) *tsafeWaiter {
	return &tsafeWaiter{
		target: target,
		done:   make(chan struct{}),
	}
}

func (w *tsafeWaiter) pending() bool {
	return w.state.Load() == tsafeWaiterPending
}

func (w *tsafeWaiter) finish() bool {
	return w.state.CompareAndSwap(tsafeWaiterPending, tsafeWaiterDone)
}

type tsafeWaiterManager struct {
	mut             sync.Mutex
	latestTSafe     *atomic.Uint64
	waiters         typeutil.Heap[*tsafeWaiter]
	closed          bool
	activeWaiter    int
	cancelledWaiter int
}

func newTSafeWaiterManager(latestTSafe *atomic.Uint64) *tsafeWaiterManager {
	return &tsafeWaiterManager{
		latestTSafe: latestTSafe,
		waiters: typeutil.NewObjectArrayBasedMinimumHeap(
			[]*tsafeWaiter{},
			func(waiter *tsafeWaiter) uint64 { return waiter.target },
		),
	}
}

func (m *tsafeWaiterManager) addWaiter(target uint64) (uint64, *tsafeWaiter, bool) {
	m.mut.Lock()
	defer m.mut.Unlock()

	latest := m.latestTSafe.Load()
	if latest >= target {
		return latest, nil, false
	}
	if m.closed {
		return latest, nil, true
	}

	waiter := newTSafeWaiter(target)
	m.waiters.Push(waiter)
	m.activeWaiter++
	return latest, waiter, false
}

func (m *tsafeWaiterManager) cancel(waiter *tsafeWaiter) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if waiter.finish() {
		m.activeWaiter--
		m.cancelledWaiter++
	}
	if m.cancelledWaiter > 0 && m.cancelledWaiter*2 >= m.waiters.Len() {
		m.compactLocked()
	}
}

func (m *tsafeWaiterManager) update(tsafe uint64) (int, int) {
	var ready []*tsafeWaiter
	avoidedWakeup := 0

	m.mut.Lock()
	if tsafe > m.latestTSafe.Load() {
		m.latestTSafe.Store(tsafe)
		ready = m.popReadyLocked(tsafe)
		avoidedWakeup = m.activeWaiter
	}
	m.mut.Unlock()

	for _, waiter := range ready {
		close(waiter.done)
	}
	return len(ready), avoidedWakeup
}

func (m *tsafeWaiterManager) closeAll() {
	var ready []*tsafeWaiter

	m.mut.Lock()
	m.closed = true
	for m.waiters.Len() > 0 {
		waiter := m.waiters.Pop()
		if waiter.finish() {
			m.activeWaiter--
			ready = append(ready, waiter)
		}
	}
	m.activeWaiter = 0
	m.cancelledWaiter = 0
	m.mut.Unlock()

	for _, waiter := range ready {
		close(waiter.done)
	}
}

func (m *tsafeWaiterManager) popReadyLocked(tsafe uint64) []*tsafeWaiter {
	ready := make([]*tsafeWaiter, 0)
	for m.waiters.Len() > 0 {
		waiter := m.waiters.Peek()
		if !waiter.pending() {
			m.waiters.Pop()
			m.cancelledWaiter--
			continue
		}
		if waiter.target > tsafe {
			break
		}
		m.waiters.Pop()
		if waiter.finish() {
			m.activeWaiter--
			ready = append(ready, waiter)
		}
	}
	return ready
}

func (m *tsafeWaiterManager) compactLocked() {
	waiters := make([]*tsafeWaiter, 0, m.waiters.Len()-m.cancelledWaiter)
	for m.waiters.Len() > 0 {
		waiter := m.waiters.Pop()
		if waiter.pending() {
			waiters = append(waiters, waiter)
		}
	}
	m.waiters = typeutil.NewObjectArrayBasedMinimumHeap(
		waiters,
		func(waiter *tsafeWaiter) uint64 { return waiter.target },
	)
	m.cancelledWaiter = 0
}
