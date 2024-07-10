package lclock

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UnconfirmedLogicClock is the clock that can be confirmed.
type UnconfirmedLogicClock struct {
	clock int64
	m     *LogicClockManager
}

// Clock returns the allocated clock.
func (c *UnconfirmedLogicClock) Clock() int64 {
	return c.clock
}

// Confirm confirms the clock and returns the confirmed clock.
func (c *UnconfirmedLogicClock) Confirm(err error) int64 {
	return c.m.confirm(c.clock, err)
}

// ConfirmedInfo is a pair of clock that contains LAC and LEC.
type ConfirmedInfo struct {
	LAC int64 // Last add confirmed clock that can be read from cache.
	LF  int64 // Last fenced clock that can be read from cache.
}

// LogicClockManager is the manager that manages the logic clock allocation and confirming.
type LogicClockManager struct {
	cv      *syncutil.ContextCond
	fencing bool // Mark if the logic clock is fenced.

	clock                     int64
	unconfirmedBeginClockHeap typeutil.Heap[int64]
	unconfirmedBeginClockMap  map[int64]struct{}
	confirmedInfo             ConfirmedInfo
}

// NewLogicClockManager creates a new LogicClockManager.
func NewLogicClockManager() *LogicClockManager {
	return &LogicClockManager{
		cv:      syncutil.NewContextCond(&sync.Mutex{}),
		fencing: false,

		unconfirmedBeginClockHeap: typeutil.NewArrayBasedMinimumHeap[int64]([]int64{}),
		unconfirmedBeginClockMap:  map[int64]struct{}{},
		confirmedInfo:             ConfirmedInfo{},
		clock:                     1,
	}
}

// AllocateClock allocates a new clock.
func (lcm *LogicClockManager) AllocateClock(ctx context.Context) (UnconfirmedLogicClock, error) {
	// if the logic clock is fenced, wait until it's not fenced.
	if err := lcm.waitUntilNotFencedAndLock(ctx); err != nil {
		return UnconfirmedLogicClock{}, err
	}
	defer lcm.cv.L.Unlock()

	clock := lcm.allocateNewClock()
	lcm.unconfirmedBeginClockHeap.Push(clock)
	lcm.unconfirmedBeginClockMap[clock] = struct{}{}
	return UnconfirmedLogicClock{
		clock: clock,
		m:     lcm,
	}, nil
}

// waitUntilNotFencedAndLock waits until the logic clock is not fenced.
func (lcm *LogicClockManager) waitUntilNotFencedAndLock(ctx context.Context) error {
	lcm.cv.L.Lock()
	for lcm.fencing {
		if err := lcm.cv.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// confirm confirms the clock and returns the confirmed clock.
func (lcm *LogicClockManager) confirm(clock int64, err error) int64 {
	lcm.cv.L.Lock()
	defer lcm.cv.L.Unlock()

	if _, ok := lcm.unconfirmedBeginClockMap[clock]; !ok {
		panic("unconfirmed clock not found")
	}

	delete(lcm.unconfirmedBeginClockMap, clock)
	for lcm.unconfirmedBeginClockHeap.Len() > 0 {
		if _, ok := lcm.unconfirmedBeginClockMap[lcm.unconfirmedBeginClockHeap.Peek()]; ok {
			break
		}
		// pop all deleted clocks and advance LAC
		lcm.confirmedInfo.LAC = lcm.unconfirmedBeginClockHeap.Peek()
		lcm.unconfirmedBeginClockHeap.Pop()
	}

	confirmedClock := lcm.allocateNewClock()
	if err != nil || lcm.fencing {
		// if the clock is confirmed with error,
		// fence the logic clock until all concurrency operations are done.
		lcm.fencing = true
		lcm.confirmedInfo.LF = confirmedClock // confirmedClock is always the latest, so it's the new LF.
	}
	if lcm.unconfirmedBeginClockHeap.Len() == 0 {
		// there's no concurrency operation, so the logic clock can turn into unfenced.
		// and the LAC can be updated as latest.
		lcm.cv.UnsafeBroadcast()
		lcm.fencing = false
		lcm.confirmedInfo.LAC = lcm.allocateNewClock()
	}
	return confirmedClock
}

// ConfirmedInfo returns the confirmed info of manager.
func (lcm *LogicClockManager) ConfirmedInfo() ConfirmedInfo {
	lcm.cv.L.Lock()
	defer lcm.cv.L.Unlock()
	return lcm.confirmedInfo
}

// allocateNewClock allocates a new clock.
func (lcm *LogicClockManager) allocateNewClock() int64 {
	clock := lcm.clock
	lcm.clock++
	if clock < 0 {
		// Unreachable: it's hard to reach it.
		panic("logic clock overflow")
	}
	return clock
}
