package syncutil

import (
	"context"
	"sync"
)

// NewContextCond creates a new condition variable can be acted with context.
// Broadcast is implemented by channel, so performance is not good as sync.Cond.
func NewContextCond(l sync.Locker) *ContextCond {
	return &ContextCond{L: l}
}

// ContextCond is a condition variable implementation.
type ContextCond struct {
	L  sync.Locker
	ch chan struct{}
}

// Lock locks the underlying locker and do a broadcast.
func (cv *ContextCond) LockAndBroadcast() {
	cv.L.Lock()
	if cv.ch != nil {
		close(cv.ch)
		cv.ch = nil
	}
}

// Wait waits for a broadcast or context timeout.
func (cv *ContextCond) Wait(ctx context.Context) error {
	if cv.ch == nil {
		cv.ch = make(chan struct{})
	}
	ch := cv.ch
	cv.L.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	cv.L.Lock()
	return nil
}
