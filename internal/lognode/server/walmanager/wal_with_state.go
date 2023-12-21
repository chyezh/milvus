package walmanager

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/syncutil"
)

// walState is the state of a wal.
type walState struct {
	term      int64
	available bool
}

func (w1 walState) Before(w2 walState) bool {
	// w1 is before w2 if term of w1 is less than w2.
	// or w1 is available and w2 is not available.
	// because wal should always be available before unavailable in same term.
	return w1.term < w2.term || (w1.term == w2.term && w1.available && !w2.available)
}

type walStateWithCond struct {
	walState
	cond syncutil.ContextCond
}

// GetState returns the state of the wal.
func (w *walStateWithCond) GetState() walState {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	return w.walState
}

// SetStateAndNotify sets the state of the wal.
func (w *walStateWithCond) SetStateAndNotify(s walState) {
	w.cond.LockAndBroadcast()
	if w.Before(s) {
		w.term = s.term
		w.available = s.available
	}
	w.cond.L.Unlock()
}

// WatchChanged waits until the state is changed.
func (w *walStateWithCond) WatchChanged(ctx context.Context, s walState) error {
	w.cond.L.Lock()
	for w.term == s.term && w.available == s.available {
		if err := w.cond.Wait(ctx); err != nil {
			return err
		}
	}
	w.cond.L.Unlock()
	return nil
}

// walWithState is a wal with its state.
type walWithState struct {
	mu            sync.Mutex
	currentState  walStateWithCond
	wal           wal.WAL
	expectedState walStateWithCond
}

// GetWAL returns the wal instance.
func (w *walWithState) GetWAL() wal.WAL {
	w.mu.Lock()
	l := w.wal
	w.mu.Unlock()
	return l
}

// GetCurrentState returns the current state of the wal.
func (w *walWithState) GetCurrentState() walState {
	return w.currentState.GetState()
}

// WaitCurrentState waits until the current state is changed.
func (w *walWithState) WaitCurrentStateReachExpected(ctx context.Context, expectedState walState) error {
	state := w.currentState.GetState()
	for state.Before(expectedState) {
		if err := w.currentState.WatchChanged(ctx, state); err != nil {
			return err
		}
		state = w.currentState.GetState()
	}
	return nil
}

// SetExpectedState sets the expected state of the wal.
func (w *walWithState) SetExpectedState(s walState) {
	w.expectedState.SetStateAndNotify(s)
}

// NewIncomingWAL creates a new wal instance.
// It's not thread safe with CloseCurrentWAL.
func (w *walWithState) NewIncomingWAL(l wal.WAL) {
	w.mu.Lock()
	w.wal = l
	w.mu.Unlock()

	// set the state to available, notify the watcher.
	newState := walState{
		term:      l.Channel().Term,
		available: true,
	}
	w.currentState.SetStateAndNotify(newState)
}

// CloseCurrentWAL closes the current wal.
// It's not thread safe with NewIncomingWAL.
func (w *walWithState) CloseCurrentWAL() {
	// swap the wal, fast forbidden the append operation
	w.mu.Lock()
	oldWAL := w.wal
	w.mu.Unlock()

	// close the wal graceful, and set the state to unavailable.
	// notify the watcher.
	newState := walState{
		term:      oldWAL.Channel().Term,
		available: false,
	}
	oldWAL.Close()
	w.currentState.SetStateAndNotify(newState)
}
