package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// newWALLifetime create a WALLifetime with opener.
func newWALLifetime(opener wal.Opener, channel string) *walLifetime {
	ctx, cancel := context.WithCancel(context.Background())
	l := &walLifetime{
		ctx:    ctx,
		cancel: cancel,
		finish: make(chan struct{}),
		opener: opener,
		logger: log.With(zap.String("channel", channel)),
	}
	go l.backgroundTask()
	return l
}

// walLifetime is the lifetime management of a wal.
// It promise a wal is keep state consistency in distributed environment.
// (term, available) illuminate the state of wal.
// wal will be changed as following rules:
// term is always increasing, available is always before unavailable in same term, such as:
// (-1, false) -> (0, true) -> (1, true) -> (2, true) -> (3, false) -> (7, true) -> ...
type walLifetime struct {
	ctx       context.Context
	cancel    context.CancelFunc
	finish    chan struct{}
	opener    wal.Opener
	logger    *log.MLogger
	statePair walStatePair
}

// GetWAL returns a available wal instance for the channel.
// Return nil if the wal is not available now.
func (w *walLifetime) GetWAL() wal.WAL {
	return w.statePair.GetWAL()
}

// Open opens a wal instance for the channel on this Manager.
func (w *walLifetime) Open(ctx context.Context, opt *wal.OpenOption) error {
	// Set expected WAL state to available at given term.
	expected := availableExpectedWALState{
		ctx:     ctx,
		channel: opt.Channel,
	}
	w.statePair.SetExpectedState(expected)

	// Wait until the WAL state is ready or term expired or error occurs.
	return w.statePair.WaitCurrentStateReachExpected(ctx, expected)
}

// Remove removes the wal instance for the channel on this Manager.
func (w *walLifetime) Remove(ctx context.Context, term int64) error {
	// Set expected WAL state to unavailable at given term.
	expected := unavailableExpectedWALState{
		term: term,
	}
	w.statePair.SetExpectedState(expected)

	// Wait until the WAL state is ready or term expired or error occurs.
	return w.statePair.WaitCurrentStateReachExpected(ctx, expected)
}

// Close closes the wal lifetime.
func (w *walLifetime) Close() {
	// Close all background task.
	w.cancel()
	<-w.finish

	// No background task is running now, close current wal if needed.
	currentState := w.statePair.GetCurrentState()
	if oldWAL := currentState.GetWAL(); oldWAL != nil {
		oldWAL.Close()
		w.logger.Info("close current term wal done at wal life time close")
	}
	w.logger.Info("wal lifetime closed")
}

// backgroundTask is the background task for wal manager.
// wal open/close operation is executed in background task with single goroutine.
func (w *walLifetime) backgroundTask() {
	defer func() {
		w.logger.Info("wal lifetime background task exit")
		close(w.finish)
	}()

	// wait for expectedState change.
	expectedState := initialExpectedWALState
	for {
		// single wal open/close operation should be serialized.
		if err := w.statePair.WaitExpectedStateChanged(context.Background(), expectedState); err != nil {
			// context canceled.
			return
		}
		expectedState = w.statePair.GetExpectedState()
		w.logger.Info("expected state changed, do a life cycle", zap.String("expected", toStateString(expectedState)))
		w.doLifetimeChanged(expectedState)
	}
}

// doLifetimeChanged executes the wal open/close operation once.
func (w *walLifetime) doLifetimeChanged(expectedState expectedWALState) {
	currentState := w.statePair.GetCurrentState()

	logger := w.logger.With(zap.String("expected", toStateString(expectedState)), zap.String("current", toStateString(currentState)))

	// filter the expired expectedState.
	if !isStateBefore(currentState, expectedState) {
		logger.Info("current state is not before expected state, do nothing")
		return
	}

	// must be term increasing or available -> unavailable, close current term wal whatever.
	term := currentState.Term()
	if oldWAL := currentState.GetWAL(); oldWAL != nil {
		oldWAL.Close()
		logger.Info("close current term wal done")
	}

	// if expected state is unavailable, change term to expected state and return.
	if !expectedState.Available() {
		w.statePair.SetCurrentState(unavailableCurrentWALState{
			term: expectedState.Term(),
			err:  nil,
		})
		return
	}

	// otherwise, push term to current state unavailable and open a new wal.
	w.statePair.SetCurrentState(unavailableCurrentWALState{
		term: term,
		err:  nil,
	})

	// If expected state is available, open a new wal.
	// TODO: merge the expectedState and expected state context together.
	l, err := w.opener.Open(expectedState.Context(), &wal.OpenOption{})
	if err != nil {
		logger.Warn("open new wal fail", zap.Error(err))
		w.statePair.SetCurrentState(unavailableCurrentWALState{
			term: expectedState.Term(),
			err:  err,
		})
		return
	}
	logger.Info("open new wal done")
	w.statePair.SetCurrentState(availableCurrentWALState{
		l: l,
	})
}
