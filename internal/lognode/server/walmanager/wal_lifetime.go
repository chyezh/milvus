package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// newWALLifetime create a WALLifetime with opener.
func newWALLifetime(opener wal.Opener, channel string, opt *OpenOption) *walLifetime {
	ctx, cancel := context.WithCancel(context.Background())
	l := &walLifetime{
		ctx:        ctx,
		cancel:     cancel,
		finish:     make(chan struct{}),
		opener:     opener,
		openOption: opt,
		statePair:  newWALStatePair(),
		logger:     log.With(zap.String("channel", channel)),
	}
	go l.backgroundTask()
	return l
}

// walLifetime is the lifetime management of a wal.
// It promise a wal is keep state consistency in distributed environment.
// All operation on wal management will be sorted with following rules:
// (term, available) illuminate the state of wal.
// term is always increasing, available is always before unavailable in same term, such as:
// (-1, false) -> (0, true) -> (1, true) -> (2, true) -> (3, false) -> (7, true) -> ...
type walLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc

	finish     chan struct{}
	opener     wal.Opener
	openOption *OpenOption
	statePair  *walStatePair
	logger     *log.MLogger
}

// GetWAL returns a available wal instance for the channel.
// Return nil if the wal is not available now.
func (w *walLifetime) GetWAL() wal.WAL {
	return w.statePair.GetWAL()
}

// Open opens a wal instance for the channel on this Manager.
func (w *walLifetime) Open(ctx context.Context, channel *logpb.PChannelInfo) error {
	// Set expected WAL state to available at given term.
	expected := newAvailableExpectedState(ctx, channel)
	if !w.statePair.SetExpectedState(expected) {
		return status.NewUnmatchedChannelTerm("expired term, cannot change expected state for open")
	}

	// Wait until the WAL state is ready or term expired or error occurs.
	return w.statePair.WaitCurrentStateReachExpected(ctx, expected)
}

// Remove removes the wal instance for the channel on this Manager.
func (w *walLifetime) Remove(ctx context.Context, term int64) error {
	// Set expected WAL state to unavailable at given term.
	expected := newUnavailableExpectedState(term)
	if !w.statePair.SetExpectedState(expected) {
		return status.NewUnmatchedChannelTerm("expired term, cannot change expected state for remove")
	}

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
	logger := log.With(zap.String("current", toStateString(currentState)))
	if oldWAL := currentState.GetWAL(); oldWAL != nil {
		oldWAL.Close()
		logger.Info("close current term wal done at wal life time close")
	}
	logger.Info("wal lifetime closed")
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
		if err := w.statePair.WaitExpectedStateChanged(w.ctx, expectedState); err != nil {
			// context canceled. break the background task.
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

	// Filter the expired expectedState.
	if !isStateBefore(currentState, expectedState) {
		// Happen at: the unavailable expected state at current term, but current wal open operation is failed.
		logger.Info("current state is not before expected state, do nothing")
		return
	}

	// !!! Even if the expected state is canceled (context.Context.Err()), the operation must be executed.
	// Otherwise a dead lock may be caused by unexpected rpc sequence.
	// New Current state after these operation must be same or greater than expected state.

	// Must be term increasing or available -> unavailable, close current term wal is always applied.
	term := currentState.Term()
	if oldWAL := currentState.GetWAL(); oldWAL != nil {
		oldWAL.Close()
		logger.Info("close current term wal done")
		// Push term to current state unavailable and open a new wal.
		// -> (currentTerm,false)
		w.statePair.SetCurrentState(newUnavailableCurrentState(term, nil))
	}

	// If expected state is unavailable, change term to expected state and return.
	if !expectedState.Available() {
		// -> (expectedTerm,false)
		w.statePair.SetCurrentState(newUnavailableCurrentState(expectedState.Term(), nil))
		return
	}

	// If expected state is available, open a new wal.
	// TODO: merge the expectedState and expected state context together.
	l, err := w.opener.Open(expectedState.Context(), &wal.OpenOption{
		Channel:             expectedState.GetPChannelInfo(),
		InterceptorBuilders: w.openOption.InterceptorBuilders,
	})
	if err != nil {
		logger.Warn("open new wal fail", zap.Error(err))
		// Open new wal at expected term failed, push expected term to current state unavailable.
		// -> (expectedTerm,false)
		w.statePair.SetCurrentState(newUnavailableCurrentState(expectedState.Term(), err))
		return
	}
	logger.Info("open new wal done")
	// -> (expectedTerm,true)
	w.statePair.SetCurrentState(newAvailableCurrentState(l))
}
