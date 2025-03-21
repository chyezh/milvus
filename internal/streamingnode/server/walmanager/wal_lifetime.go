package walmanager

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// newWALLifetime create a WALLifetime with opener.
func newWALLifetime(opener wal.Opener, channel string, logger *log.MLogger) *walLifetime {
	l := &walLifetime{
		backgroundNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		channel:            channel,
		applyWALCh:         make(chan applyWALState),
		opener:             opener,
		logger:             logger.With(zap.String("channel", channel)),
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
	backgroundNotifier *syncutil.AsyncTaskNotifier[struct{}]
	channel            string

	applyWALCh chan applyWALState
	opener     wal.Opener
	logger     *log.MLogger

	currentState currentWALState
}

// GetWAL returns a available wal instance for the channel.
// Return nil if the wal is not available now.
func (w *walLifetime) GetWAL() wal.WAL {
	return w.currentState.GetWAL()
}

// applyWALState is the request for apply wal operation.
type applyWALState struct {
	types.PChannelAssignState
	Reporter Reporter
}

// Apply applies the wal operation.
func (w *walLifetime) Apply(state applyWALState) {
	select {
	case <-w.backgroundNotifier.Context().Done():
		w.logger.Info("apply wal request ignored, because lifetime is closed", zap.String("request", state.String()))
	case w.applyWALCh <- state:
	}
}

// Close closes the wal lifetime.
func (w *walLifetime) Close() {
	// Close all background task.
	w.backgroundNotifier.Cancel()
	w.backgroundNotifier.BlockUntilFinish()

	// No background task is running now, close current wal if needed.
	logger := log.With(zap.String("current", w.currentState.String()))
	if oldWAL := w.currentState.GetWAL(); oldWAL != nil {
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
		w.backgroundNotifier.Finish(struct{}{})
	}()

	for {
		select {
		case <-w.backgroundNotifier.Context().Done():
			return
		case req := <-w.applyWALCh:
			w.doLifetimeChanged(req)
		}
	}
}

// doLifetimeChanged executes the wal open/close operation once.
func (w *walLifetime) doLifetimeChanged(expectedState applyWALState) {
	logger := w.logger.With(zap.String("expected", expectedState.String()), zap.String("current", w.currentState.String()))
	logger.Info("do lifetime changed")

	if !w.currentState.Before(expectedState.PChannelAssignState) {
		// Happen at: the unavailable expected state at current term, but current wal open operation is failed.
		logger.Info("current state is not before expected state, do nothing")
		expectedState.Reporter.Report(SyncResponse{
			States: []AssignWithError{{
				Channel: expectedState.PChannelAssignState,
				Err:     status.NewIgnoreOperation("current state %s is not before expected state %s", w.currentState.String(), expectedState.String()),
			}},
		})
		return
	}

	// term must be increasing or available -> unavailable, close current term wal is always applied.
	if oldWAL := w.currentState.GetWAL(); oldWAL != nil {
		oldWAL.Close()
		logger.Info("close current term wal done")
		// Push term to current state unavailable and open a new wal.
		// -> (currentTerm,false)
		w.currentState.MarkAsUnavailable()
	}

	w.currentState.SetCurrentState(expectedState.PChannelAssignState)
	if !expectedState.Available {
		expectedState.Reporter.Report(SyncResponse{
			States: []AssignWithError{{Channel: expectedState.PChannelAssignState, Err: nil}},
		})
		return
	}

	// If expected state is available, open a new wal.
	l, err := w.opener.Open(w.backgroundNotifier.Context(), &wal.OpenOption{Channel: expectedState.Channel})
	if err != nil {
		logger.Warn("open new wal fail", zap.Error(err))
		// Open new wal at expected term failed, push expected term to current state unavailable.
		// -> (expectedTerm,false)
		w.currentState.MarkAsUnavailable()
		expectedState.Reporter.Report(SyncResponse{States: []AssignWithError{{Channel: expectedState.PChannelAssignState, Err: err}}})
		return
	}
	logger.Info("open new wal done")
	// -> (expectedTerm,true)
	w.currentState.MarkAsAvailable(l)
	expectedState.Reporter.Report(SyncResponse{States: []AssignWithError{{Channel: expectedState.PChannelAssignState, Err: nil}}})
}
