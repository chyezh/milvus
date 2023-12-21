package walmanager

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
)

// newWALManagerExecutor creates a new walManageExecutor for a single channel.
func newWALManagerExecutor(opener wal.Opener, channel string) *walManageExecutor {
	return &walManageExecutor{
		opener:    opener,
		channel:   channel,
		lifetime:  lifetime.NewLifetime(lifetime.Working),
		logger:    log.With(zap.String("channel", channel)),
		actionsCh: make(chan *walManageAction, 10),
		finishCh:  make(chan struct{}),
		mu:        sync.Mutex{},
		wal:       nil,
	}
}

// walManageExecutor is the action for wal.
// All Write(Management) Operation for a wal should be serialized with order of term.
// And Remove operation should be executed after Open or compact like nothing happened.
type walManageExecutor struct {
	opener  wal.Opener
	channel string

	lifetime  lifetime.Lifetime[lifetime.State]
	logger    *log.MLogger
	actionsCh chan *walManageAction
	finishCh  chan struct{}
	mu        sync.Mutex
	wal       wal.WAL
}

// GetWAL returns a available wal instance for the channel.
func (wma *walManageExecutor) GetWAL() (wal.WAL, error) {
	if wma.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal manager executor is closed")
	}
	defer wma.lifetime.Done()

	return wma.getWAL(), nil
}

// Open opens a wal instance for the channel on this Manager.
func (wma *walManageExecutor) Open(ctx context.Context, opt *wal.OpenOption) error {
	if wma.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("wal manager executor is closed")
	}
	defer wma.lifetime.Done()

	return wma.request(ctx, &walManageAction{
		ctx:         ctx,
		action:      walManageActionOpen,
		term:        opt.Channel.Term,
		allocateOpt: opt,
		result:      make(chan error, 1),
	})
}

// Remove removes the wal instance for the channel on this Manager.
func (wma *walManageExecutor) Remove(ctx context.Context, term int64) error {
	if wma.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("wal manager executor is closed")
	}
	defer wma.lifetime.Done()

	return wma.request(ctx, &walManageAction{
		ctx:    ctx,
		action: walManageActionRemove,
		term:   term,
		result: make(chan error, 1),
	})
}

// close closes the walManageExecutor.
func (wma *walManageExecutor) Close() {
	wma.lifetime.SetState(lifetime.Stopped)
	wma.lifetime.Wait()
	// close the action channel, so stop background task.
	close(wma.actionsCh)
	<-wma.finishCh
}

// request is the internal method for open and remove.
func (wma *walManageExecutor) request(ctx context.Context, req *walManageAction) error {
	wma.actionsCh <- req
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-req.result:
		return err
	}
}

// start background task for walManageExecutor.
func (wma *walManageExecutor) start() {
	go wma.eventLoop()
}

// eventLoop is the background task for walManageExecutor.
func (wma *walManageExecutor) eventLoop() {
	defer func() {
		oldWAL := wma.getWAL()
		// Close the wal if it is not closed before executor is closed.
		if oldWAL != nil {
			oldWAL.Close()
		}
		close(wma.finishCh)
		wma.logger.Info("wal manage executor stopped")
	}()

	wma.logger.Info("wal manage executor started...")
	for {
		action, ok := <-wma.actionsCh
		if !ok {
			return
		}
		// Collect all pending actions for this .
		actions := wma.getSortedWalManageActions(action)

		// Ignore all old actions
		for i := 0; i < len(actions)-1; i++ {
			actions[i].result <- status.NewIgnoreOperation("high term operation is coming")
		}

		// Apply last action to reach the final state of a wal (Open or Closed).
		// This is a blocking operation, may cost a long time for underlying wal recovery.
		// So we apply a operation merge before these action.
		lastAction := actions[len(actions)-1]
		lastAction.result <- wma.applyAction(lastAction)
	}
}

func (wma *walManageExecutor) getSortedWalManageActions(firstAction *walManageAction) []*walManageAction {
	actions := make([]*walManageAction, 0, len(wma.actionsCh)+1)
	actions = append(actions, firstAction)
L:
	for {
		select {
		case action, ok := <-wma.actionsCh:
			if !ok {
				break
			}
			actions = append(actions, action)
		default:
			break L
		}
	}
	// sort by term and action type.
	sort.Sort(walManageActions(actions))
	// Get the last action for last term.
	return actions
}

// getWAL returns the wal instance.
func (wma *walManageExecutor) getWAL() wal.WAL {
	wma.mu.Lock()
	l := wma.wal
	wma.mu.Unlock()
	return l
}

// swap swaps the wal instance.
func (wma *walManageExecutor) swap(wal wal.WAL) wal.WAL {
	wma.mu.Lock()
	oldWAL := wma.wal
	wma.wal = wal
	wma.mu.Unlock()
	return oldWAL
}

// applyAction applies the action to the wal.
func (wma *walManageExecutor) applyAction(action *walManageAction) error {
	oldWAL := wma.getWAL()
	logger := wma.logger.With(zap.Int64("newTerm", action.term))
	if oldWAL != nil {
		logger = logger.With(zap.Int64("oldTerm", oldWAL.Channel().Term))
	}

	// Skip operation if action term is too low.
	if oldWAL != nil && oldWAL.Channel().Term > action.term {
		err := status.NewIgnoreOperation(fmt.Sprintf("high term operation is already applied, %d > %d", oldWAL.Channel().Term, action.term))
		logger.Info("ignore operation", zap.Error(err))
		return err
	}

	switch action.action {
	case walManageActionOpen:
		if oldWAL != nil {
			if oldWAL.Channel().Term == action.term {
				err := status.NewIgnoreOperation(fmt.Sprintf("wal is already opened for term %d", action.term))
				logger.Info("ignore operation", zap.Error(err))
				return err
			}
			// Close the old term wal before open it.
			oldWAL.Close()
		}

		// open new wal at new term
		l, err := wma.opener.Open(context.TODO(), action.allocateOpt)
		if err != nil {
			return err
		}
		// swap the wal instance.
		_ = wma.swap(l)
	case walManageActionRemove:
		if oldWAL == nil {
			err := status.NewIgnoreOperation(fmt.Sprintf("wal is already removed for term %d", action.term))
			logger.Info("ignore operation", zap.Error(err))
			return err
		}
		oldWAL.Close()
		_ = wma.swap(nil)
	}
	return nil
}
