package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
)

type WALLifetime struct {
	opener  wal.Opener
	channel string
	logger  *log.MLogger
	wal     *walWithState
}

// GetWAL returns a available wal instance for the channel.
func (w *WALLifetime) GetWAL() wal.WAL {
	return w.wal.GetWAL()
}

// Open opens a wal instance for the channel on this Manager.
func (w *WALLifetime) Open(ctx context.Context, opt *wal.OpenOption) error {
	expectedState := walState{
		term:      opt.Channel.Term,
		available: true,
	}
	w.wal.SetExpectedState(expectedState)
	w.wal.WaitCurrentStateReachExpected(ctx, expectedState)
	return nil
}

// Remove removes the wal instance for the channel on this Manager.
func (w *WALLifetime) Remove(ctx context.Context, term int64) error {
}
