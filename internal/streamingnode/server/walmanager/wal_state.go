package walmanager

import (
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type currentWALState struct {
	l atomic.Pointer[wal.WAL]
	types.PChannelAssignState
}

func (s *currentWALState) GetWAL() wal.WAL {
	l := s.l.Load()
	if l == nil {
		return nil
	}
	return *l
}

func (s *currentWALState) MarkAsAvailable(l wal.WAL) {
	s.PChannelAssignState.Available = true
	s.l.Store(&l)
}

func (s *currentWALState) MarkAsUnavailable() {
	s.PChannelAssignState.Available = false
	s.l.Store(nil)
}

func (s *currentWALState) SetCurrentState(state types.PChannelAssignState) {
	s.PChannelAssignState = state
}
