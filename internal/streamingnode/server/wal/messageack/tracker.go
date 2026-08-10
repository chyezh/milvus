package messageack

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
)

type Tracker struct {
	mu             sync.Mutex
	completedPoint utility.WALConsumeCheckpoint
	pending        []Record
	onAdvance      func(utility.WALConsumeCheckpoint)
}

func NewTracker(initial utility.WALConsumeCheckpoint, onAdvance func(utility.WALConsumeCheckpoint)) *Tracker {
	return &Tracker{
		completedPoint: initial,
		onAdvance:      onAdvance,
	}
}

func (t *Tracker) Track(point utility.WALConsumeCheckpoint) Record {
	record := NewRecord(point, t.advanceCompletedPrefix)
	t.mu.Lock()
	t.pending = append(t.pending, record)
	t.mu.Unlock()
	return record
}

func (t *Tracker) CompletedPoint() utility.WALConsumeCheckpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return *t.completedPoint.Clone()
}

func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func (t *Tracker) advanceCompletedPrefix() {
	t.mu.Lock()
	completed := 0
	for completed < len(t.pending) && t.pending[completed].Completed() {
		completed++
	}
	if completed == 0 {
		t.mu.Unlock()
		return
	}
	point := t.pending[completed-1].Point()
	clear(t.pending[:completed])
	t.pending = t.pending[completed:]
	t.completedPoint = point
	onAdvance := t.onAdvance
	t.mu.Unlock()
	if onAdvance != nil {
		onAdvance(point)
	}
}
