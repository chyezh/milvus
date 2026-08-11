package messageack

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type trackedEntry struct {
	point      utility.WALConsumeCheckpoint
	completed  bool
	controller message.RefCountedImmutableMessageController
}

type Tracker struct {
	mu             sync.Mutex
	completedPoint utility.WALConsumeCheckpoint
	pending        []*trackedEntry
	onAdvance      func(utility.WALConsumeCheckpoint)
}

func NewTracker(initial utility.WALConsumeCheckpoint, onAdvance func(utility.WALConsumeCheckpoint)) *Tracker {
	return &Tracker{
		completedPoint: initial,
		onAdvance:      onAdvance,
	}
}

func (t *Tracker) Track(raw message.ImmutableMessage) message.RefCountedImmutableMessageController {
	entry := &trackedEntry{
		point: utility.WALConsumeCheckpoint{
			MessageID: raw.LastConfirmedMessageID(),
			TimeTick:  raw.TimeTick(),
		},
	}
	controller := message.NewRefCountedImmutableMessage(raw, func() {
		t.complete(entry)
	})
	entry.controller = controller

	t.mu.Lock()
	t.pending = append(t.pending, entry)
	t.mu.Unlock()
	return controller
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

func (t *Tracker) complete(entry *trackedEntry) {
	t.mu.Lock()
	if entry.completed {
		t.mu.Unlock()
		return
	}
	entry.completed = true
	entry.controller = nil

	completed := 0
	for completed < len(t.pending) && t.pending[completed].completed {
		completed++
	}
	if completed == 0 {
		t.mu.Unlock()
		return
	}
	point := *t.pending[completed-1].point.Clone()
	clear(t.pending[:completed])
	t.pending = t.pending[completed:]
	t.completedPoint = point
	onAdvance := t.onAdvance
	t.mu.Unlock()
	if onAdvance != nil {
		onAdvance(point)
	}
}
