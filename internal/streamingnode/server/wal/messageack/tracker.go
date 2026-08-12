package messageack

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type trackedEntry struct {
	point                 utility.WALConsumeCheckpoint
	message               message.ImmutableMessage
	consumersDone         chan struct{}
	consumersCompleted    bool
	broadcastAckRequired  bool
	broadcastAckCompleted bool
	completed             bool
}

// TrackedMessage exposes the message-level completion events needed by
// RecoveryStorage sinks without exposing the tracked entry itself.
type TrackedMessage struct {
	tracker *Tracker
	entry   *trackedEntry
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

func (t *Tracker) Track(raw message.ImmutableMessage) (message.OwnedImmutableMessage, *TrackedMessage) {
	entry := &trackedEntry{
		point: utility.WALConsumeCheckpoint{
			MessageID: raw.LastConfirmedMessageID(),
			TimeTick:  raw.TimeTick(),
		},
		message:               raw,
		consumersDone:         make(chan struct{}),
		broadcastAckRequired:  raw.BroadcastHeader() != nil,
		broadcastAckCompleted: raw.BroadcastHeader() == nil,
	}
	owner := message.NewOwnedImmutableMessage(raw, func() {
		t.completeConsumers(entry)
	})

	t.mu.Lock()
	t.pending = append(t.pending, entry)
	t.mu.Unlock()
	return owner, &TrackedMessage{tracker: t, entry: entry}
}

func (m *TrackedMessage) Message() message.ImmutableMessage {
	m.tracker.mu.Lock()
	defer m.tracker.mu.Unlock()
	if m.entry.message == nil {
		panic("tracked message accessed after completion")
	}
	return m.entry.message
}

func (m *TrackedMessage) ConsumersDone() <-chan struct{} {
	return m.entry.consumersDone
}

// RequiresBroadcastAck reports whether the tracked message requires a
// coordinator-side broadcast acknowledgement.
func (m *TrackedMessage) RequiresBroadcastAck() bool {
	m.tracker.mu.Lock()
	defer m.tracker.mu.Unlock()
	return m.entry.broadcastAckRequired
}

func (m *TrackedMessage) CompleteBroadcastAck() {
	m.tracker.completeBroadcastAck(m.entry)
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

func (t *Tracker) completeConsumers(entry *trackedEntry) {
	t.mu.Lock()
	if entry.consumersCompleted {
		t.mu.Unlock()
		return
	}
	entry.consumersCompleted = true
	close(entry.consumersDone)
	onAdvance, point, advanced := t.completeLocked(entry)
	t.mu.Unlock()
	if advanced && onAdvance != nil {
		onAdvance(point)
	}
}

func (t *Tracker) completeBroadcastAck(entry *trackedEntry) {
	t.mu.Lock()
	if entry.broadcastAckCompleted {
		t.mu.Unlock()
		return
	}
	entry.broadcastAckCompleted = true
	onAdvance, point, advanced := t.completeLocked(entry)
	t.mu.Unlock()
	if advanced && onAdvance != nil {
		onAdvance(point)
	}
}

func (t *Tracker) completeLocked(entry *trackedEntry) (func(utility.WALConsumeCheckpoint), utility.WALConsumeCheckpoint, bool) {
	if entry.completed || !entry.consumersCompleted || !entry.broadcastAckCompleted {
		return nil, utility.WALConsumeCheckpoint{}, false
	}
	entry.completed = true

	completed := 0
	for completed < len(t.pending) && t.pending[completed].completed {
		completed++
	}
	if completed == 0 {
		return nil, utility.WALConsumeCheckpoint{}, false
	}
	point := *t.pending[completed-1].point.Clone()
	for _, completedEntry := range t.pending[:completed] {
		completedEntry.message = nil
	}
	clear(t.pending[:completed])
	t.pending = t.pending[completed:]
	t.completedPoint = point
	return t.onAdvance, point, true
}
