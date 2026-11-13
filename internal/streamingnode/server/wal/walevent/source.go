package walevent

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Source serializes accepted inputs, applies them locally, and exposes the
// applied prefix to subscribers. A source has exactly one lifetime per WAL
// term. It must not be recreated at Seq zero after a publisher failure.
//
// The local path calls Consumer.Apply directly; it does not serialize or make
// an RPC to itself. Remote readers use exactly the same Consumer.Apply contract.
type Source struct {
	ctx      context.Context
	cancel   context.CancelFunc
	gate     chan struct{} // serializes publication and metadata cuts
	consumer *Consumer
	mu       sync.Mutex // journal, closure and subscription boundary
	changed  chan struct{}
	closed   error
	applied  Cursor
	journal  []journalEntry
	start    int
	count    int
	bytes    int
	maxBytes int
}

type journalEntry struct {
	event Event
	bytes int
}

func NewSource(term int64, maxEvents, maxBytes int, apply Applier) *Source {
	if term <= 0 || maxEvents <= 0 || maxBytes <= 0 {
		panic("WAL event source requires positive term and journal bounds")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Source{
		ctx: ctx, cancel: cancel,
		gate: make(chan struct{}, 1), consumer: NewConsumer(Cursor{Term: term}, apply),
		changed: make(chan struct{}), applied: Cursor{Term: term},
		journal: make([]journalEntry, maxEvents), maxBytes: maxBytes,
	}
}

// Publish freezes producer-owned metadata before it enters the ordered stream.
// The producer context controls admission only. Once accepted, the event uses
// the source lifetime: losing a control RPC must not interrupt a local Apply.
// The applier must not synchronously publish another event on this source;
// persistence workers submit completion facts after releasing their locks.
func (s *Source) Publish(ctx context.Context, payload Payload) (Cursor, error) {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return Cursor{}, ctx.Err()
	}
	s.mu.Lock()
	cursor, err := s.applied, s.closed
	s.mu.Unlock()
	if err != nil {
		return cursor, err
	}
	if err := ctx.Err(); err != nil {
		return cursor, err
	}
	frozen, size := payload.freeze()
	cursor.Seq++
	event := Event{Cursor: cursor, Payload: frozen}
	if err := s.consumer.Apply(s.ctx, event); err != nil {
		s.closeWithError(err)
		return Cursor{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Close can race a local Apply. Never export a partially closed source.
	if s.closed != nil {
		return Cursor{}, s.closed
	}
	s.applied = cursor
	s.appendLocked(event, size+32)
	close(s.changed)
	s.changed = make(chan struct{})
	return cursor, nil
}

// Cut captures immutable metadata and registers its live suffix under the same
// publication gate. Capture must only freeze local metadata and retain existing
// resource references; it must not perform I/O or await query readiness. The
// caller owns and releases those references, including on subscription failure.
// The callback receives AppliedSeq, never an unapplied producer head.
func (s *Source) Cut(ctx context.Context, capture func(Cursor) error) (*Reader, error) {
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	cursor, err := s.applied, s.closed
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := capture(cursor); err != nil {
		return nil, err
	}
	return s.Subscribe(cursor)
}

// Subscribe resumes strictly after an applied cursor. Validation shares the
// journal lock with append and eviction. Readers use the same journal for both
// history and live events, so there is no history-to-live registration gap.
func (s *Source) Subscribe(after Cursor) (*Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCursorLocked(after); err != nil {
		return nil, err
	}
	return &Reader{source: s, after: after, done: make(chan struct{})}, nil
}

func (s *Source) checkCursorLocked(after Cursor) error {
	if s.closed != nil {
		return s.closed
	}
	if after.Term != s.applied.Term || after.Seq > s.applied.Seq {
		return newResetError(s.applied, after, "cursor does not belong to the applied prefix")
	}
	if s.applied.Seq-after.Seq > uint64(s.count) {
		return newResetError(s.applied, after, "event history was evicted")
	}
	return nil
}

func (s *Source) appendLocked(event Event, size int) {
	for s.count > 0 && (s.count == len(s.journal) || s.bytes+size > s.maxBytes) {
		s.bytes -= s.journal[s.start].bytes
		s.journal[s.start] = journalEntry{}
		s.start = (s.start + 1) % len(s.journal)
		s.count--
	}
	// An oversized event still applies locally, but is not retained. Lagging
	// readers reconcile through a baseline/replay instead of pinning unbounded
	// memory or blocking checkpoint advancement.
	if size > s.maxBytes {
		return
	}
	index := (s.start + s.count) % len(s.journal)
	s.journal[index] = journalEntry{event: event, bytes: size}
	s.count++
	s.bytes += size
}

func (s *Source) Close() {
	s.closeWithError(merr.WrapErrServiceUnavailable("WAL event source closed"))
}

func (s *Source) closeWithError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed != nil {
		return
	}
	s.closed = err
	s.cancel()
	clear(s.journal)
	s.count, s.bytes = 0, 0
	close(s.changed)
}

// Reader has no private event queue and never pins journal history. Once its
// unread prefix is evicted, Next returns ResetError instead of skipping events.
type Reader struct {
	mu     sync.Mutex // serializes Next calls
	source *Source
	after  Cursor
	done   chan struct{}
	once   sync.Once
}

func (r *Reader) Next(ctx context.Context) (Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-r.done:
			return Event{}, merr.WrapErrServiceUnavailable("WAL event reader closed")
		default:
		}
		s := r.source
		s.mu.Lock()
		if err := s.checkCursorLocked(r.after); err != nil {
			s.mu.Unlock()
			return Event{}, err
		}
		if r.after.Seq < s.applied.Seq {
			offset := uint64(s.count) - (s.applied.Seq - r.after.Seq)
			event := s.journal[(s.start+int(offset))%len(s.journal)].event
			r.after = event.Cursor
			s.mu.Unlock()
			return event, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-r.done:
			return Event{}, merr.WrapErrServiceUnavailable("WAL event reader closed")
		case <-changed:
		}
	}
}

func (r *Reader) Close() { r.once.Do(func() { close(r.done) }) }
