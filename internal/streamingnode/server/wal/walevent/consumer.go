package walevent

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Applier updates query-facing logical state. It may schedule asynchronous
// resource preparation, but must not wait for local QueryView readiness.
// A failed Apply may have changed state: the consumer becomes terminal and
// must be replaced together with its reconciled baseline before trying again.
type Applier func(context.Context, Event) error

// Consumer enforces the same prefix contract for local and remote sources.
// Its lifetime is a logical baseline, not a network subscription. A reconnect
// reuses it; installing a different term requires an explicitly reconciled
// baseline and a new consumer, without discarding unrelated query resources.
type Consumer struct {
	mu      sync.Mutex
	cursor  Cursor
	apply   Applier
	failure error
}

func NewConsumer(baseline Cursor, apply Applier) *Consumer {
	return &Consumer{cursor: baseline, apply: apply}
}

func (c *Consumer) Cursor() Cursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

// Err returns a terminal application failure. Transport errors and rejected
// cursors leave the installed prefix intact and do not poison the consumer.
func (c *Consumer) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

// Apply discards duplicate events in the same term and rejects gaps and term
// changes before invoking the applier. Only a successful Apply advances Seq.
func (c *Consumer) Apply(ctx context.Context, event Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.Cursor.Term != c.cursor.Term {
		return newResetError(c.cursor, event.Cursor, "WAL term changed")
	}
	if event.Cursor.Seq <= c.cursor.Seq {
		return nil
	}
	if event.Cursor.Seq != c.cursor.Seq+1 {
		return newResetError(c.cursor, event.Cursor, "event sequence gap")
	}
	if err := c.apply(ctx, event); err != nil {
		c.failure = merr.Wrap(err, "apply WAL replica event")
		return c.failure
	}
	c.cursor = event.Cursor
	return nil
}

// ResetError asks the subscription owner to reconcile a metadata baseline and
// WAL replay range. It is a transport outcome, not an event or a request to
// withdraw the replica's existing read service.
type ResetError struct {
	Applied   Cursor
	Requested Cursor
	cause     error
}

func (e *ResetError) Error() string { return e.cause.Error() }

func (e *ResetError) Unwrap() error { return e.cause }

// NewResumeRequired reconstructs a transport resume rejection. Applied is the
// local cursor; Requested identifies the primary's current baseline candidate.
func NewResumeRequired(applied, current Cursor) *ResetError {
	return newResetError(applied, current, "primary no longer retains the requested event prefix")
}

func newResetError(applied, requested Cursor, reason string) *ResetError {
	return &ResetError{
		Applied: applied, Requested: requested,
		cause: merr.WrapErrServiceUnavailable(
			"WAL replica subscription needs a baseline: " + reason),
	}
}
