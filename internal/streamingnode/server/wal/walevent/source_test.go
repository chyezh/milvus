package walevent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testCheckpoint(tt uint64) Payload {
	return CheckpointPublished{Checkpoint: &streamingpb.WALCheckpoint{TimeTick: tt}}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSourceReaderBudget(t *testing.T) {
	require.Panics(t, func() { NewSource(1, 1, 1, nil, WithMaxReaders(0)) })
	s := NewSource(7, 2, 1024, func(context.Context, Event) error { return nil }, WithMaxReaders(2))
	defer s.Close()
	first, err := s.Subscribe(Cursor{Term: 7})
	require.NoError(t, err)
	second, err := s.Cut(t.Context(), func(Cursor) error { return nil })
	require.NoError(t, err)
	_, err = s.Subscribe(Cursor{Term: 7})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = s.Cut(t.Context(), func(Cursor) error { t.Fatal("exhausted reader budget must reject before copying metadata"); return nil })
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	// Capacity pressure never blocks the primary's local Apply.
	cursor, err := s.Publish(t.Context(), testCheckpoint(10))
	require.NoError(t, err)
	first.Close()
	first.Close()
	replacement, err := s.Subscribe(cursor)
	require.NoError(t, err)
	_, err = s.Subscribe(cursor)
	require.Error(t, err, "double Close must not free another reader's slot")
	second.Close()
	replacement.Close()
	require.Zero(t, s.readers)
	s.Close()
	_, err = s.Subscribe(cursor)
	require.Error(t, err)
}

func TestSourceCloseDuringMetadataCaptureReleasesReader(t *testing.T) {
	s := NewSource(7, 1, 1024, func(context.Context, Event) error { return nil }, WithMaxReaders(1))
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		reader, err := s.Cut(t.Context(), func(Cursor) error { close(entered); <-release; return nil })
		if reader != nil {
			reader.Close()
		}
		done <- err
	}()
	<-entered
	s.Close()
	close(release)
	require.ErrorIs(t, <-done, merr.ErrServiceUnavailable)
	require.Zero(t, s.readers)
}

func TestSourceLocalAndRemotePrefix(t *testing.T) {
	ctx := testContext(t)
	var local, remote []uint64
	apply := func(dst *[]uint64) Applier {
		return func(_ context.Context, e Event) error {
			*dst = append(*dst, e.Payload.(CheckpointPublished).Checkpoint.TimeTick)
			return nil
		}
	}
	s := NewSource(7, 512, 1<<20, apply(&local))
	t.Cleanup(s.Close)
	c := NewConsumer(Cursor{Term: 7}, apply(&remote))
	r, err := s.Subscribe(c.Cursor())
	require.NoError(t, err)
	t.Cleanup(r.Close)
	// A concurrent producer cannot assign a cursor independently of Apply.
	var wg sync.WaitGroup
	results := make(chan error, 200)
	for producer := 0; producer < 4; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, err := s.Publish(ctx, testCheckpoint(uint64(producer*50+i)))
				results <- err
			}
		}(producer)
	}
	for i := 0; i < 200; i++ {
		e, err := r.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, Cursor{Term: 7, Seq: uint64(i + 1)}, e.Cursor)
		require.NoError(t, c.Apply(ctx, e))
		// A replayed response must not apply twice.
		require.NoError(t, c.Apply(ctx, e))
		if i == 75 {
			// Receive one response, then lose the transport before applying it.
			_, err := r.Next(ctx)
			require.NoError(t, err)
			r.Close()
			r, err = s.Subscribe(c.Cursor())
			require.NoError(t, err)
			defer r.Close()
		}
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, local, remote)
	require.Equal(t, Cursor{Term: 7, Seq: 200}, c.Cursor())
}

func TestSourceHistoryBounds(t *testing.T) {
	ctx := testContext(t)
	for _, bounds := range []struct {
		name   string
		events int
		bytes  int
	}{{"count", 2, 1 << 20}, {"bytes", 100, 70}} {
		t.Run(bounds.name, func(t *testing.T) {
			s := NewSource(7, bounds.events, bounds.bytes, func(context.Context, Event) error { return nil })
			defer s.Close()
			slow, err := s.Subscribe(Cursor{Term: 7})
			require.NoError(t, err)
			defer slow.Close()
			for i := 1; i <= 8; i++ {
				_, err := s.Publish(ctx, testCheckpoint(uint64(i)))
				require.NoError(t, err)
			}
			var reset *ResetError
			_, err = slow.Next(ctx)
			require.ErrorAs(t, err, &reset)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.Contains(t, err.Error(), "evicted")
			_, err = s.Subscribe(Cursor{Term: 7})
			require.ErrorAs(t, err, &reset)
			_, err = s.Subscribe(Cursor{Term: 8})
			require.ErrorAs(t, err, &reset)
			_, err = s.Subscribe(Cursor{Term: 7, Seq: 9})
			require.ErrorAs(t, err, &reset)
			r, err := s.Subscribe(Cursor{Term: 7, Seq: 6})
			require.NoError(t, err)
			defer r.Close()
			for i := uint64(7); i <= 8; i++ {
				e, err := r.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, i, e.Cursor.Seq)
			}
			require.LessOrEqual(t, s.bytes, bounds.bytes)
		})
	}
}

func TestOversizedEventDoesNotBlockLocalApply(t *testing.T) {
	ctx := testContext(t)
	applied := 0
	s := NewSource(7, 2, 35, func(context.Context, Event) error { applied++; return nil })
	defer s.Close()
	_, err := s.Publish(ctx, testCheckpoint(1))
	require.NoError(t, err)
	_, err = s.Publish(ctx, QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{{
		Meta: &viewpb.QueryViewMeta{Vchannel: "a-view-too-large-for-the-journal"},
	}}})
	require.NoError(t, err)
	require.Equal(t, 2, applied)
	require.Zero(t, s.bytes)
	_, err = s.Subscribe(Cursor{Term: 7, Seq: 1})
	var reset *ResetError
	require.ErrorAs(t, err, &reset)
	r, err := s.Subscribe(Cursor{Term: 7, Seq: 2})
	require.NoError(t, err)
	defer r.Close()
	_, err = s.Publish(ctx, testCheckpoint(3))
	require.NoError(t, err)
	e, err := r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), e.Cursor.Seq)
}

func TestSourceCutUsesAppliedPrefix(t *testing.T) {
	ctx := testContext(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var state uint64
	s := NewSource(8, 10, 1<<20, func(_ context.Context, e Event) error {
		if e.Cursor.Seq == 1 {
			close(entered)
			<-release
		}
		state = e.Cursor.Seq
		return nil
	})
	defer s.Close()
	published := make(chan error, 1)
	go func() { _, err := s.Publish(ctx, testCheckpoint(1)); published <- err }()
	<-entered
	// Seq 1 is assigned but not applied: no remote reader may observe it yet.
	_, err := s.Subscribe(Cursor{Term: 8, Seq: 1})
	var reset *ResetError
	require.ErrorAs(t, err, &reset)
	close(release)
	require.NoError(t, <-published)
	cutEntered, releaseCut := make(chan struct{}), make(chan struct{})
	cutReader := make(chan *Reader, 1)
	cutError := make(chan error, 1)
	go func() {
		r, err := s.Cut(ctx, func(cursor Cursor) error {
			if cursor != (Cursor{Term: 8, Seq: 1}) || state != 1 {
				return merr.WrapErrServiceInternalMsg("snapshot captured an unapplied event")
			}
			close(cutEntered)
			<-releaseCut
			return nil
		})
		cutReader <- r
		cutError <- err
	}()
	<-cutEntered
	go func() { _, err := s.Publish(ctx, testCheckpoint(2)); published <- err }()
	close(releaseCut)
	r := <-cutReader
	require.NoError(t, <-cutError)
	defer r.Close()
	require.NoError(t, <-published)
	e, err := r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, Cursor{Term: 8, Seq: 2}, e.Cursor)
}

func TestConsumerFailureAndCursorValidation(t *testing.T) {
	ctx := testContext(t)
	failure := merr.WrapErrServiceInternalMsg("partial apply failed")
	calls := 0
	c := NewConsumer(Cursor{Term: 8, Seq: 3}, func(context.Context, Event) error {
		calls++
		return failure
	})
	var reset *ResetError
	for _, cursor := range []Cursor{{Term: 7, Seq: 4}, {Term: 8, Seq: 5}} {
		err := c.Apply(ctx, Event{Cursor: cursor})
		require.ErrorAs(t, err, &reset)
	}
	require.NoError(t, c.Apply(ctx, Event{Cursor: Cursor{Term: 8, Seq: 2}}))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, c.Apply(canceled, Event{Cursor: Cursor{Term: 8, Seq: 4}}), context.Canceled)
	require.Zero(t, calls)
	require.ErrorIs(t, c.Apply(ctx, Event{Cursor: Cursor{Term: 8, Seq: 4}}), failure)
	require.ErrorIs(t, c.Apply(ctx, Event{Cursor: Cursor{Term: 8, Seq: 4}}), failure)
	require.Equal(t, 1, calls)
	require.Equal(t, Cursor{Term: 8, Seq: 3}, c.Cursor())
}

func TestSourceCancellationAndClosure(t *testing.T) {
	ctx := testContext(t)
	s := NewSource(7, 4, 1024, func(context.Context, Event) error { return nil })
	defer s.Close()
	r, err := s.Subscribe(Cursor{Term: 7})
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = r.Next(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.Publish(canceled, testCheckpoint(1))
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.Cut(canceled, func(Cursor) error { t.Fatal("canceled cut"); return nil })
	require.ErrorIs(t, err, context.Canceled)
	failure := merr.WrapErrServiceInternalMsg("capture failed")
	_, err = s.Cut(ctx, func(Cursor) error { return failure })
	require.ErrorIs(t, err, failure)
	nextErr := make(chan error, 1)
	go func() { _, err := r.Next(ctx); nextErr <- err }()
	r.Close()
	r.Close()
	require.ErrorIs(t, <-nextErr, merr.ErrServiceUnavailable)
	s.Close()
	s.Close()
	_, err = s.Subscribe(Cursor{Term: 7})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = s.Publish(ctx, testCheckpoint(1))
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	_, err = s.Cut(ctx, func(Cursor) error { t.Fatal("closed cut"); return nil })
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestSourceFailureClosesReaders(t *testing.T) {
	ctx := testContext(t)
	failure := merr.WrapErrServiceInternalMsg("apply failed")
	s := NewSource(7, 4, 1024, func(context.Context, Event) error { return failure })
	defer s.Close()
	r, err := s.Subscribe(Cursor{Term: 7})
	require.NoError(t, err)
	defer r.Close()
	_, err = s.Publish(ctx, testCheckpoint(1))
	require.ErrorIs(t, err, failure)
	_, err = r.Next(ctx)
	require.ErrorIs(t, err, failure)
	require.False(t, errors.As(err, new(*ResetError)))
}

func TestSourceCloseDuringApply(t *testing.T) {
	ctx := testContext(t)
	var s *Source
	s = NewSource(7, 4, 1024, func(context.Context, Event) error { s.Close(); return nil })
	_, err := s.Publish(ctx, testCheckpoint(1))
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Panics(t, func() { NewSource(0, 1, 1, nil) })
	require.Panics(t, func() { NewSource(1, 0, 1, nil) })
	require.Panics(t, func() { NewSource(1, 1, 0, nil) })
}

func TestAcceptedEventSurvivesProducerCancellation(t *testing.T) {
	ctx := testContext(t)
	producerCtx, cancelProducer := context.WithCancel(ctx)
	defer cancelProducer()
	entered, release := make(chan struct{}), make(chan struct{})
	s := NewSource(7, 4, 1024, func(ctx context.Context, _ Event) error {
		close(entered)
		<-release
		return ctx.Err()
	})
	defer s.Close()
	result := make(chan error, 1)
	go func() { _, err := s.Publish(producerCtx, testCheckpoint(1)); result <- err }()
	<-entered
	cancelProducer()
	close(release)
	require.NoError(t, <-result)
	r, err := s.Subscribe(Cursor{Term: 7})
	require.NoError(t, err)
	defer r.Close()
	e, err := r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, Cursor{Term: 7, Seq: 1}, e.Cursor)
}

func TestClosingSourceCancelsInFlightApply(t *testing.T) {
	ctx := testContext(t)
	entered := make(chan struct{})
	s := NewSource(7, 4, 1024, func(ctx context.Context, _ Event) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	result := make(chan error, 1)
	go func() { _, err := s.Publish(ctx, testCheckpoint(1)); result <- err }()
	<-entered
	s.Close()
	require.ErrorIs(t, <-result, context.Canceled)
}
