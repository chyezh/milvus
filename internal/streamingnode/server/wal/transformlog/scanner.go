package transformlog

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type scanner struct {
	log             *transformLog
	name            string
	startAfter      uint64
	end             uint64
	caughtUpTarget  uint64
	ch              chan wal.TransformLogEvent
	done            chan struct{}
	close           chan struct{}
	errMu           sync.Mutex
	err             error
	closed          sync.Once
	caughtUpEmitted bool
}

func newScanner(log *transformLog, name string, startAfter uint64, end uint64, caughtUpTarget uint64) *scanner {
	return &scanner{
		log:            log,
		name:           name,
		startAfter:     startAfter,
		end:            end,
		caughtUpTarget: caughtUpTarget,
		ch:             make(chan wal.TransformLogEvent, 16),
		done:           make(chan struct{}),
		close:          make(chan struct{}),
	}
}

func (s *scanner) Name() string {
	return s.name
}

func (s *scanner) Chan() <-chan wal.TransformLogEvent {
	return s.ch
}

func (s *scanner) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *scanner) Done() <-chan struct{} {
	return s.done
}

func (s *scanner) Close() error {
	s.closed.Do(func() {
		close(s.close)
	})
	<-s.done
	return s.Error()
}

func (s *scanner) run(ctx context.Context) {
	defer close(s.done)
	cursor := s.startAfter
	for {
		entry, ok, err := s.log.nextEntryAfter(ctx, cursor)
		if err != nil {
			s.setError(err)
			return
		}
		if ok {
			timeTick := entry.GetTimeTick()
			if s.exceedsEnd(timeTick) {
				return
			}
			if !s.sendEvent(ctx, wal.TransformLogEvent{Entry: entry}) {
				return
			}
			cursor = timeTick
			if !s.caughtUpEmitted && cursor >= s.caughtUpTarget {
				if !s.emitCaughtUp(ctx) {
					return
				}
				if s.end > 0 {
					return
				}
			}
			continue
		}
		if !s.caughtUpEmitted {
			if !s.emitCaughtUp(ctx) {
				return
			}
			if s.end > 0 {
				return
			}
		}
		if !s.waitAppend(ctx) {
			return
		}
	}
}

func (s *scanner) emitCaughtUp(ctx context.Context) bool {
	s.caughtUpEmitted = true
	return s.sendEvent(ctx, wal.TransformLogEvent{
		CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: s.startAfter},
	})
}

func (s *scanner) sendEvent(ctx context.Context, event wal.TransformLogEvent) bool {
	select {
	case s.ch <- event:
		return true
	case <-s.close:
		return false
	case <-ctx.Done():
		s.setError(ctx.Err())
		return false
	}
}

func (s *scanner) waitAppend(ctx context.Context) bool {
	notifyCh := s.log.notifyChannel()
	select {
	case <-notifyCh:
		return true
	case <-s.close:
		return false
	case <-ctx.Done():
		s.setError(ctx.Err())
		return false
	}
}

func (s *scanner) exceedsEnd(timeTick uint64) bool {
	return s.end > 0 && timeTick > s.end
}

func (s *scanner) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
