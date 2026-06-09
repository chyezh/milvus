package growing

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func (m *Manager) Read(ctx context.Context, opt transformlog.ReadOption) transformlog.Scanner {
	if opt.VChannel == "" {
		return newTransformLogErrorScanner(opt.Name, errors.Wrap(transformlog.ErrInvalidReadOption, "vchannel is required"))
	}
	if funcutil.ToPhysicalChannel(opt.VChannel) != m.channelName && m.channelName != "" {
		return newTransformLogErrorScanner(opt.Name, errors.Wrap(transformlog.ErrInvalidReadOption, "vchannel does not belong to manager pchannel"))
	}
	vchannel := m.vChannel(opt.VChannel)
	if vchannel == nil {
		return newTransformLogErrorScanner(opt.Name, errors.Wrap(transformlog.ErrVChannelUnavailable, "vchannel is not available"))
	}
	return vchannel.ReadTransformLog(ctx, opt)
}

func (info *vChannelView) ReadTransformLog(ctx context.Context, opt transformlog.ReadOption) transformlog.Scanner {
	info.mu.Lock()
	meta := proto.Clone(info.meta).(*streamingpb.VChannelMeta)
	chunks := cloneTransformLogChunks(info.retainedTransformLogChunks)
	info.mu.Unlock()

	transformMeta := meta.GetTransformLogMeta()
	if transformMeta != nil && opt.StartAfterTimeTick < transformMeta.GetTruncateTimeTick() {
		return newTransformLogErrorScanner(opt.Name, errors.Wrap(transformlog.ErrStartPointTruncated, "start point is truncated"))
	}
	scanner := newTransformLogScanner(opt.Name, opt.StartAfterTimeTick)
	go scanner.send(ctx, info, chunks)
	return scanner
}

type transformLogScanner struct {
	name       string
	startAfter uint64
	ch         chan transformlog.Event
	done       chan struct{}
	close      chan struct{}
	errMu      sync.Mutex
	err        error
	closed     sync.Once
}

func newTransformLogScanner(name string, startAfter uint64) *transformLogScanner {
	return &transformLogScanner{
		name:       name,
		startAfter: startAfter,
		ch:         make(chan transformlog.Event, 16),
		done:       make(chan struct{}),
		close:      make(chan struct{}),
	}
}

func (s *transformLogScanner) Name() string {
	return s.name
}

func (s *transformLogScanner) Chan() <-chan transformlog.Event {
	return s.ch
}

func (s *transformLogScanner) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *transformLogScanner) Done() <-chan struct{} {
	return s.done
}

func (s *transformLogScanner) Close() error {
	s.closed.Do(func() {
		close(s.close)
	})
	<-s.done
	return s.Error()
}

func (s *transformLogScanner) send(ctx context.Context, vchannel *vChannelView, chunks []*streamingpb.TransformLogChunk) {
	defer close(s.done)
	for _, chunk := range chunks {
		for _, entry := range chunk.GetEntries() {
			if entry.GetTimeTick() <= s.startAfter {
				continue
			}
			if !s.sendEvent(ctx, transformlog.Event{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)}) {
				return
			}
		}
	}
	vchannel.registerTransformLogScanner(s)
	defer vchannel.unregisterTransformLogScanner(s)
	if !s.sendEvent(ctx, transformlog.Event{CaughtUp: &transformlog.CaughtUp{StartAfterTimeTick: s.startAfter}}) {
		return
	}
	select {
	case <-s.close:
	case <-ctx.Done():
		s.setError(ctx.Err())
	}
}

func (s *transformLogScanner) sendEvent(ctx context.Context, event transformlog.Event) bool {
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

func (s *transformLogScanner) publishEntry(entry *streamingpb.TransformLogEntry) {
	if entry.GetTimeTick() <= s.startAfter {
		return
	}
	_ = s.sendEvent(context.Background(), transformlog.Event{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)})
}

func (s *transformLogScanner) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

type transformLogErrorScanner struct {
	name string
	done chan struct{}
	err  error
}

func newTransformLogErrorScanner(name string, err error) transformlog.Scanner {
	done := make(chan struct{})
	close(done)
	return &transformLogErrorScanner{name: name, done: done, err: err}
}

func (s *transformLogErrorScanner) Name() string {
	return s.name
}

func (s *transformLogErrorScanner) Chan() <-chan transformlog.Event {
	ch := make(chan transformlog.Event)
	close(ch)
	return ch
}

func (s *transformLogErrorScanner) Error() error {
	return s.err
}

func (s *transformLogErrorScanner) Done() <-chan struct{} {
	return s.done
}

func (s *transformLogErrorScanner) Close() error {
	return s.err
}

func cloneTransformLogChunks(chunks []*streamingpb.TransformLogChunk) []*streamingpb.TransformLogChunk {
	if len(chunks) == 0 {
		return nil
	}
	cloned := make([]*streamingpb.TransformLogChunk, 0, len(chunks))
	for _, chunk := range chunks {
		cloned = append(cloned, proto.Clone(chunk).(*streamingpb.TransformLogChunk))
	}
	return cloned
}

func (info *vChannelView) registerTransformLogScanner(scanner *transformLogScanner) {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.transformLogSubscribers == nil {
		info.transformLogSubscribers = make(map[*transformLogScanner]struct{})
	}
	info.transformLogSubscribers[scanner] = struct{}{}
}

func (info *vChannelView) unregisterTransformLogScanner(scanner *transformLogScanner) {
	info.mu.Lock()
	defer info.mu.Unlock()
	delete(info.transformLogSubscribers, scanner)
}

func (info *vChannelView) publishTransformLogEntries(entries []*streamingpb.TransformLogEntry) {
	info.mu.Lock()
	subscribers := make([]*transformLogScanner, 0, len(info.transformLogSubscribers))
	for subscriber := range info.transformLogSubscribers {
		subscribers = append(subscribers, subscriber)
	}
	info.mu.Unlock()
	for _, entry := range entries {
		for _, subscriber := range subscribers {
			subscriber.publishEntry(entry)
		}
	}
}
