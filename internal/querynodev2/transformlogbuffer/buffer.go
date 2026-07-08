package transformlogbuffer

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type Buffer struct {
	streams wal.TransformLogStreamManager

	mu                sync.Mutex
	streamsByPChannel map[string]*streamState
	channels          map[string]*vchannelBuffer

	drainTasks chan *registration
}

func New(streams wal.TransformLogStreamManager) *Buffer {
	b := &Buffer{
		streams:           streams,
		streamsByPChannel: make(map[string]*streamState),
		channels:          make(map[string]*vchannelBuffer),
		drainTasks:        make(chan *registration, 1024),
	}
	for i := 0; i < 4; i++ {
		go b.drainWorker()
	}
	return b
}

func (b *Buffer) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	if view == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	meta := view.IntoProto().GetMeta()
	vchannel := meta.GetVchannel()
	startFrom := meta.GetTransformStartAfterTimetick()
	if vchannel == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	b.mu.Lock()
	defer b.mu.Unlock()
	buf := b.channels[vchannel]
	if buf == nil {
		stream, err := b.getOrCreateStreamLocked(ctx, pchannel)
		if err != nil {
			return nil, err
		}
		buf = newVChannelBuffer(b, pchannel, vchannel, startFrom, nil)
		sub, err := stream.stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
			VChannel:           vchannel,
			StartAfterTimeTick: startFrom,
			Handler:            bufEventHandler{buffer: buf},
		})
		if err != nil {
			return nil, err
		}
		buf.sub = sub
		b.channels[vchannel] = buf
		stream.refs[vchannel] = buf
	}
	if err := buf.acquire(ctx, startFrom); err != nil {
		return nil, err
	}
	return &guard{buffer: buf, startFrom: startFrom}, nil
}

func (b *Buffer) RegisterSegment(ctx context.Context, segment qnview.TransformSegment) (qnview.TransformRegistration, error) {
	if segment == nil || segment.VChannel() == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	b.mu.Lock()
	buf := b.channels[segment.VChannel()]
	b.mu.Unlock()
	if buf == nil {
		return nil, fmt.Errorf("transform log buffer for vchannel %q is not acquired", segment.VChannel())
	}
	return buf.registerSegment(ctx, segment)
}

func (b *Buffer) scheduleDrain(ctx context.Context, reg *registration) error {
	select {
	case b.drainTasks <- reg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Buffer) drainWorker() {
	for reg := range b.drainTasks {
		err := reg.buffer.drainRegistration(reg.ctx, reg)
		if err != nil {
			reg.buffer.removeRegistration(reg)
		}
		reg.finish(err)
	}
}

func (b *Buffer) getOrCreateStreamLocked(ctx context.Context, pchannel string) (*streamState, error) {
	state := b.streamsByPChannel[pchannel]
	if state != nil {
		return state, nil
	}
	if b.streams == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	stream, err := b.streams.AcquireStream(ctx, pchannel)
	if err != nil {
		return nil, err
	}
	state = &streamState{
		pchannel: pchannel,
		stream:   stream,
		refs:     make(map[string]*vchannelBuffer),
	}
	b.streamsByPChannel[pchannel] = state
	return state, nil
}

func (b *Buffer) remove(vchannel string, buf *vchannelBuffer) {
	b.mu.Lock()
	if b.channels[vchannel] == buf {
		delete(b.channels, vchannel)
	}
	if state := b.streamsByPChannel[buf.pchannel]; state != nil {
		delete(state.refs, vchannel)
		if len(state.refs) == 0 {
			delete(b.streamsByPChannel, buf.pchannel)
			stream := state.stream
			b.mu.Unlock()
			_ = stream.Close()
			return
		}
	}
	b.mu.Unlock()
}

type streamState struct {
	pchannel string
	stream   wal.TransformLogStream
	refs     map[string]*vchannelBuffer
}

type bufEventHandler struct {
	buffer *vchannelBuffer
}

func (h bufEventHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		h.buffer.fail(event.Err)
		return nil
	}
	if event.Entry != nil {
		h.buffer.onEntry(event.Entry)
	}
	if event.CaughtUp != nil {
		h.buffer.onCaughtUp()
	}
	return nil
}

func (h bufEventHandler) Close() {}

type guard struct {
	once      sync.Once
	buffer    *vchannelBuffer
	startFrom uint64
}

func (g *guard) Release() {
	g.once.Do(func() {
		g.buffer.releaseGuard(g.startFrom)
	})
}

func (g *guard) WaitTransformVisible(ctx context.Context, timetick uint64) error {
	return g.buffer.waitTransformVisible(ctx, timetick)
}

type vchannelBuffer struct {
	owner    *Buffer
	pchannel string
	vchannel string
	sub      wal.TransformLogSubscription

	mu               sync.Mutex
	retentionStart   uint64
	visibleTimeTick  uint64
	visibilityNotify chan struct{}
	guards           map[uint64]int
	entries          []*streamingpb.TransformLogEntry
	live             map[int64]*registration
	pending          map[int64]*registration
	caughtUp         bool
	err              error
}

func newVChannelBuffer(owner *Buffer, pchannel string, vchannel string, startFrom uint64, sub wal.TransformLogSubscription) *vchannelBuffer {
	return &vchannelBuffer{
		owner:            owner,
		pchannel:         pchannel,
		vchannel:         vchannel,
		sub:              sub,
		retentionStart:   startFrom,
		visibleTimeTick:  startFrom,
		visibilityNotify: make(chan struct{}),
		guards:           make(map[uint64]int),
		live:             make(map[int64]*registration),
		pending:          make(map[int64]*registration),
	}
}

func (b *vchannelBuffer) acquire(_ context.Context, startFrom uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	if startFrom < b.retentionStart {
		return fmt.Errorf("transform log buffer range starts from %d, cannot serve %d", b.retentionStart, startFrom)
	}
	b.guards[startFrom]++
	return nil
}

func (b *vchannelBuffer) registerSegment(ctx context.Context, segment qnview.TransformSegment) (qnview.TransformRegistration, error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return nil, b.err
	}
	startFrom := segment.TransformStartAfterTimeTick()
	if startFrom < b.retentionStart {
		b.mu.Unlock()
		return nil, fmt.Errorf("transform log buffer range starts from %d, cannot serve segment %d from %d", b.retentionStart, segment.ID(), startFrom)
	}
	reg := newRegistration(b, segment)
	b.pending[segment.ID()] = reg
	b.mu.Unlock()

	if err := b.owner.scheduleDrain(ctx, reg); err != nil {
		b.removeRegistration(reg)
		reg.finish(err)
		return nil, err
	}
	return reg, nil
}

func (b *vchannelBuffer) drainRegistration(ctx context.Context, reg *registration) error {
	for {
		batch, err := b.nextCatchupBatch(reg)
		if err != nil || len(batch) == 0 {
			return err
		}
		for _, entry := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := reg.segment.ApplyTransform(ctx, entry); err != nil {
				return err
			}
			if entry.GetTimeTick() > reg.drainedTo {
				reg.drainedTo = entry.GetTimeTick()
			}
		}
	}
}

func (b *vchannelBuffer) nextCatchupBatch(reg *registration) ([]*streamingpb.TransformLogEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return nil, b.err
	}
	if b.pending[reg.segment.ID()] != reg {
		return nil, nil
	}
	batch := make([]*streamingpb.TransformLogEntry, 0)
	for _, entry := range b.entries {
		if entry.GetTimeTick() > reg.drainedTo {
			batch = append(batch, entry)
		}
	}
	if len(batch) == 0 {
		delete(b.pending, reg.segment.ID())
		b.live[reg.segment.ID()] = reg
		return nil, nil
	}
	return batch, nil
}

func (b *vchannelBuffer) waitTransformVisible(ctx context.Context, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if timetick <= b.retentionStart || b.visibleTimeTick >= timetick {
			return nil
		}
		if b.err != nil {
			return b.err
		}
		notify := b.visibilityNotify
		b.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			b.mu.Lock()
			return ctx.Err()
		}
		b.mu.Lock()
	}
}

func (b *vchannelBuffer) unregister(reg *registration) {
	b.removeRegistration(reg)
}

func (b *vchannelBuffer) removeRegistration(reg *registration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pending, reg.segment.ID())
	if b.live[reg.segment.ID()] == reg {
		delete(b.live, reg.segment.ID())
	}
}

func (b *vchannelBuffer) releaseGuard(startFrom uint64) {
	b.mu.Lock()
	if count := b.guards[startFrom]; count > 1 {
		b.guards[startFrom] = count - 1
		b.trimLocked()
		b.mu.Unlock()
		return
	}
	delete(b.guards, startFrom)
	if len(b.guards) == 0 {
		b.mu.Unlock()
		if b.sub != nil {
			_ = b.sub.Close()
		}
		b.owner.remove(b.vchannel, b)
		return
	}
	b.trimLocked()
	b.mu.Unlock()
}

func (b *vchannelBuffer) trimLocked() {
	minStart := uint64(0)
	first := true
	for startFrom := range b.guards {
		if first || startFrom < minStart {
			minStart = startFrom
			first = false
		}
	}
	for _, reg := range b.pending {
		if first || reg.startFrom < minStart {
			minStart = reg.startFrom
			first = false
		}
	}
	if first || minStart <= b.retentionStart {
		return
	}
	kept := b.entries[:0]
	for _, entry := range b.entries {
		if entry.GetTimeTick() > minStart {
			kept = append(kept, entry)
		}
	}
	b.entries = kept
	b.retentionStart = minStart
}

func (b *vchannelBuffer) onEntry(entry *streamingpb.TransformLogEntry) {
	b.mu.Lock()
	if entry.GetTimeTick() > b.retentionStart {
		b.entries = append(b.entries, entry)
	}
	applies := make([]*registration, 0, len(b.live))
	for _, reg := range b.live {
		applies = append(applies, reg)
	}
	b.mu.Unlock()

	for _, reg := range applies {
		if err := reg.segment.ApplyTransform(context.Background(), entry); err != nil {
			b.fail(err)
			return
		}
	}

	b.mu.Lock()
	if entry.GetTimeTick() > b.visibleTimeTick {
		b.visibleTimeTick = entry.GetTimeTick()
		b.notifyVisibilityLocked()
	}
	b.mu.Unlock()
}

func (b *vchannelBuffer) onCaughtUp() {
	b.mu.Lock()
	if b.caughtUp {
		b.mu.Unlock()
		return
	}
	b.caughtUp = true
	b.notifyVisibilityLocked()
	b.mu.Unlock()
}

func (b *vchannelBuffer) fail(err error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return
	}
	b.err = err
	b.notifyVisibilityLocked()
	regs := make([]*registration, 0, len(b.live)+len(b.pending))
	for _, reg := range b.live {
		regs = append(regs, reg)
	}
	for _, reg := range b.pending {
		regs = append(regs, reg)
	}
	b.mu.Unlock()

	for _, reg := range regs {
		reg.finish(err)
	}
}

func (b *vchannelBuffer) notifyVisibilityLocked() {
	close(b.visibilityNotify)
	b.visibilityNotify = make(chan struct{})
}

type registration struct {
	buffer     *vchannelBuffer
	segment    qnview.TransformSegment
	startFrom  uint64
	drainedTo  uint64
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	err        error
	errMu      sync.Mutex
	once       sync.Once
	finishOnce sync.Once
}

func newRegistration(buffer *vchannelBuffer, segment qnview.TransformSegment) *registration {
	ctx, cancel := context.WithCancel(context.Background())
	return &registration{
		buffer:    buffer,
		segment:   segment,
		startFrom: segment.TransformStartAfterTimeTick(),
		drainedTo: segment.TransformStartAfterTimeTick(),
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
}

func (r *registration) WaitCatchup(ctx context.Context) error {
	select {
	case <-r.done:
		r.errMu.Lock()
		defer r.errMu.Unlock()
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *registration) Unregister() {
	r.once.Do(func() {
		r.cancel()
		r.buffer.unregister(r)
	})
}

func (r *registration) finish(err error) {
	r.finishOnce.Do(func() {
		r.errMu.Lock()
		r.err = err
		r.errMu.Unlock()
		close(r.done)
	})
}
