package transformlog

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type TransformLog interface {
	Append(message.ImmutableMessage, AppendOption) AppendResult
	AppendBarrier(uint64) AppendResult
	Flush(context.Context, FlushOption) (FlushResult, error)
	Materialize(context.Context, MaterializeOption) (MaterializeResult, error)
	Truncate(TruncateOption) TruncateResult

	Recover(context.Context, *streamingpb.VChannelTransformLogMeta) (RecoverResult, error)
	SnapshotMeta() *streamingpb.VChannelTransformLogMeta
	LatestTimeTick() uint64
	DataCheckpointTimeTick() uint64
	DataBarrierTimeTick() uint64
	MaterializedTimeTick() uint64
	MaterializedBarrierTimeTick() uint64
	HasDirty() bool
	ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelTransformLogMeta
	MarkSnapshotPersisted(*streamingpb.VChannelTransformLogMeta)
	HasPendingWork() bool
	ShouldMaterialize() bool
}

type Config struct {
	VChannel            string
	MaxRows             uint64
	MaterializeMaxRows  uint64
	MaterializeMaxBytes uint64
	Meta                *streamingpb.VChannelTransformLogMeta
	Store               Store
	Materializer        Materializer
}

type AppendResult struct {
	Appended     bool
	ShouldFlush  bool
	DataTimeTick uint64
}

type AppendOption struct {
	DeleteFilter func(partitionID int64, timeTick uint64) bool
}

func (o AppendOption) acceptDelete(partitionID int64, timeTick uint64) bool {
	if o.DeleteFilter == nil {
		return true
	}
	return o.DeleteFilter(partitionID, timeTick)
}

type FlushOption struct {
	TargetTimeTick uint64
}

type FlushResult struct {
	Started            bool
	DurableTimeTick    uint64
	NextTargetTimeTick uint64
}

type MaterializeOption struct {
	TargetTimeTick uint64
}

type MaterializeResult struct {
	Started                 bool
	MaterializedTimeTick    uint64
	MaterializedRows        uint64
	MaterializedBytes       uint64
	HasMaterializedSegments bool
}

type TruncateOption struct {
	TimeTick uint64
}

type TruncateResult struct {
	Changed bool
}

type RecoverResult struct {
	Recovered          bool
	CheckpointTimeTick uint64
}

type transformLog struct {
	flushMu               sync.Mutex
	materializeMu         sync.Mutex
	mu                    sync.Mutex
	notifyCh              chan struct{}
	vchannel              string
	meta                  *streamingpb.VChannelTransformLogMeta
	persistedDataTimeTick uint64
	persistedMaterialized uint64
	dirty                 bool
	pendingDirtySnapshot  *streamingpb.VChannelTransformLogMeta
	buffer                buffer
	store                 Store
	materializer          Materializer
	materializeMaxRows    uint64
	materializeMaxBytes   uint64
	streamNotifier        func()

	chunks []*chunkDescriptor
}

func New(config Config) TransformLog {
	meta := cloneMetaOrNew(config.Meta)
	return &transformLog{
		vchannel:              config.VChannel,
		meta:                  meta,
		persistedDataTimeTick: meta.GetCheckpointTimeTick(),
		persistedMaterialized: meta.GetMaterializedTimeTick(),
		notifyCh:              make(chan struct{}),
		buffer:                newBuffer(config.MaxRows),
		store:                 config.Store,
		materializer:          config.Materializer,
		materializeMaxRows:    config.MaterializeMaxRows,
		materializeMaxBytes:   config.MaterializeMaxBytes,
	}
}

func (t *transformLog) Append(msg message.ImmutableMessage, opt AppendOption) AppendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if msg.TimeTick() <= t.meta.GetCheckpointTimeTick() || msg.TimeTick() <= t.buffer.DataTimeTick() {
		return AppendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	if !t.buffer.Append(msg, opt) {
		return AppendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	t.notifyScannersLocked()
	t.notifyStreamLocked()
	return AppendResult{
		Appended:     true,
		ShouldFlush:  t.buffer.ShouldFlush(),
		DataTimeTick: t.buffer.DataTimeTick(),
	}
}

func (t *transformLog) AppendBarrier(timeTick uint64) AppendResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if timeTick <= t.meta.GetCheckpointTimeTick() || timeTick <= t.buffer.DataTimeTick() {
		return AppendResult{DataTimeTick: t.buffer.DataTimeTick()}
	}
	t.buffer.AppendEntry(transformBarrierEntry(timeTick))
	t.notifyScannersLocked()
	t.notifyStreamLocked()
	return AppendResult{
		Appended:     true,
		ShouldFlush:  t.buffer.ShouldFlush(),
		DataTimeTick: t.buffer.DataTimeTick(),
	}
}

func (t *transformLog) Flush(ctx context.Context, opt FlushOption) (FlushResult, error) {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	var work flushWork
	t.mu.Lock()
	if !t.buffer.StartFlush(opt.TargetTimeTick) && (!t.buffer.IsFlushing() || t.buffer.FlushTargetTimeTick() == 0) {
		t.mu.Unlock()
		return FlushResult{}, nil
	}
	targetTimeTick := t.buffer.FlushTargetTimeTick()
	if targetTimeTick > t.meta.GetCheckpointTimeTick() {
		work = t.prepareFlushLocked(targetTimeTick)
	} else {
		work = flushWork{TargetTimeTick: targetTimeTick}
	}
	t.mu.Unlock()

	if work.Chunk != nil {
		if t.store == nil {
			return FlushResult{}, errors.New("transform log store is nil")
		}
		if err := t.store.WriteTransformLogChunk(ctx, t.vchannel, work.Chunk); err != nil {
			return FlushResult{}, err
		}
	}

	t.mu.Lock()
	result := t.commitFlushLocked(work)
	result.Started = true
	t.mu.Unlock()
	return result, nil
}

func (t *transformLog) Materialize(ctx context.Context, opt MaterializeOption) (MaterializeResult, error) {
	t.materializeMu.Lock()
	defer t.materializeMu.Unlock()
	t.mu.Lock()
	targetTimeTick := opt.TargetTimeTick
	if targetTimeTick == 0 {
		targetTimeTick = t.meta.GetCheckpointTimeTick()
	}
	if targetTimeTick <= t.meta.GetMaterializedTimeTick() {
		t.mu.Unlock()
		return MaterializeResult{}, nil
	}
	if targetTimeTick > t.meta.GetCheckpointTimeTick() {
		targetTimeTick = t.meta.GetCheckpointTimeTick()
	}
	if targetTimeTick <= t.meta.GetMaterializedTimeTick() {
		t.mu.Unlock()
		return MaterializeResult{}, nil
	}
	t.mu.Unlock()

	work, err := t.prepareMaterialize(ctx, targetTimeTick)
	if err != nil {
		return MaterializeResult{}, err
	}

	if len(work.Entries) > 0 {
		if t.materializer == nil {
			return MaterializeResult{}, errors.New("transform log materializer is nil")
		}
		if err := t.materializer.Materialize(ctx, MaterializeRequest{
			VChannel:       t.vchannel,
			TargetTimeTick: work.TargetTimeTick,
			Entries:        work.Entries,
			MaxRows:        t.materializeMaxRows,
			MaxBytes:       t.materializeMaxBytes,
		}); err != nil {
			return MaterializeResult{}, err
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitMaterializeLocked(work), nil
}

func (t *transformLog) Truncate(opt TruncateOption) TruncateResult {
	var changed bool
	t.mu.Lock()
	if opt.TimeTick <= t.meta.GetTruncateTimeTick() {
		t.mu.Unlock()
		return TruncateResult{}
	}
	t.meta.TruncateTimeTick = opt.TimeTick
	t.dirty = true
	changed = true
	for len(t.chunks) > 0 {
		chunk := t.chunks[0]
		if chunk.toTimeTick == 0 {
			t.mu.Unlock()
			if err := t.loadChunk(context.TODO(), chunk); err != nil {
				return TruncateResult{Changed: changed}
			}
			t.mu.Lock()
			continue
		}
		if chunk.toTimeTick > opt.TimeTick {
			break
		}
		t.chunks = t.chunks[1:]
		if chunk.id >= t.meta.GetFirstChunkId() {
			t.meta.FirstChunkId = chunk.id + 1
		}
	}
	t.mu.Unlock()
	return TruncateResult{Changed: changed}
}

func (t *transformLog) Recover(ctx context.Context, meta *streamingpb.VChannelTransformLogMeta) (RecoverResult, error) {
	_ = ctx
	t.mu.Lock()
	if meta != nil {
		t.meta = cloneMetaOrNew(meta)
		t.persistedDataTimeTick = t.meta.GetCheckpointTimeTick()
		t.persistedMaterialized = t.meta.GetMaterializedTimeTick()
	}
	recoverMeta := cloneMeta(t.meta)
	t.chunks = nil
	t.buffer = newBuffer(t.buffer.maxRows)
	t.mu.Unlock()
	if recoverMeta == nil || recoverMeta.GetFirstChunkId() == recoverMeta.GetNextChunkId() {
		return RecoverResult{}, nil
	}
	chunks := make([]*chunkDescriptor, 0, recoverMeta.GetNextChunkId()-recoverMeta.GetFirstChunkId())
	for chunkID := recoverMeta.GetFirstChunkId(); chunkID < recoverMeta.GetNextChunkId(); chunkID++ {
		chunks = append(chunks, newColdChunkDescriptor(chunkID))
	}
	t.mu.Lock()
	t.chunks = chunks
	t.mu.Unlock()
	return RecoverResult{Recovered: true, CheckpointTimeTick: recoverMeta.GetCheckpointTimeTick()}, nil
}

func (t *transformLog) SnapshotMeta() *streamingpb.VChannelTransformLogMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneMeta(t.meta)
}

func (t *transformLog) LatestTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return max(t.meta.GetCheckpointTimeTick(), t.buffer.DataTimeTick())
}

func (t *transformLog) DataCheckpointTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.meta.GetCheckpointTimeTick()
}

func (t *transformLog) DataBarrierTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.persistedDataTimeTick
}

func (t *transformLog) MaterializedTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.meta.GetMaterializedTimeTick()
}

func (t *transformLog) MaterializedBarrierTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.persistedMaterialized
}

func (t *transformLog) HasDirty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dirty
}

func (t *transformLog) ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelTransformLogMeta {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pendingDirtySnapshot != nil {
		return cloneMeta(t.pendingDirtySnapshot)
	}
	if !t.dirty {
		return nil
	}
	t.pendingDirtySnapshot = cloneMeta(t.meta)
	return cloneMeta(t.pendingDirtySnapshot)
}

func (t *transformLog) MarkSnapshotPersisted(snapshot *streamingpb.VChannelTransformLogMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if snapshot.GetCheckpointTimeTick() > t.persistedDataTimeTick {
		t.persistedDataTimeTick = snapshot.GetCheckpointTimeTick()
	}
	if snapshot.GetMaterializedTimeTick() > t.persistedMaterialized {
		t.persistedMaterialized = snapshot.GetMaterializedTimeTick()
	}
	if t.pendingDirtySnapshot != nil && proto.Equal(t.pendingDirtySnapshot, snapshot) {
		t.pendingDirtySnapshot = nil
	}
	t.dirty = !proto.Equal(t.meta, snapshot)
}

func (t *transformLog) HasPendingWork() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.buffer.IsEmpty() ||
		t.buffer.IsFlushing() ||
		t.buffer.FlushTargetTimeTick() > t.persistedDataTimeTick
}

func (t *transformLog) ShouldMaterialize() bool {
	t.mu.Lock()
	targetTimeTick := t.meta.GetCheckpointTimeTick()
	t.mu.Unlock()
	rows, bytes, err := t.pendingMaterializeStats(context.TODO(), targetTimeTick)
	if err != nil {
		return false
	}
	maxRows := t.materializeMaxRows
	if maxRows == 0 {
		maxRows = defaultMaterializeMaxRows
	}
	maxBytes := t.materializeMaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaterializeMaxBytes
	}
	return rows >= maxRows || bytes >= maxBytes
}

type flushWork struct {
	TargetTimeTick uint64
	Chunk          *streamingpb.TransformLogChunk
}

func (t *transformLog) prepareFlushLocked(targetTimeTick uint64) flushWork {
	return flushWork{
		TargetTimeTick: targetTimeTick,
		Chunk:          t.buffer.FlushChunk(t.meta.GetNextChunkId(), targetTimeTick),
	}
}

func (t *transformLog) commitFlushLocked(work flushWork) FlushResult {
	var result FlushResult
	if work.Chunk != nil {
		toTimeTick := work.Chunk.GetEntries()[len(work.Chunk.GetEntries())-1].GetTimeTick()
		t.buffer.DiscardThrough(toTimeTick)
		t.chunks = append(t.chunks, newLoadedChunkDescriptor(work.Chunk))
		if work.Chunk.GetChunkId() >= t.meta.GetNextChunkId() {
			t.meta.NextChunkId = work.Chunk.GetChunkId() + 1
		}
		t.dirty = true
		result.DurableTimeTick = toTimeTick
		if !t.buffer.HasFlushWorkThrough(work.TargetTimeTick) {
			result.DurableTimeTick = work.TargetTimeTick
		}
		if result.DurableTimeTick > t.meta.GetCheckpointTimeTick() {
			t.meta.CheckpointTimeTick = result.DurableTimeTick
		}
	} else if work.TargetTimeTick > t.meta.GetCheckpointTimeTick() {
		t.meta.CheckpointTimeTick = work.TargetTimeTick
		t.dirty = true
		result.DurableTimeTick = work.TargetTimeTick
	}

	nextDurableTimeTick := maxTimeTick(t.meta.GetCheckpointTimeTick(), result.DurableTimeTick)
	currentFlushTarget := t.buffer.FlushTargetTimeTick()
	t.buffer.FinishFlush()
	switch {
	case currentFlushTarget > nextDurableTimeTick:
		result.NextTargetTimeTick = currentFlushTarget
	case t.buffer.HasFlushWorkThrough(currentFlushTarget):
		result.NextTargetTimeTick = currentFlushTarget
	case t.buffer.ShouldFlush():
		result.NextTargetTimeTick = t.buffer.DataTimeTick()
	}
	return result
}

type materializeWork struct {
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	Rows           uint64
	Bytes          uint64
}

func (t *transformLog) prepareMaterialize(ctx context.Context, targetTimeTick uint64) (materializeWork, error) {
	work := materializeWork{TargetTimeTick: targetTimeTick}
	t.mu.Lock()
	cursor := t.meta.GetMaterializedTimeTick()
	t.mu.Unlock()
	for {
		entry, ok, err := t.nextEntryAfter(ctx, cursor)
		if err != nil {
			return materializeWork{}, err
		}
		if !ok || entry.GetTimeTick() > targetTimeTick {
			return work, nil
		}
		cursor = entry.GetTimeTick()
		if !isTransformDeleteEntry(entry) {
			continue
		}
		work.Entries = append(work.Entries, entry)
		work.Rows += transformLogEntryRows(entry)
		work.Bytes += uint64(proto.Size(entry))
	}
}

func (t *transformLog) pendingMaterializeStats(ctx context.Context, targetTimeTick uint64) (uint64, uint64, error) {
	t.mu.Lock()
	cursor := t.meta.GetMaterializedTimeTick()
	t.mu.Unlock()
	var rows uint64
	var bytes uint64
	for {
		entry, ok, err := t.nextEntryAfter(ctx, cursor)
		if err != nil {
			return 0, 0, err
		}
		if !ok || entry.GetTimeTick() > targetTimeTick {
			return rows, bytes, nil
		}
		cursor = entry.GetTimeTick()
		if !isTransformDeleteEntry(entry) {
			continue
		}
		rows += transformLogEntryRows(entry)
		bytes += uint64(proto.Size(entry))
	}
}

func (t *transformLog) commitMaterializeLocked(work materializeWork) MaterializeResult {
	if work.TargetTimeTick <= t.meta.GetMaterializedTimeTick() {
		return MaterializeResult{}
	}
	t.meta.MaterializedTimeTick = work.TargetTimeTick
	t.dirty = true
	return MaterializeResult{
		Started:                 true,
		MaterializedTimeTick:    work.TargetTimeTick,
		MaterializedRows:        work.Rows,
		MaterializedBytes:       work.Bytes,
		HasMaterializedSegments: len(work.Entries) > 0,
	}
}

func (t *transformLog) nextEntryAfter(ctx context.Context, after uint64) (*streamingpb.TransformLogEntry, bool, error) {
	for {
		t.mu.Lock()
		entry, chunkToLoad, err := t.nextEntryAfterLocked(after)
		t.mu.Unlock()
		if err != nil {
			return nil, false, err
		}
		if entry != nil {
			return entry, true, nil
		}
		if chunkToLoad == nil {
			return nil, false, nil
		}
		if err := t.loadChunk(ctx, chunkToLoad); err != nil {
			return nil, false, err
		}
	}
}

func (t *transformLog) nextEntryAfterLocked(after uint64) (*streamingpb.TransformLogEntry, *chunkDescriptor, error) {
	if after < t.meta.GetTruncateTimeTick() {
		return nil, nil, errors.Wrap(wal.ErrTransformLogStartPointTruncated, "start point is truncated")
	}
	for _, chunk := range t.chunks {
		if chunk.toTimeTick > 0 && chunk.toTimeTick <= after {
			continue
		}
		if !chunk.loaded() {
			return nil, chunk, nil
		}
		for _, entry := range chunk.entries {
			if entry.GetTimeTick() > after {
				return cloneTransformLogEntry(entry), nil, nil
			}
		}
	}
	for _, entry := range t.buffer.entries {
		if entry.timeTick > after {
			return cloneTransformLogEntry(entry.entry), nil, nil
		}
	}
	return nil, nil, nil
}

func (t *transformLog) loadChunk(ctx context.Context, chunk *chunkDescriptor) error {
	for {
		t.mu.Lock()
		switch chunk.state {
		case chunkStateLoaded:
			t.mu.Unlock()
			return nil
		case chunkStateLoading:
			done := chunk.loadDone
			t.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		case chunkStateCold:
			if t.store == nil {
				t.mu.Unlock()
				return errors.New("transform log store is nil")
			}
			done := make(chan struct{})
			chunk.state = chunkStateLoading
			chunk.loadDone = done
			t.mu.Unlock()

			loaded, err := t.store.ReadTransformLogChunk(ctx, t.vchannel, chunk.id)
			var entries []*streamingpb.TransformLogEntry
			if err == nil {
				err = validateChunk(loaded, chunk.id, 0)
			}
			if err == nil {
				entries = cloneTransformLogEntries(loaded.GetEntries())
			}

			t.mu.Lock()
			if err == nil {
				chunk.setEntries(entries)
				err = t.validateLoadedChunkOrderLocked(chunk)
			}
			if err != nil {
				chunk.clearEntries()
				chunk.state = chunkStateCold
			}
			close(done)
			chunk.loadDone = nil
			t.mu.Unlock()
			return err
		}
	}
}

func (t *transformLog) validateLoadedChunkOrderLocked(chunk *chunkDescriptor) error {
	if !chunk.loaded() {
		return nil
	}
	for idx, candidate := range t.chunks {
		if candidate != chunk {
			continue
		}
		if idx > 0 {
			previous := t.chunks[idx-1]
			if previous.toTimeTick > 0 && chunk.fromTimeTick <= previous.toTimeTick {
				return errors.Errorf("transform log chunk %d entries are not ordered", chunk.id)
			}
		}
		if idx+1 < len(t.chunks) {
			next := t.chunks[idx+1]
			if next.fromTimeTick > 0 && chunk.toTimeTick >= next.fromTimeTick {
				return errors.Errorf("transform log chunk %d entries are not ordered", chunk.id)
			}
		}
		return nil
	}
	return nil
}

func (t *transformLog) notifyChannel() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notifyCh
}

func (t *transformLog) notifyScannersLocked() {
	close(t.notifyCh)
	t.notifyCh = make(chan struct{})
}

func (t *transformLog) setStreamNotifier(notifier func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streamNotifier = notifier
}

func (t *transformLog) notifyStreamLocked() {
	if t.streamNotifier != nil {
		t.streamNotifier()
	}
}

func (t *transformLog) latestTimeTickLocked() uint64 {
	return maxTimeTick(t.meta.GetCheckpointTimeTick(), t.buffer.DataTimeTick())
}

func validateChunk(chunk *streamingpb.TransformLogChunk, expectedChunkID uint64, previousTimeTick uint64) error {
	if chunk == nil {
		return errors.Errorf("transform log chunk %d is nil", expectedChunkID)
	}
	if chunk.GetChunkId() != expectedChunkID {
		return errors.Errorf("transform log chunk id mismatch, expected %d, got %d", expectedChunkID, chunk.GetChunkId())
	}
	if len(chunk.GetEntries()) == 0 {
		return errors.Errorf("transform log chunk %d is empty", expectedChunkID)
	}
	for _, entry := range chunk.GetEntries() {
		if entry.GetTimeTick() <= previousTimeTick {
			return errors.Errorf("transform log chunk %d entries are not ordered", expectedChunkID)
		}
		previousTimeTick = entry.GetTimeTick()
	}
	return nil
}

type chunkState int

const (
	chunkStateCold chunkState = iota
	chunkStateLoading
	chunkStateLoaded
)

type chunkDescriptor struct {
	id           uint64
	state        chunkState
	loadDone     chan struct{}
	fromTimeTick uint64
	toTimeTick   uint64
	entries      []*streamingpb.TransformLogEntry
}

func newColdChunkDescriptor(chunkID uint64) *chunkDescriptor {
	return &chunkDescriptor{
		id:    chunkID,
		state: chunkStateCold,
	}
}

func newLoadedChunkDescriptor(chunk *streamingpb.TransformLogChunk) *chunkDescriptor {
	descriptor := &chunkDescriptor{
		id:    chunk.GetChunkId(),
		state: chunkStateLoaded,
	}
	descriptor.setEntries(cloneTransformLogEntries(chunk.GetEntries()))
	return descriptor
}

func (c *chunkDescriptor) loaded() bool {
	return c.state == chunkStateLoaded
}

func (c *chunkDescriptor) setEntries(entries []*streamingpb.TransformLogEntry) {
	c.entries = entries
	if len(entries) == 0 {
		c.fromTimeTick = 0
		c.toTimeTick = 0
		return
	}
	c.fromTimeTick = entries[0].GetTimeTick()
	c.toTimeTick = entries[len(entries)-1].GetTimeTick()
	c.state = chunkStateLoaded
}

func (c *chunkDescriptor) clearEntries() {
	c.entries = nil
	c.fromTimeTick = 0
	c.toTimeTick = 0
}

func cloneMetaOrNew(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return &streamingpb.VChannelTransformLogMeta{}
	}
	return cloneMeta(meta)
}

func cloneMeta(meta *streamingpb.VChannelTransformLogMeta) *streamingpb.VChannelTransformLogMeta {
	if meta == nil {
		return nil
	}
	return proto.Clone(meta).(*streamingpb.VChannelTransformLogMeta)
}

func cloneTransformLogEntries(entries []*streamingpb.TransformLogEntry) []*streamingpb.TransformLogEntry {
	cloned := make([]*streamingpb.TransformLogEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, cloneTransformLogEntry(entry))
	}
	return cloned
}

func maxTimeTick(left uint64, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func isTransformDeleteEntry(entry *streamingpb.TransformLogEntry) bool {
	if entry == nil {
		return false
	}
	return entry.GetDelete() != nil
}
