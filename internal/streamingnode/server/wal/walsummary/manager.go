// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package walsummary

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Manager is the pchannel-scoped WALSummary runtime. It owns the chunk
// generations and the manifest, collects the staging of every vchannel view,
// persists chunks asynchronously, and runs the retention GC.
//
// Persistence model: the summary is persisted to object storage independently
// of the catalog checkpoint. A chunk is only ever released from memory (the
// message handles) after the chunk object AND the manifest record of it are
// durable, so the global WAL checkpoint — which advances past a message only
// when every retained handle is released — can never outrun the summary.
type Manager struct {
	mu  sync.Mutex
	cfg ManagerConfig
	// manifestMu serializes manifest edits and publishes. Every path that
	// amends the in-memory manifest and writes it out (flush publish, GC
	// release, pending-GC removal) must take manifestMu first: the publish
	// marshals a clone taken under mu, and a concurrent edit of the same
	// backing arrays (recordChunk's sort, removeChunkEntry, removePendingGC)
	// would otherwise publish a torn view.
	manifestMu sync.Mutex
	// views holds one summary view per vchannel. A vchannel's view lives for
	// the whole lifetime of the manager; the vchannel module drops it on
	// cleanup via RemoveView.
	views map[string]*SummaryView

	// nextGeneration is the generation the next flush writes.
	nextGeneration uint64
	// manifest is the in-memory manifest: the chunk index recovery may read,
	// plus the pending GC queue. It is published by every flush.
	manifest *streamingpb.PChannelSummaryManifest
	// latestCoveredTimeTick is the newest timetick covered by a durable chunk.
	latestCoveredTimeTick uint64
	// materializedFrontiers holds the transform materialization frontier per
	// vchannel, mirrored from VChannelMeta. It is the hard lower bound of the
	// retention: records not yet materialized must not be released.
	materializedFrontiers map[string]uint64
	// pendingDurableFrontiers records the restored durable frontier of a
	// vchannel whose view does not exist yet. Recovery sets it before the
	// vchannel modules are built; View applies it when the view is created,
	// so replay does not re-stage records the manifest already covers.
	pendingDurableFrontiers map[string]uint64

	// flushTasks are the in-flight / queued flush tasks. At most one task is
	// scheduled at a time: a request while one is pending is a no-op, because
	// the executing task collects every view's staging. This keeps the
	// predecessor graph a single chain instead of growing one task per
	// observation.
	flushTasks []*summaryFlushTask
	// stagingBytes is the pchannel-wide estimate of the staging records not
	// yet written to a chunk, summed across every vchannel view. It is the
	// input to the autonomous flush decision: a chunk is written when the
	// total reaches FlushMaxBytes, so chunk size tracks the configured
	// threshold instead of per-vchannel bursts. It is maintained by
	// onStagingGrown (observation path) and recomputed from the views after
	// every collect.
	stagingBytes uint64
}

// ManagerConfig carries the wiring of one pchannel's summary manager.
type ManagerConfig struct {
	PChannel string
	Term     int64
	// Store is the object storage layer of the summary store.
	Store *Store
	// Runtime provides the scheduler and the module notifier.
	Runtime moduleapi.Runtime
	// FlushMaxBytes is the pchannel-wide staging size that triggers an
	// autonomous flush: the manager accumulates every vchannel view's staging
	// bytes and writes a chunk once the total reaches the threshold, so the
	// chunk size tracks the configured value instead of per-vchannel bursts.
	FlushMaxBytes uint64
	// RetentionMaxBytes is the soft budget of the retained chunk objects. GC
	// releases chunks above the budget, bounded below by the per-vchannel
	// materialization frontiers.
	RetentionMaxBytes uint64
	Logger            *mlog.Logger
}

// NewManager creates the summary manager of one pchannel.
func NewManager(config ManagerConfig) *Manager {
	return &Manager{
		cfg:                   config,
		views:                 make(map[string]*SummaryView),
		manifest:              &streamingpb.PChannelSummaryManifest{},
		materializedFrontiers: make(map[string]uint64),
	}
}

// View returns the summary view of a vchannel, creating it on first use. A
// pchannel has one view per vchannel for the whole lifetime of the manager.
// A view created after recovery inherits the durable frontier the recovery
// path recorded via SetDurableTimeTick, so replay skips already-durable
// records.
func (m *Manager) View(vchannel string) *SummaryView {
	m.mu.Lock()
	defer m.mu.Unlock()
	view, ok := m.views[vchannel]
	if !ok {
		view = NewSummaryView(m, vchannel)
		if frontier := m.pendingDurableFrontiers[vchannel]; frontier > 0 {
			view.mu.Lock()
			view.durableTimeTick = frontier
			view.mu.Unlock()
		}
		m.views[vchannel] = view
	}
	return view
}

// SetDurableTimeTick restores the durable frontier of one vchannel into its
// view. It is called by the recovery path before the vchannel modules are
// built; the view may not exist yet, in which case the frontier is recorded
// and applied by View. Existing staging with timetick at or below the frontier
// is discarded and its handles released — recovery runs before any
// observation, so there is none, but a repeated call stays correct.
func (m *Manager) SetDurableTimeTick(vchannel string, timetick uint64) {
	m.mu.Lock()
	view, ok := m.views[vchannel]
	if !ok {
		if m.pendingDurableFrontiers == nil {
			m.pendingDurableFrontiers = make(map[string]uint64)
		}
		if timetick > m.pendingDurableFrontiers[vchannel] {
			m.pendingDurableFrontiers[vchannel] = timetick
		}
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	view.mu.Lock()
	if timetick > view.durableTimeTick {
		view.durableTimeTick = timetick
	}
	view.mu.Unlock()
}

// DurableTimeTick returns the newest durable record timetick of one vchannel,
// derived from the recovered manifest: the largest per-vchannel chunk index
// end across all recorded chunks.
func (m *Manager) DurableTimeTick(vchannel string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var frontier uint64
	for _, chunk := range m.manifest.GetChunks() {
		if index := vchannelChunkIndex(chunk, vchannel); index != nil && index.GetEndTimetick() > frontier {
			frontier = index.GetEndTimetick()
		}
	}
	return frontier
}

// RemoveView drops the summary view of a vchannel, discarding its staging.
// It is called by the vchannel module after the vchannel cleanup snapshot is
// durable: no record of a dropped vchannel may survive in memory. The staged
// handles are released so the WAL checkpoint can advance past them.
func (m *Manager) RemoveView(vchannel string) {
	m.mu.Lock()
	view, ok := m.views[vchannel]
	if ok {
		delete(m.views, vchannel)
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	view.mu.Lock()
	records := view.takeStagingLocked()
	view.mu.Unlock()
	for _, record := range records {
		record.handle.Release()
	}
}

// SetMaterializedTimeTick mirrors the transform materialization frontier of a
// vchannel into the retention computation. It is updated by the transform
// consumer as it emits L0 output; records below the frontier are eligible for
// release.
func (m *Manager) SetMaterializedTimeTick(vchannel string, timetick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.materializedFrontiers[vchannel] < timetick {
		m.materializedFrontiers[vchannel] = timetick
	}
}

// onStagingGrown accounts one view's staging growth toward the pchannel-wide
// flush threshold. It is called from the WAL observation path after the view
// appended a record; it must not block.
//
// The cached total can transiently over-count right after a collect (a grow
// whose record was already taken by the flush), so a suspected threshold hit
// is confirmed by recomputing the total from the views before scheduling.
func (m *Manager) onStagingGrown(delta uint64) {
	m.mu.Lock()
	m.stagingBytes += delta
	if m.stagingBytes >= m.cfg.FlushMaxBytes {
		total := m.totalStagingLocked()
		m.stagingBytes = total
		m.mu.Unlock()
		if total >= m.cfg.FlushMaxBytes {
			m.requestFlush()
		}
		return
	}
	m.mu.Unlock()
}

// totalStagingLocked recomputes the pchannel-wide staging bytes from the
// views. Caller holds m.mu.
func (m *Manager) totalStagingLocked() uint64 {
	var total uint64
	for _, view := range m.views {
		view.mu.Lock()
		total += view.stagingBytes
		view.mu.Unlock()
	}
	return total
}

// requestFlush schedules one flush task. It is called from the WAL
// observation path; it must not block.
func (m *Manager) requestFlush() {
	m.mu.Lock()
	task := m.newFlushTaskLocked()
	m.mu.Unlock()
	if task == nil {
		return
	}
	if scheduler := m.cfg.Runtime.Scheduler; scheduler != nil {
		scheduler.Submit(task)
	}
}

// newFlushTaskLocked returns a flush task, or nil when one is already
// scheduled or in flight. Caller holds m.mu.
//
// At most one flush task exists at a time: the executing task collects every
// view's staging (see collectStaging), so a request while one is pending would
// only duplicate the same work. The task is compacted away once done, and the
// next request then schedules the successor — flushes stay strictly
// sequential, and the task set stays O(1) instead of growing one task per
// observation.
func (m *Manager) newFlushTaskLocked() *summaryFlushTask {
	m.flushTasks = compactFlushTasks(m.flushTasks)
	if len(m.flushTasks) > 0 {
		return nil
	}
	task := &summaryFlushTask{
		log: m,
	}
	m.flushTasks = append(m.flushTasks, task)
	return task
}

// HasPendingWork reports whether a flush is scheduled or in flight, or a view
// still holds staging. Recovery uses it to decide whether a drop cleanup may
// tear the summary down.
func (m *Manager) HasPendingWork() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushTasks = compactFlushTasks(m.flushTasks)
	return len(m.flushTasks) > 0
}

// ReadTransformEntries returns the durable transform entries of one vchannel
// with timetick in (from, to], collected from the retained chunks. The chunks
// are scanned in generation order; a chunk whose span ends at or before from
// is skipped, a chunk whose span starts after to stops the scan.
//
// The transform consumer never reads object storage at runtime: it observes
// messages directly. This method is the one-time recovery path: after a
// restart the consumer's in-memory window is empty, and recovery loads the
// durable backlog between the restored materialization frontier and the
// durable frontier through this method.
func (m *Manager) ReadTransformEntries(
	ctx context.Context,
	vchannel string,
	from, to uint64,
) ([]*streamingpb.TransformLogEntry, error) {
	m.mu.Lock()
	// Snapshot the chunk index under the lock: publishManifest appends to and
	// sorts the live slice's backing array in place, so iterating the live
	// slice unlocked could skip or duplicate entries under a concurrent flush.
	chunks := append([]*streamingpb.PChannelSummaryChunkIndexEntry(nil), m.manifest.GetChunks()...)
	m.mu.Unlock()
	out := make([]*streamingpb.TransformLogEntry, 0)
	for _, chunk := range chunks {
		if chunk.GetEndTimetick() <= from {
			continue
		}
		if chunk.GetStartTimetick() > to {
			break
		}
		index := vchannelChunkIndex(chunk, vchannel)
		if index == nil {
			continue
		}
		records, err := m.cfg.Store.ReadTransformSection(ctx, chunk.GetGeneration(), chunk.GetTerm(), vchannel, index)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			tt := record.GetTimeTick()
			if tt <= from || tt > to {
				continue
			}
			out = append(out, &streamingpb.TransformLogEntry{
				TimeTick: tt,
				Entry: &streamingpb.TransformLogEntry_Delete{
					Delete: record.GetDelete(),
				},
			})
		}
	}
	return out, nil
}

// vchannelChunkIndex returns a chunk's index entry of one vchannel, or nil.
func vchannelChunkIndex(chunk *streamingpb.PChannelSummaryChunkIndexEntry, vchannel string) *streamingpb.VChannelSummaryChunkIndex {
	for _, index := range chunk.GetVchannels() {
		if index.GetVchannel() == vchannel {
			return index
		}
	}
	return nil
}

// LatestCoveredTimeTick returns the newest timetick covered by a durable
// chunk.
func (m *Manager) LatestCoveredTimeTick() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestCoveredTimeTick
}

// flushOnce executes one flush: collect the staging of every view, write one
// chunk, publish the manifest, then release the message handles. It is the
// execution body of summaryFlushTask.
//
// flushOnce is the execution body of summaryFlushTask. Every failure path pins
// the batch on the task so a retry rewrites the exact same bytes under the
// same generation: a chunk-write failure (pendingChunk) and a manifest-publish
// failure (pendingPublish) both retry with the pinned batch, never a
// re-collection of the grown staging set. A grown batch under the same
// generation would collide with an already-durable (or partially written)
// chunk object (storeCorrupted) and livelock the flush forever. Staging that
// arrives during the retry window stays in the views and is collected by the
// next flush task into a fresh generation.
func (m *Manager) flushOnce(ctx context.Context, task *summaryFlushTask) error {
	// A chunk write failed on a previous attempt: the object may or may not be
	// durable (a dropped ack after a successful write). Retry the SAME
	// generation with the SAME pinned batch; new staging is never folded into
	// this batch, so the rewrite is byte-identical and idempotent.
	if task.pendingChunk != nil {
		generation := task.pendingChunk.generation
		batch := task.pendingChunk.batch
		if err := m.persistChunk(ctx, generation, batch); err != nil {
			return err
		}
		task.pendingChunk = nil
		if err := m.publishManifest(ctx, generation, batch); err != nil {
			task.pendingPublish = &pendingPublishState{generation: generation, batch: batch}
			return err
		}
		m.completeBatch(generation, batch)
		return nil
	}
	// A chunk durably written by a previous attempt whose manifest publish
	// failed: retry only the publish with the pinned batch. New staging stays
	// in the views and is collected by the next flush task into a fresh
	// generation after this publish succeeds.
	if task.pendingPublish != nil {
		if err := m.publishManifest(ctx, task.pendingPublish.generation, task.pendingPublish.batch); err != nil {
			return err
		}
		m.completeBatch(task.pendingPublish.generation, task.pendingPublish.batch)
		task.pendingPublish = nil
		return nil
	}
	// Collect the staging under the view locks. The records stay logically
	// owned by the views until the chunk and manifest are durable.
	batch := m.collectStaging()
	// Re-sync the pchannel staging total: the collect emptied every view, and
	// a grow whose notification is still in flight must not leave the cached
	// total over-counting.
	m.mu.Lock()
	m.stagingBytes = m.totalStagingLocked()
	m.mu.Unlock()
	if len(batch.recordsByVChannel) == 0 {
		return nil
	}
	generation := m.claimGenerationLocked()
	if err := m.persistChunk(ctx, generation, batch); err != nil {
		// The generation stays claimed: the pinned retry writes it. Never
		// restore the staging — the retry must rewrite the identical bytes, and
		// newer records go to a later generation, not into this batch.
		task.pendingChunk = &pendingChunkState{generation: generation, batch: batch}
		return err
	}
	if err := m.publishManifest(ctx, generation, batch); err != nil {
		task.pendingPublish = &pendingPublishState{generation: generation, batch: batch}
		return err
	}
	// Durable end to end: release the handles, which lets the WAL checkpoint
	// advance past every record of this batch.
	m.completeBatch(generation, batch)
	return nil
}

// collectStaging takes the staging of every view.
func (m *Manager) collectStaging() *flushBatch {
	m.mu.Lock()
	views := make([]*SummaryView, 0, len(m.views))
	for _, view := range m.views {
		views = append(views, view)
	}
	m.mu.Unlock()
	batch := &flushBatch{
		recordsByVChannel: make(map[string][]*stagedRecord),
	}
	for _, view := range views {
		view.mu.Lock()
		records := view.takeStagingLocked()
		view.mu.Unlock()
		if len(records) > 0 {
			batch.recordsByVChannel[view.vchannel] = records
		}
	}
	return batch
}

// claimGenerationLocked consumes the next generation for one flush.
func (m *Manager) claimGenerationLocked() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation := m.nextGeneration
	m.nextGeneration++
	return generation
}

// persistChunk writes the chunk object for one batch and derives the manifest
// entry from the returned footer. The generation stays claimed by the pinned
// retry on failure (see flushOnce); a chunk durable but unrecorded is found by
// recovery probing forward.
func (m *Manager) persistChunk(ctx context.Context, generation uint64, batch *flushBatch) error {
	records := make(map[string][]*streamingpb.VChannelSummaryTransformRecord, len(batch.recordsByVChannel))
	maxTimeTick := uint64(0)
	for vchannel, staged := range batch.recordsByVChannel {
		records[vchannel] = make([]*streamingpb.VChannelSummaryTransformRecord, 0, len(staged))
		for _, record := range staged {
			records[vchannel] = append(records[vchannel], &streamingpb.VChannelSummaryTransformRecord{
				TimeTick: record.timeTick,
				Delete:   record.entry.GetDelete(),
			})
			if record.timeTick > maxTimeTick {
				maxTimeTick = record.timeTick
			}
		}
	}
	footer, objectSize, err := m.cfg.Store.WriteChunk(ctx, generation, records)
	if err != nil {
		return err
	}
	batch.footer = footer
	batch.objectSize = objectSize
	batch.maxTimeTick = maxTimeTick
	return nil
}

// publishManifest amends the in-memory manifest with the new chunk and writes
// it. Caller must have already made the chunk durable.
//
// The publish is serialized with the GC publish through manifestMu: the
// manifest is amended under mu, marshalled from a clone taken under the same
// lock, and only installed on success. A concurrent GC edit of the same
// backing arrays can therefore never be marshalled in a torn state, and a
// failed write leaves the on-disk state unchanged.
//
// On write failure the amendment and the claimed generation are rolled back,
// so the flush retry rewrites the same generation: the batch is never
// duplicated into two chunk objects.
func (m *Manager) publishManifest(ctx context.Context, generation uint64, batch *flushBatch) error {
	m.manifestMu.Lock()
	defer m.manifestMu.Unlock()
	m.mu.Lock()
	recordChunk(m.manifest, chunkIndexEntryFromFooter(batch.footer, batch.objectSize))
	next := proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest)
	m.mu.Unlock()
	if err := m.cfg.Store.WriteManifest(ctx, next); err != nil {
		m.mu.Lock()
		m.manifest.Chunks = removeChunkEntry(m.manifest.Chunks, generation)
		if m.nextGeneration > generation {
			m.nextGeneration = generation
		}
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.manifest = next
	m.mu.Unlock()
	return nil
}

// completeBatch installs the durable state and releases the handles.
func (m *Manager) completeBatch(generation uint64, batch *flushBatch) {
	m.mu.Lock()
	// A publish failure rolls the claimed generation back; a pinned-batch
	// retry that then succeeds must advance past it, or the next flush would
	// claim the generation again and collide with the durable chunk object.
	if m.nextGeneration <= generation {
		m.nextGeneration = generation + 1
	}
	if batch.maxTimeTick > m.latestCoveredTimeTick {
		m.latestCoveredTimeTick = batch.maxTimeTick
	}
	views := make([]*SummaryView, 0, len(batch.recordsByVChannel))
	for vchannel := range batch.recordsByVChannel {
		// The view may already be gone (vchannel cleanup raced the flush);
		// its staging was released by RemoveView, so nothing to mark durable.
		if view, ok := m.views[vchannel]; ok {
			views = append(views, view)
		}
	}
	m.mu.Unlock()
	for _, view := range views {
		view.markDurable(batch.maxTimeTick)
	}
	for _, records := range batch.recordsByVChannel {
		for _, record := range records {
			record.handle.Release()
		}
	}
}

// flushBatch is one flush's collected staging.
type flushBatch struct {
	recordsByVChannel map[string][]*stagedRecord
	footer            *streamingpb.PChannelSummaryChunkFooter
	objectSize        uint64
	maxTimeTick       uint64
}

// summaryFlushTask is a nodescheduler task running one flush. At most one task
// exists at a time (see newFlushTaskLocked), so there is no predecessor graph.
type summaryFlushTask struct {
	log  *Manager
	done atomic.Bool
	// pendingChunk pins the batch of a chunk whose write failed: the retry
	// rewrites the SAME generation with the SAME batch instead of re-collecting
	// staging (see flushOnce), so a grown batch can never collide with a
	// partially written chunk object.
	pendingChunk *pendingChunkState
	// pendingPublish pins the batch of a chunk that was durably written but
	// whose manifest publish failed: the retry publishes the same generation
	// instead of re-collecting staging (see flushOnce).
	pendingPublish *pendingPublishState
}

// pendingChunkState is the pinned state of a batch whose chunk write failed.
// The chunk key may already hold this payload (a dropped ack after a
// successful write), so the retry must rewrite the identical bytes.
type pendingChunkState struct {
	generation uint64
	batch      *flushBatch
}

// pendingPublishState is the pinned state of a chunk written but not yet
// recorded in a published manifest.
type pendingPublishState struct {
	generation uint64
	batch      *flushBatch
}

// Done reports whether the task finished.
func (t *summaryFlushTask) Done() bool {
	return t.done.Load()
}

// Execute runs the flush. Transient failures are marked ErrDelay so the
// scheduler retries the task with backoff; flushOnce pinned the failed batch
// (pendingChunk for a chunk write, pendingPublish for a manifest publish), so
// a retry never re-collects a grown staging set into the same chunk key. A
// terminal failure (corrupted or fenced store) drops the task instead: the
// pinned batch is abandoned with its handles retained, so the WAL checkpoint
// stalls before it and recovery replays it after a restart.
func (t *summaryFlushTask) Execute(ctx context.Context) error {
	if err := t.log.flushOnce(ctx, t); err != nil {
		if isTerminalSummaryFlushError(err) {
			t.done.Store(true)
			if logger := t.log.cfg.Logger; logger != nil {
				logger.Error(ctx, "summary flush failed terminally, abandoning pinned batch", mlog.Err(err))
			}
			return err
		}
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	t.done.Store(true)
	return nil
}

func compactFlushTasks(tasks []*summaryFlushTask) []*summaryFlushTask {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	clear(pending[len(pending):])
	return pending
}

var _ nodescheduler.Task = (*summaryFlushTask)(nil)
