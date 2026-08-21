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
	mu   sync.Mutex
	cfg  ManagerConfig
	view *SummaryView

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

	// flushTasks are the in-flight / queued flush tasks, ordered by
	// scheduling; every task is a predecessor of the next, so flushes never
	// overlap and generations are claimed in order.
	flushTasks []*summaryFlushTask
	// flushThrough is the newest timetick some flush task was asked to cover.
	flushThrough uint64
}

// ManagerConfig carries the wiring of one pchannel's summary manager.
type ManagerConfig struct {
	PChannel string
	Term     int64
	// Store is the object storage layer of the summary store.
	Store *Store
	// Runtime provides the scheduler and the module notifier.
	Runtime moduleapi.Runtime
	// FlushMaxBytes is the staging size that triggers an autonomous flush.
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
		manifest:              &streamingpb.PChannelSummaryManifest{},
		materializedFrontiers: make(map[string]uint64),
	}
}

func (m *Manager) config() ManagerConfig {
	return m.cfg
}

// View returns the summary view of a vchannel, creating it on first use. A
// pchannel has one view per vchannel for the whole lifetime of the manager.
func (m *Manager) View(vchannel string) *SummaryView {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.view == nil {
		m.view = NewSummaryView(m, vchannel)
	}
	return m.view
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

// requestFlush schedules one flush task. It is called from the WAL
// observation path; it must not block.
func (m *Manager) requestFlush() {
	m.mu.Lock()
	m.flushThrough = ^uint64(0)
	task := m.newFlushTaskLocked()
	m.mu.Unlock()
	if task == nil {
		return
	}
	if scheduler := m.cfg.Runtime.Scheduler; scheduler != nil {
		scheduler.Submit(task)
	}
}

// requestFlushThrough schedules a flush that must cover every record through
// targetTimeTick. Once triggered, the whole current staging is batched into
// the scheduled frontier.
func (m *Manager) requestFlushThrough(targetTimeTick uint64) {
	m.mu.Lock()
	if targetTimeTick > m.flushThrough {
		m.flushThrough = targetTimeTick
	}
	task := m.newFlushTaskLocked()
	m.mu.Unlock()
	if task == nil {
		return
	}
	if scheduler := m.cfg.Runtime.Scheduler; scheduler != nil {
		scheduler.Submit(task)
	}
}

// newFlushTaskLocked appends a flush task whose predecessors are every task
// scheduled before it. Caller holds m.mu.
func (m *Manager) newFlushTaskLocked() *summaryFlushTask {
	m.flushTasks = compactFlushTasks(m.flushTasks)
	predecessors := make([]*summaryFlushTask, 0, len(m.flushTasks))
	for _, task := range m.flushTasks {
		predecessors = append(predecessors, task)
	}
	task := &summaryFlushTask{
		log: m,
	}
	task.predecessors = predecessors
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
// On any failure the collected staging is restored to the views, so the
// records and their handles survive for the retry. A chunk that was written
// but never recorded (failure in publishManifest) is found by recovery
// probing forward, and its generation is not consumed.
func (m *Manager) flushOnce(ctx context.Context) error {
	// Collect the staging under the view locks. The records stay logically
	// owned by the views until the chunk and manifest are durable.
	batch := m.collectStaging()
	if len(batch.recordsByVChannel) == 0 {
		return nil
	}
	generation := m.claimGenerationLocked()
	if err := m.persistChunk(ctx, generation, batch); err != nil {
		m.restoreStaging(batch)
		return err
	}
	// Chunk durable: record it in the manifest and publish. The manifest write
	// must follow the chunk write; a crash between them is repaired by
	// recovery probing forward.
	if err := m.publishManifest(ctx, generation, batch); err != nil {
		m.restoreStaging(batch)
		return err
	}
	// Durable end to end: release the handles, which lets the WAL checkpoint
	// advance past every record of this batch.
	m.completeBatch(generation, batch)
	return nil
}

// restoreStaging puts a failed batch's records back into their views, in the
// original order. The handles were never released, so the records remain
// valid; only the staging bookkeeping needs rebuilding.
func (m *Manager) restoreStaging(batch *flushBatch) {
	for vchannel, records := range batch.recordsByVChannel {
		view := m.View(vchannel)
		view.mu.Lock()
		view.staging = append(records, view.staging...)
		for _, record := range records {
			view.stagingBytes += uint64(proto.Size(record.entry))
		}
		view.mu.Unlock()
	}
}

// collectStaging takes the staging of every view.
func (m *Manager) collectStaging() *flushBatch {
	m.mu.Lock()
	view := m.view
	m.mu.Unlock()
	if view == nil {
		return &flushBatch{}
	}
	view.mu.Lock()
	records := view.takeStagingLocked()
	view.mu.Unlock()
	batch := &flushBatch{
		recordsByVChannel: make(map[string][]*stagedRecord),
	}
	if len(records) > 0 {
		batch.recordsByVChannel[view.vchannel] = records
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
// entry from the returned footer. The generation is not consumed on failure:
// a chunk durable but unrecorded is found by recovery probing forward.
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
func (m *Manager) publishManifest(ctx context.Context, generation uint64, batch *flushBatch) error {
	m.mu.Lock()
	recordChunk(m.manifest, chunkIndexEntryFromFooter(batch.footer, batch.objectSize))
	m.mu.Unlock()
	if err := m.cfg.Store.WriteManifest(ctx, m.manifest); err != nil {
		return err
	}
	return nil
}

// completeBatch installs the durable state and releases the handles.
func (m *Manager) completeBatch(generation uint64, batch *flushBatch) {
	m.mu.Lock()
	if batch.maxTimeTick > m.latestCoveredTimeTick {
		m.latestCoveredTimeTick = batch.maxTimeTick
	}
	m.mu.Unlock()
	if m.view != nil {
		m.view.markDurable(batch.maxTimeTick)
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

// summaryFlushTask is a nodescheduler task running one flush.
type summaryFlushTask struct {
	log          *Manager
	predecessors []*summaryFlushTask
	done         atomic.Bool
}

// Done reports whether the task finished.
func (t *summaryFlushTask) Done() bool {
	return t.done.Load()
}

// Execute runs the flush once its predecessors finished.
func (t *summaryFlushTask) Execute(ctx context.Context) error {
	if !t.predecessorsDone() {
		return nodescheduler.ErrDelay
	}
	if err := t.log.flushOnce(ctx); err != nil {
		return errors.Mark(err, nodescheduler.ErrDelay)
	}
	t.done.Store(true)
	return nil
}

func (t *summaryFlushTask) predecessorsDone() bool {
	for _, predecessor := range t.predecessors {
		if predecessor != nil && !predecessor.Done() {
			return false
		}
	}
	return true
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
