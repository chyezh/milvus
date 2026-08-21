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

// Package transformlog implements the transform consumer of a vchannel: it
// turns the transform records of the WALSummary into L0 segments.
//
// The transform records used to be persisted per-vchannel as TransformLogChunk
// objects with a VChannelTransformLogMeta recovery meta. That persistence now
// lives in the pchannel-scoped WALSummary (see the walsummary package), which
// decides entirely on its own when records become durable. This package never
// reads the summary store: it registers as a FlushListener of the summary and
// replaces its in-memory materialization window from the delivered flush
// events. Its materialization frontier is carried by VChannelMeta
// (transform_materialized_time_tick), persisted together with the vchannel
// catalog snapshot instead of a dedicated transform meta.
package transformlog

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Config carries the wiring of one vchannel's transform consumer.
type Config struct {
	VChannel string
	// MaterializedTimeTick is the initial materialization frontier, restored
	// from VChannelMeta.transform_materialized_time_tick.
	MaterializedTimeTick uint64
	// PendingEntries is the initial in-memory materialization window: the
	// durable records of this vchannel after MaterializedTimeTick, loaded
	// once by recovery. Runtime flushes replace it through OnSummaryFlushed.
	PendingEntries      []*streamingpb.TransformLogEntry
	MaterializeMaxRows  uint64
	MaterializeMaxBytes uint64
	Materializer        Materializer
	Runtime             moduleapi.Runtime
	// OnMaterialized is invoked with the new frontier after a materialization
	// batch commits. It must be non-blocking and must not call back into the
	// TransformLog. The vchannel module uses it to mirror the frontier into
	// VChannelMeta and mark the vchannel snapshot dirty.
	OnMaterialized func(timeTick uint64)
}

type materializeOption struct {
	TargetTimeTick uint64
}

type materializeResult struct {
	Started                 bool
	MaterializedTimeTick    uint64
	MaterializedRows        uint64
	MaterializedBytes       uint64
	HasMaterializedSegments bool
}

// TransformLog is the transform consumer of one vchannel: it materializes the
// summary's transform records into L0 segments.
type TransformLog struct {
	materializeMu sync.Mutex
	mu            sync.Mutex
	vchannel      string

	// materializedTimeTick is the committed materialization frontier: every
	// transform record through this timetick has been emitted as L0 output.
	materializedTimeTick uint64
	// materializeUpperBound is the VChannel-wide L1 safety frontier: the
	// largest timetick materialization may currently reach.
	materializeUpperBound uint64
	// requestedMaterializeTimeTick is the desired materialization frontier,
	// retained so advancing the upper bound can continue the same request
	// without another WAL trigger.
	requestedMaterializeTimeTick uint64
	// durableTimeTick is the newest timetick the summary has made durable
	// for this vchannel. It advances only through flush events.
	durableTimeTick uint64
	// pending is the in-memory materialization window: the durable records
	// of this vchannel after the committed frontier, in ascending timetick
	// order. Flush events append to it; committed batches trim its head.
	pending []*streamingpb.TransformLogEntry

	materializeMaxRows  uint64
	materializeMaxBytes uint64
	materializer        Materializer
	runtime             moduleapi.Runtime
	onMaterialized      func(uint64)

	materializeTasks []*transformMaterializeTask
}

// New creates the transform consumer of one vchannel.
func New(config Config) *TransformLog {
	upperBound := uint64(math.MaxUint64)
	if config.MaterializeMaxRows == 0 {
		config.MaterializeMaxRows = defaultMaterializeMaxRows
	}
	if config.MaterializeMaxBytes == 0 {
		config.MaterializeMaxBytes = defaultMaterializeMaxBytes
	}
	// The initial window loaded by recovery may carry records already
	// committed by the restored frontier; trim them defensively.
	pending := config.PendingEntries[:0]
	for _, entry := range config.PendingEntries {
		if entry != nil && entry.GetTimeTick() > config.MaterializedTimeTick {
			pending = append(pending, entry)
		}
	}
	durable := config.MaterializedTimeTick
	if len(pending) > 0 {
		durable = pending[len(pending)-1].GetTimeTick()
	}
	return &TransformLog{
		vchannel:              config.VChannel,
		materializedTimeTick:  config.MaterializedTimeTick,
		materializeUpperBound: upperBound,
		durableTimeTick:       durable,
		pending:               pending,
		materializer:          config.Materializer,
		materializeMaxRows:    config.MaterializeMaxRows,
		materializeMaxBytes:   config.MaterializeMaxBytes,
		runtime:               config.Runtime,
		onMaterialized:        config.OnMaterialized,
	}
}

// MaterializedTimeTick returns the committed materialization frontier.
func (t *TransformLog) MaterializedTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.materializedTimeTick
}

// DurableTimeTick returns the newest timetick the summary has made durable
// for this vchannel. Materialization never runs past it: only records already
// persisted by the summary are visible to the consumer.
func (t *TransformLog) DurableTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.durableTimeTick
}

// OnSummaryFlushed implements walsummary.FlushListener: it replaces the
// in-memory window with the durable records of the flushed chunk. The summary
// alone decides when persistence happens; this consumer only observes the
// outcome. It must be non-blocking and must not call back into the summary.
func (t *TransformLog) OnSummaryFlushed(batch *walsummary.FlushedBatch) {
	if t == nil {
		return
	}
	entries := batch.RecordsByVChannel[t.vchannel]
	if len(entries) == 0 {
		return
	}
	t.mu.Lock()
	last := entries[len(entries)-1].GetTimeTick()
	if last > t.durableTimeTick {
		t.durableTimeTick = last
	}
	// Append only what the committed frontier does not cover yet. The entries
	// of one flush are ascending, and flushes are strictly sequential, so the
	// window stays ascending.
	for _, entry := range entries {
		if entry.GetTimeTick() > t.materializedTimeTick {
			t.pending = append(t.pending, entry)
		}
	}
	// New durable data may unblock a pending materialization request.
	task := t.newRequestedMaterializeTaskLocked()
	t.mu.Unlock()
	if task != nil && t.runtime.Scheduler != nil {
		t.runtime.Scheduler.Submit(task)
	}
}

// RequestMaterializeThrough records the desired materialization frontier and
// schedules the largest safe prefix allowed by the current L1 upper bound.
// The desired frontier is retained so advancing the upper bound can continue
// the same request without another WAL trigger.
func (t *TransformLog) RequestMaterializeThrough(timetick uint64) bool {
	if t == nil || t.runtime.Scheduler == nil {
		return false
	}
	t.mu.Lock()
	if timetick > t.requestedMaterializeTimeTick {
		t.requestedMaterializeTimeTick = timetick
	}
	task := t.newRequestedMaterializeTaskLocked()
	t.mu.Unlock()
	if task == nil {
		return false
	}
	t.runtime.Scheduler.Submit(task)
	return true
}

// SetMaterializeUpperBound updates the VChannel-wide L1 safety frontier and
// retries any previously requested materialization that can now make progress.
// Publishing the same bound again is a no-op: the bound only changes on segment
// create / cleanup / final-commit transitions, and skipping unchanged publishes
// keeps the WAL observation hot path from rescheduling materialize tasks on
// every accepted insert.
func (t *TransformLog) SetMaterializeUpperBound(timetick uint64) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	if timetick == t.materializeUpperBound {
		t.mu.Unlock()
		return false
	}
	t.materializeUpperBound = timetick
	if t.runtime.Scheduler == nil {
		t.mu.Unlock()
		return false
	}
	task := t.newRequestedMaterializeTaskLocked()
	t.mu.Unlock()
	if task == nil {
		return false
	}
	t.runtime.Scheduler.Submit(task)
	return true
}

// materializeTargetLocked returns the largest materialization frontier that is
// both requested and allowed by the current L1 upper bound.
func (t *TransformLog) materializeTargetLocked() uint64 {
	target := t.requestedMaterializeTimeTick
	if target > t.materializeUpperBound {
		target = t.materializeUpperBound
	}
	return target
}

// materialize executes one materialization batch through targetTimeTick.
func (t *TransformLog) materialize(ctx context.Context, opt materializeOption) (materializeResult, error) {
	t.materializeMu.Lock()
	defer t.materializeMu.Unlock()
	t.mu.Lock()
	targetTimeTick := opt.TargetTimeTick
	if targetTimeTick == 0 {
		targetTimeTick = t.materializeTargetLocked()
	}
	if targetTimeTick > t.materializeUpperBound {
		targetTimeTick = t.materializeUpperBound
	}
	if targetTimeTick <= t.materializedTimeTick {
		t.mu.Unlock()
		return materializeResult{}, nil
	}
	// Only records already made durable by the summary are visible: the
	// consumer never reads the summary store, its window moves with flush
	// events only.
	if targetTimeTick > t.durableTimeTick {
		targetTimeTick = t.durableTimeTick
	}
	if targetTimeTick <= t.materializedTimeTick {
		t.mu.Unlock()
		return materializeResult{}, nil
	}
	maxRows := t.materializeMaxRows
	maxBytes := t.materializeMaxBytes
	t.mu.Unlock()

	work := t.prepareMaterialize(targetTimeTick, maxRows, maxBytes)

	if len(work.Entries) > 0 {
		if t.materializer == nil {
			return materializeResult{}, merr.WrapErrServiceInternalMsg("transform log materializer is nil")
		}
		if err := t.materializer.Materialize(ctx, MaterializeRequest{
			VChannel:       t.vchannel,
			TargetTimeTick: work.TargetTimeTick,
			Entries:        work.Entries,
			MaxRows:        maxRows,
			MaxBytes:       maxBytes,
		}); err != nil {
			return materializeResult{}, err
		}
	}

	t.mu.Lock()
	result := t.commitMaterializeLocked(work)
	var nextTask *transformMaterializeTask
	if t.runtime.Scheduler != nil {
		// Schedule the continuation while the retained request is still
		// pending. This covers both a capped batch (rows/bytes limit) and a
		// frontier that advanced only partially: the summary may not have
		// flushed the requested frontier yet (durable-timetick cutoff) or the
		// L1 upper bound may have retracted it. The current task has not
		// completed yet, so it becomes a predecessor of the continuation and
		// the batches stay strictly sequential; a continuation whose data has
		// not been flushed yet stays delayed in the scheduler.
		if target := t.materializeTargetLocked(); target > t.materializedTimeTick {
			nextTask = t.newMaterializeTaskLocked(target)
		}
	}
	t.mu.Unlock()
	if nextTask != nil {
		t.runtime.Scheduler.Submit(nextTask)
	}
	if t.onMaterialized != nil && work.TargetTimeTick > 0 {
		t.onMaterialized(work.TargetTimeTick)
	}
	return result, nil
}

// prepareMaterialize walks the in-memory window through targetTimeTick and
// caps the batch by rows and bytes. The retained materialize request
// continues from the capped frontier in a follow-up task, so a whole backlog
// is never built into a single Materialize call. The window holds only
// durable records, so nothing here reads the summary store.
func (t *TransformLog) prepareMaterialize(
	targetTimeTick uint64,
	maxRows uint64,
	maxBytes uint64,
) materializeWork {
	t.mu.Lock()
	defer t.mu.Unlock()
	cursor := t.materializedTimeTick
	work := materializeWork{TargetTimeTick: cursor}
	lastIncluded := cursor
	for _, entry := range t.pending {
		tt := entry.GetTimeTick()
		if tt <= cursor {
			continue
		}
		if tt > targetTimeTick {
			break
		}
		if !isTransformDeleteEntry(entry) {
			continue
		}
		rows := transformLogEntryRows(entry)
		bytes := uint64(proto.Size(entry))
		if work.Rows > 0 && (work.Rows+rows > maxRows || work.Bytes+bytes > maxBytes) {
			work.TargetTimeTick = lastIncluded
			return work
		}
		work.Entries = append(work.Entries, entry)
		work.Rows += rows
		work.Bytes += bytes
		lastIncluded = tt
	}
	work.TargetTimeTick = lastIncluded
	return work
}

func (t *TransformLog) commitMaterializeLocked(work materializeWork) materializeResult {
	if work.TargetTimeTick <= t.materializedTimeTick {
		return materializeResult{}
	}
	t.materializedTimeTick = work.TargetTimeTick
	// Trim the consumed head of the window: every record through the committed
	// frontier has been emitted as L0 output and is no longer needed.
	trim := 0
	for trim < len(t.pending) && t.pending[trim].GetTimeTick() <= t.materializedTimeTick {
		trim++
	}
	if trim > 0 {
		t.pending = append([]*streamingpb.TransformLogEntry(nil), t.pending[trim:]...)
	}
	return materializeResult{
		Started:                 true,
		MaterializedTimeTick:    work.TargetTimeTick,
		MaterializedRows:        work.Rows,
		MaterializedBytes:       work.Bytes,
		HasMaterializedSegments: len(work.Entries) > 0,
	}
}

type materializeWork struct {
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	Rows           uint64
	Bytes          uint64
}

// isTransformDeleteEntry reports whether an entry carries a delete payload.
func isTransformDeleteEntry(entry *streamingpb.TransformLogEntry) bool {
	if entry == nil {
		return false
	}
	return entry.GetDelete() != nil
}
