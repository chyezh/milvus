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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// SummaryView observes the WAL messages of one vchannel and collects the
// transform records that are not yet durable in a summary chunk.
//
// A message that carries no transform payload is not retained at all: it has
// nothing to persist, so the caller may release its handle immediately. A
// message that does produce a record is retained (one handle per message) from
// observation until the chunk covering it is durable and the manifest records
// it; releasing the handle then is what lets the WAL checkpoint advance past
// the record.
type SummaryView struct {
	manager  *Manager
	vchannel string

	mu sync.Mutex
	// staging holds the records observed but not yet written to a chunk, in
	// observation order (which is WAL order for a single observer).
	staging []*stagedRecord
	// stagingBytes is the estimated on-disk size of the staging records. It is
	// the input to the autonomous flush decision.
	stagingBytes uint64
	// durableTimeTick is the newest record timetick already covered by a
	// durable chunk. Records with timetick > durableTimeTick are only in
	// staging or nowhere.
	durableTimeTick uint64
	// latestTimeTick is the newest timetick observed on this vchannel,
	// payload-carrying or not. It is the materialization ceiling: no record
	// may be materialized past the newest observed message.
	latestTimeTick uint64
}

// stagedRecord is one retained WAL message and the transform record it yields.
type stagedRecord struct {
	timeTick uint64
	entry    *streamingpb.TransformLogEntry
	// handle keeps the WAL message alive until the record is durable.
	handle message.RetainedImmutableMessage
}

// NewSummaryView creates the summary view of one vchannel.
func NewSummaryView(manager *Manager, vchannel string) *SummaryView {
	return &SummaryView{
		manager:  manager,
		vchannel: vchannel,
	}
}

// VChannel returns the vchannel of the view.
func (v *SummaryView) VChannel() string {
	return v.vchannel
}

// ObserveMessage observes one WAL message. It is called synchronously on the
// WAL observation hot path; it must not block or do metadata I/O.
//
// The view retains one handle per payload-carrying message. The caller keeps
// ownership of its own handle; the view's clone is released by the manager once
// the covering chunk is durable.
//
// A record whose timetick is not greater than the durable frontier is skipped:
// recovery replay re-observes records the manifest already covers, and staging
// them again would rewrite the same records into new chunks. The durable
// frontier is restored from the manifest by the recovery path.
func (v *SummaryView) ObserveMessage(ctx context.Context, retained message.RetainedImmutableMessage) {
	if retained == nil {
		return
	}
	msg := retained.Message()
	v.mu.Lock()
	if msg.TimeTick() > v.latestTimeTick {
		v.latestTimeTick = msg.TimeTick()
	}
	entry := messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{})
	if entry == nil || entry.GetTimeTick() <= v.durableTimeTick {
		v.mu.Unlock()
		return
	}
	handle := retained.Clone()
	v.staging = append(v.staging, &stagedRecord{
		timeTick: entry.GetTimeTick(),
		entry:    entry,
		handle:   handle,
	})
	v.stagingBytes += uint64(proto.Size(entry))
	// Capture the threshold decision under the lock: takeStagingLocked (the
	// flush worker) resets this field concurrently, so reading it unlocked
	// could both miss and double-trigger a flush.
	overThreshold := v.stagingBytes >= v.manager.config().FlushMaxBytes
	v.mu.Unlock()
	if overThreshold {
		v.manager.requestFlush()
	}
}

// Manager returns the owning summary manager.
func (v *SummaryView) Manager() *Manager {
	return v.manager
}

// LatestTimeTick returns the newest timetick observed on this vchannel.
func (v *SummaryView) LatestTimeTick() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.latestTimeTick
}

// RequestPersistThrough schedules persistence when the view holds staging
// records whose timetick is not greater than targetTimeTick. It is the forced
// persist path driven by the tracker (stall / under pressure): the caller
// needs the records through targetTimeTick durable before the checkpoint may
// advance.
func (v *SummaryView) RequestPersistThrough(targetTimeTick uint64) {
	v.mu.Lock()
	hasStagingThrough := false
	for _, record := range v.staging {
		if record.timeTick <= targetTimeTick {
			hasStagingThrough = true
			break
		}
	}
	v.mu.Unlock()
	if hasStagingThrough {
		v.manager.requestFlush()
	}
}

// takeStagingLocked moves every staged record out of the view. The records
// remain logically owned by the view until markDurable is called: the handles
// are only released then, so a flush failure loses nothing.
func (v *SummaryView) takeStagingLocked() []*stagedRecord {
	if len(v.staging) == 0 {
		return nil
	}
	records := v.staging
	v.staging = nil
	v.stagingBytes = 0
	return records
}

// markDurable advances the durable frontier after a successful flush. It must
// only be called with the records this flush wrote; the handles inside are
// released by the caller.
func (v *SummaryView) markDurable(timeTick uint64) {
	v.mu.Lock()
	if timeTick > v.durableTimeTick {
		v.durableTimeTick = timeTick
	}
	v.mu.Unlock()
}

// DurableTimeTick returns the newest record timetick covered by a durable
// chunk.
func (v *SummaryView) DurableTimeTick() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.durableTimeTick
}

// recordTimetickRange returns the [start, end] span of the records, or zeros
// when empty.
func recordTimetickRange(records []*stagedRecord) (uint64, uint64) {
	var start, end uint64
	for _, record := range records {
		if start == 0 || record.timeTick < start {
			start = record.timeTick
		}
		if record.timeTick > end {
			end = record.timeTick
		}
	}
	return start, end
}
