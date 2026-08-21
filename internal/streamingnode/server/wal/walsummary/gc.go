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
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// GCOnce releases retained chunks that are both (a) above the byte budget
// (soft bound, whole objects) and (b) entirely below every vchannel's release
// floor — the maximum of its materialization frontier and its subscription
// window lower bound. A chunk that still holds a not-yet-materialized record
// is never released, whatever the budget pressure.
//
// The manifest is the only index into the chunk set, so release is a manifest
// edit: the chunk moves from `chunks` to `pending_gc`, the manifest is
// published, and only then is the object deleted. `pending_gc` is both the
// work queue and the progress record: a crash between the manifest write and
// the delete leaves the entry in `pending_gc`, and the next GC run finishes
// the delete. The delete itself is best-effort — a leftover object is inert
// (nothing references it) and is reaped by a later run or by store removal.
//
// A manifest write that fails mid-GC is safe: the in-memory manifest still
// lists everything, and the next attempt redoes the same computation.
func (m *Manager) GCOnce(ctx context.Context) error {
	m.mu.Lock()
	// Finish deletes queued by an earlier GC first.
	pendingGC := m.manifest.GetPendingGc()
	m.mu.Unlock()
	for _, ref := range pendingGC {
		if err := m.cfg.Store.DeleteChunk(ctx, ref.GetGeneration()); err != nil {
			return err
		}
		m.removePendingGC(ref)
	}

	released := m.computeRetention()
	if len(released) == 0 {
		return nil
	}
	// Move the released chunks into pending_gc and publish.
	m.mu.Lock()
	for _, ref := range released {
		m.manifest.Chunks = removeChunkEntry(m.manifest.Chunks, ref.GetGeneration())
		m.manifest.PendingGc = append(m.manifest.PendingGc, ref)
	}
	sortChunkEntries(m.manifest.Chunks)
	manifest := m.manifest
	m.mu.Unlock()
	if err := m.cfg.Store.WriteManifest(ctx, manifest); err != nil {
		return err
	}
	// The manifest no longer references the released chunks: delete the objects.
	// A failure here leaves the entries in pending_gc for the next run.
	for _, ref := range released {
		if err := m.cfg.Store.DeleteChunk(ctx, ref.GetGeneration()); err != nil {
			return err
		}
		m.removePendingGC(ref)
	}
	if logger := m.cfg.Logger; logger != nil {
		logger.Info(ctx, "walsummary gc released chunks",
			mlog.String("pchannel", m.cfg.PChannel),
			mlog.Int("released", len(released)))
	}
	return nil
}

// computeRetention decides which chunks to release, oldest first, while any
// chunk below the release floor of some vchannel it covers is kept.
func (m *Manager) computeRetention() []*streamingpb.PChannelSummaryChunkRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunks := m.manifest.GetChunks()
	if len(chunks) == 0 {
		return nil
	}
	var retainedBytes uint64
	for _, chunk := range chunks {
		retainedBytes += chunk.GetObjectSize()
	}
	if retainedBytes <= m.cfg.RetentionMaxBytes {
		return nil
	}
	// Over budget: release the oldest eligible chunks until under budget. The
	// chunks are already in generation order (manifest invariant).
	released := make([]*streamingpb.PChannelSummaryChunkRef, 0)
	for _, chunk := range chunks {
		if retainedBytes <= m.cfg.RetentionMaxBytes {
			break
		}
		if !m.chunkReleasedLocked(chunk) {
			continue
		}
		released = append(released, &streamingpb.PChannelSummaryChunkRef{
			Generation: chunk.GetGeneration(),
			Term:       chunk.GetTerm(),
		})
		retainedBytes -= chunk.GetObjectSize()
	}
	return released
}

// chunkReleasedLocked reports whether a chunk is eligible for release: every
// vchannel it covers has a release floor at or above the chunk's end timetick.
// Caller holds m.mu.
func (m *Manager) chunkReleasedLocked(chunk *streamingpb.PChannelSummaryChunkIndexEntry) bool {
	for _, index := range chunk.GetVchannels() {
		floor := m.materializedFrontiers[index.GetVchannel()]
		if floor == 0 {
			// No materialization frontier yet: nothing of this vchannel may be
			// released.
			return false
		}
		if index.GetEndTimetick() > floor {
			// The chunk still holds records past the frontier.
			return false
		}
	}
	return true
}

// removePendingGC drops a finished deletion from the pending queue and, when
// that empties the queue, publishes the manifest so recovery stops probing the
// deleted objects.
func (m *Manager) removePendingGC(ref *streamingpb.PChannelSummaryChunkRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := m.manifest.GetPendingGc()[:0]
	for _, existing := range m.manifest.GetPendingGc() {
		if existing.GetGeneration() == ref.GetGeneration() {
			continue
		}
		pending = append(pending, existing)
	}
	m.manifest.PendingGc = pending
}

// removeChunkEntry drops one chunk from the manifest by generation.
func removeChunkEntry(chunks []*streamingpb.PChannelSummaryChunkIndexEntry, generation uint64) []*streamingpb.PChannelSummaryChunkIndexEntry {
	out := chunks[:0]
	for _, chunk := range chunks {
		if chunk.GetGeneration() == generation {
			continue
		}
		out = append(out, chunk)
	}
	clear(out[len(out):])
	return out
}

// sortChunkEntries keeps the manifest index in generation order.
func sortChunkEntries(chunks []*streamingpb.PChannelSummaryChunkIndexEntry) {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].GetGeneration() < chunks[j].GetGeneration()
	})
}
