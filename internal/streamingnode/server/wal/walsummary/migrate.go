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

// MigrateLegacyTransformLogs folds the legacy per-vchannel transform log data
// into the first summary chunk of the store. It is used by the recovery
// migration from the pre-summary format: the legacy TransformLogChunk objects
// are replaced by one summary chunk carrying the same transform records, so
// the transform consumer can read them back through the summary.
//
// The migration is a single all-or-nothing publication: every vchannel goes
// into one chunk, and the manifest publish is the commit point. It is
// idempotent — once the manifest records any chunk (the migration's own, or
// anything recovered by probing), the store already owns the data and the
// migration is a no-op. Recover must have run before this call, so the orphan
// tail of a half-finished previous migration is already folded into the
// manifest.
//
// Each vchannel's entries must be in ascending timetick order with strictly
// increasing timeticks; duplicates are rejected.
func (m *Manager) MigrateLegacyTransformLogs(
	ctx context.Context,
	recordsByVChannel map[string][]*streamingpb.TransformLogEntry,
) error {
	m.mu.Lock()
	if len(m.manifest.GetChunks()) > 0 {
		m.mu.Unlock()
		return nil
	}
	generation := m.nextGeneration
	m.mu.Unlock()

	records := make(map[string][]*streamingpb.VChannelSummaryTransformRecord, len(recordsByVChannel))
	maxTimeTick := uint64(0)
	recordCount := 0
	for vchannel, entries := range recordsByVChannel {
		if err := validateSortedTransformEntries(entries); err != nil {
			return err
		}
		vchannelRecords := make([]*streamingpb.VChannelSummaryTransformRecord, 0, len(entries))
		for _, entry := range entries {
			vchannelRecords = append(vchannelRecords, &streamingpb.VChannelSummaryTransformRecord{
				TimeTick: entry.GetTimeTick(),
				Delete:   entry.GetDelete(),
			})
			if entry.GetTimeTick() > maxTimeTick {
				maxTimeTick = entry.GetTimeTick()
			}
		}
		if len(vchannelRecords) > 0 {
			records[vchannel] = vchannelRecords
			recordCount += len(vchannelRecords)
		}
	}
	if len(records) == 0 {
		return nil
	}
	footer, objectSize, err := m.cfg.Store.WriteChunk(ctx, generation, records)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if len(m.manifest.GetChunks()) > 0 {
		// Another migration (or recovery) published a manifest while this one
		// was writing. The chunk just written is an orphan; probing forward on
		// the next recovery folds it in, and the transform consumer dedups by
		// timetick at materialization time.
		m.mu.Unlock()
		return nil
	}
	recordChunk(m.manifest, chunkIndexEntryFromFooter(footer, objectSize))
	m.nextGeneration = generation + 1
	m.latestCoveredTimeTick = maxTimeTick
	m.mu.Unlock()
	if err := m.cfg.Store.WriteManifest(ctx, m.manifest); err != nil {
		return err
	}
	if logger := m.cfg.Logger; logger != nil {
		logger.Info(ctx, "walsummary migrated legacy transform logs",
			mlog.String("pchannel", m.cfg.PChannel),
			mlog.Uint64("generation", generation),
			mlog.Int("vchannels", len(records)),
			mlog.Int("records", recordCount))
	}
	return nil
}

// validateSortedTransformEntries rejects unsorted or duplicate timeticks.
func validateSortedTransformEntries(entries []*streamingpb.TransformLogEntry) error {
	previous := uint64(0)
	for _, entry := range entries {
		if entry == nil {
			return storeCorruptedf("legacy transform entry is nil")
		}
		if entry.GetTimeTick() <= previous {
			return storeCorruptedf("legacy transform entries are not strictly ordered")
		}
		previous = entry.GetTimeTick()
	}
	return nil
}

// sortTransformEntries returns a copy of entries sorted by timetick, keeping
// equal timeticks adjacent in their original relative order.
func sortTransformEntries(entries []*streamingpb.TransformLogEntry) []*streamingpb.TransformLogEntry {
	sorted := make([]*streamingpb.TransformLogEntry, len(entries))
	copy(sorted, entries)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetTimeTick() < sorted[j].GetTimeTick()
	})
	return sorted
}
