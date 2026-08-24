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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// probeLimit bounds a single forward probe. A term that wrote more than this
// many chunks past its manifest is pathological; failing loudly beats scanning
// object storage without end.
const probeLimit = 1 << 16

// Recover rebuilds the in-memory state from the durable store, before WAL
// replay. It must be called once before any view observes messages.
//
// The sequence is fixed, and every step exists to close a specific way data
// could otherwise be lost:
//
//  1. read this term's manifest (missing is fine: a term that never wrote one);
//  2. probe chunks forward from the manifest's newest generation — this
//     recovers everything written after the last manifest publish (the crash
//     window between chunk write and manifest write);
//  3. on a term handoff, inherit the previous term's index — chunks the
//     previous owner published (handles released, the WAL checkpoint may have
//     passed them) but never materialized must stay visible, or those delete
//     records are lost forever;
//  4. publish this term's manifest, sealing the inherited and probed sets
//     into it — without this the tail is invisible to the NEXT recovery and
//     is lost silently;
//  5. only now may this owner write chunks (generations start past the
//     inherited set).
//
// The catalog meta is the fencing marker of the store: it records which term
// last owned the summary. A term older than the recorded one must not touch
// the store (a newer owner is already writing). The check is best-effort —
// the object-level arbitration on the chunk keys is the authoritative fence —
// and the meta is written whenever this term differs from the recorded one.
func (m *Manager) Recover(ctx context.Context) error {
	if m.cfg.MetaCatalog != nil {
		meta, err := m.cfg.MetaCatalog.GetPChannelSummaryMeta(ctx, m.cfg.PChannel)
		if err != nil {
			return err
		}
		if meta != nil && m.cfg.Term < meta.GetTerm() {
			return merr.WrapErrServiceInternalMsg(
				"walsummary of pchannel %s is fenced: catalog term %d is newer than term %d",
				m.cfg.PChannel, meta.GetTerm(), m.cfg.Term,
			)
		}
		if meta == nil || m.cfg.Term != meta.GetTerm() {
			if err := m.cfg.MetaCatalog.SavePChannelSummaryMeta(ctx, m.cfg.PChannel, &streamingpb.PChannelSummaryMeta{
				Pchannel: m.cfg.PChannel,
				Term:     m.cfg.Term,
			}); err != nil {
				return err
			}
		}
	}
	// Recover this term's own manifest first; a term that never wrote one
	// (fresh owner after a term handoff) inherits the previous term's chunks
	// below. Every step below closes a specific way data could otherwise be
	// lost:
	//
	//  1. read this term's manifest (missing is fine: a term that never wrote one);
	//  2. probe chunks forward from the manifest's newest generation — this
	//     recovers everything written after the last manifest publish (the crash
	//     window between chunk write and manifest write);
	//  3. on a term handoff, inherit the previous term's manifest and probed
	//     tail: chunks the previous owner published (handles released, the WAL
	//     checkpoint may have passed them) but that were not yet materialized
	//     must stay visible to this term, or the records are lost forever;
	//  4. publish this term's manifest, sealing the inherited and probed sets
	//     into it — without this the tail is invisible to the NEXT recovery
	//     and is lost silently;
	//  5. only now may this owner write chunks (generations start past the
	//     inherited set).
	manifest, needsPublish, err := m.recoverManifestOfTerm(ctx, m.cfg.Term)
	if err != nil {
		return err
	}
	if !needsPublish && m.cfg.Term > 0 {
		// Term handoff: this term has no chunks of its own yet. Adopt the
		// previous term's index wholesale so its un-materialized records stay
		// reachable, then seal the union into this term's manifest.
		previous, previousNeedsPublish, err := m.recoverManifestOfTerm(ctx, m.cfg.Term-1)
		if err != nil {
			return err
		}
		if previousNeedsPublish {
			manifest = previous
			needsPublish = true
		}
	}
	// Publish this term's manifest whenever it now records anything (its own
	// chunks, a probed tail, or an inherited previous-term index): the seal
	// keeps the whole set visible to the NEXT recovery and makes the inherited
	// set durable before any new chunk is written.
	if needsPublish {
		if err := m.cfg.Store.WriteManifest(ctx, manifest); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.manifest = manifest
	if latest, ok := manifestNewest(manifest); ok {
		m.nextGeneration = latest.GetGeneration() + 1
		m.latestCoveredTimeTick = latest.GetEndTimetick()
	} else {
		m.nextGeneration = 0
	}
	m.mu.Unlock()
	if logger := m.cfg.Logger; logger != nil {
		logger.Info(ctx, "walsummary recovered",
			mlog.String("pchannel", m.cfg.PChannel),
			mlog.Int64("term", m.cfg.Term),
			mlog.Int("chunks", len(manifest.GetChunks())),
			mlog.Uint64("nextGeneration", m.nextGeneration))
	}
	return nil
}

// recoverManifestOfTerm reads one term's manifest and probes the chunk tail
// written past its last manifest publish, returning the sealed union and
// whether the term records anything at all.
func (m *Manager) recoverManifestOfTerm(ctx context.Context, term int64) (*streamingpb.PChannelSummaryManifest, bool, error) {
	previous, found, err := m.cfg.Store.ReadManifestOfTerm(ctx, term)
	if err != nil {
		return nil, false, err
	}
	manifest := inheritManifest(previous, nil)
	var fromGeneration uint64
	if latest, ok := manifestNewest(manifest); ok {
		fromGeneration = latest.GetGeneration() + 1
	}
	discovered, err := m.cfg.Store.ProbeChunkForwardOfTerm(ctx, term, fromGeneration)
	if err != nil {
		return nil, false, err
	}
	if len(discovered) > probeLimit {
		return nil, false, storeCorruptedf("summary store of %s has %d unrecorded chunks beyond generation %d",
			m.cfg.PChannel, len(discovered), fromGeneration)
	}
	if len(discovered) > 0 {
		manifest = inheritManifest(manifest, discovered)
		found = true
	}
	if !found {
		return &streamingpb.PChannelSummaryManifest{}, false, nil
	}
	return manifest, true, nil
}

// manifestNewest returns the newest chunk the manifest records.
func manifestNewest(manifest *streamingpb.PChannelSummaryManifest) (*streamingpb.PChannelSummaryChunkIndexEntry, bool) {
	chunks := manifest.GetChunks()
	if len(chunks) == 0 {
		return nil, false
	}
	return chunks[len(chunks)-1], true
}

// Manifest returns a snapshot of the current manifest.
func (m *Manager) Manifest() *streamingpb.PChannelSummaryManifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.manifest
}
