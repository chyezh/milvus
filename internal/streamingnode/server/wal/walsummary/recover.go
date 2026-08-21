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
//  3. publish this term's manifest, inheriting the previous one and sealing the
//     probed tail into it — without this the tail is invisible to the NEXT
//     recovery and is lost silently;
//  4. only now may this owner write chunks (generations start past the
//     inherited set).
func (m *Manager) Recover(ctx context.Context) error {
	previous, found, err := m.cfg.Store.ReadManifest(ctx)
	if err != nil {
		return err
	}
	manifest := inheritManifest(previous, nil)
	var fromGeneration uint64
	if latest, ok := manifestNewest(manifest); ok {
		fromGeneration = latest.GetGeneration() + 1
	}
	discovered, err := m.cfg.Store.ProbeChunkForward(ctx, fromGeneration)
	if err != nil {
		return err
	}
	if len(discovered) > probeLimit {
		return storeCorruptedf("summary store of %s has %d unrecorded chunks beyond generation %d",
			m.cfg.PChannel, len(discovered), fromGeneration)
	}
	manifest = inheritManifest(manifest, discovered)
	// Publish the sealed manifest even when nothing was discovered: the first
	// owner has no previous manifest to inherit, and the publish makes the
	// inherited set durable before any new chunk is written.
	if !found || len(discovered) > 0 {
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
