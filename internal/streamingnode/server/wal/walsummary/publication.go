package walsummary

import (
	"maps"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// PublishedManifest is the metadata-only durable baseline. The writer's working
// manifest can contain completed chunks whose manifest PUT has not finished.
func (m *Manager) PublishedManifest() *streamingpb.PChannelSummaryManifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return proto.Clone(m.publishedManifest).(*streamingpb.PChannelSummaryManifest)
}

// ApplyStoragePublished installs a manifest after its durable publication. It
// performs no object I/O. RO keeps its protected history until its own runtime
// drains; a primary's GC frontier cannot invalidate a later lazy read on an old
// local QueryView. The object-retention budget bounds that runtime's lifetime.
func (m *Manager) ApplyStoragePublished(manifest *streamingpb.PChannelSummaryManifest) error {
	if err := validateManifest(manifest); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.terminalErr != nil {
		return m.terminalErr
	}
	if manifest.GetCoverage().GetEndTimeTick() < m.publishedManifest.GetCoverage().GetEndTimeTick() {
		return storeCorruptedf("published summary coverage regressed")
	}
	snapshot := proto.Clone(manifest).(*streamingpb.PChannelSummaryManifest)
	if m.cfg.ReadOnly {
		m.installReadOnlyManifestLocked(snapshot)
	}
	m.publishedManifest = snapshot
	m.manifestPublished = true
	m.refreshLastAckedLocked()
	return nil
}

func (m *Manager) installReadOnlyManifestLocked(manifest *streamingpb.PChannelSummaryManifest) {
	previous := make(map[ChunkRef]*indexedChunk, len(m.chunkIndex.chunks))
	for _, chunk := range m.chunkIndex.chunks {
		previous[ChunkRef{Term: chunk.GetTerm(), Generation: chunk.GetGeneration()}] = chunk
	}
	chunks := make([]*indexedChunk, 0, len(manifest.Chunks))
	for _, index := range manifest.Chunks {
		ref := ChunkRef{Term: index.GetTerm(), Generation: index.GetGeneration()}
		chunk := previous[ref]
		if chunk == nil {
			chunk = m.chunkIndex.newChunk(index, nil)
		}
		delete(previous, ref)
		chunks = append(chunks, chunk)
		for _, vc := range index.GetVchannels() {
			m.durableFrontiers[vc.GetVchannel()] = max(m.durableFrontiers[vc.GetVchannel()], vc.GetEndTimetick())
		}
	}
	for _, chunk := range previous {
		chunks = append(chunks, chunk)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].GetGeneration() < chunks[j].GetGeneration() })
	m.chunkIndex.chunks = chunks
	// Keep the authoritative publication distinct from the local readable
	// index. Both old and new objects were acquired before applying the event.
	local := proto.CloneOf(manifest)
	local.Chunks = make([]*streamingpb.PChannelSummaryChunkIndexEntry, 0, len(chunks))
	for _, chunk := range chunks {
		local.Chunks = append(local.Chunks, chunk.PChannelSummaryChunkIndexEntry)
	}
	if coverage := m.manifest.GetCoverage(); coverage != nil && local.Coverage != nil {
		local.Coverage.StartTimeTick = min(local.Coverage.StartTimeTick, coverage.StartTimeTick)
	}
	local.TransformFastForwardTimeTick = maps.Clone(m.manifest.GetTransformFastForwardTimeTick())
	m.manifest = local
	m.latestCoveredTimeTick = manifest.GetCoverage().GetEndTimeTick()
	m.restoredTimeTick = m.latestCoveredTimeTick
	// RO never seals chunks. Drop only records replaced by published objects;
	// observations ahead of the publisher remain available without replay.
	pending := m.pending[:0]
	m.pendingBytes = 0
	m.pendingTransforms = make(map[string]*streamingpb.VChannelSummaryTransformIndex)
	for _, record := range m.pending {
		if record.timeTick <= m.latestCoveredTimeTick {
			continue
		}
		pending = append(pending, record)
		m.pendingBytes += record.size
		if record.entry != nil {
			addTransformSize(m.pendingTransforms, record.vchannel, record.entry)
		}
	}
	clear(m.pending[len(pending):])
	m.pending = pending
	m.advanceReadableLocked(m.latestCoveredTimeTick)
	m.notifyReadersLocked()
	m.notifyAllTransformsLocked()
}
