package coordview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// pendingSyncQueryViews tracks query views dispatched to a single work node
// that are still waiting for responses. Owned by a single resumableSyncer.
// Thread-safe.
type pendingSyncQueryViews struct {
	mu      sync.Mutex
	entries map[qviews.QueryViewKey]SyncView
	unsent  []*viewpb.QueryViewOfShard // protos accumulated by Upsert, drained by sendLoop
}

func newPendingSyncQueryViews() *pendingSyncQueryViews {
	return &pendingSyncQueryViews{
		entries: make(map[qviews.QueryViewKey]SyncView),
	}
}

// Upsert inserts or replaces a pending entry and accumulates the proto
// for incremental sending via DrainUnsent.
func (p *pendingSyncQueryViews) Upsert(sv SyncView) {
	key := sv.View.QueryViewKey()

	p.mu.Lock()
	p.entries[key] = sv
	p.unsent = append(p.unsent, sv.View.IntoProto())
	p.mu.Unlock()
}

// DrainUnsent atomically drains and returns protos accumulated by Upsert.
// Used by sendLoop for incremental sends.
func (p *pendingSyncQueryViews) DrainUnsent() []*viewpb.QueryViewOfShard {
	p.mu.Lock()
	protos := p.unsent
	p.unsent = nil
	p.mu.Unlock()
	return protos
}

// MatchResponse matches a received response proto to pending entries
// and invokes the callback. If callback returns true, the entry is removed.
//
// The callback is invoked without holding the lock (it is concurrent-safe).
func (p *pendingSyncQueryViews) MatchResponse(pb *viewpb.QueryViewOfShard) {
	view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
	key := view.QueryViewKey()

	p.mu.Lock()
	entry, ok := p.entries[key]
	p.mu.Unlock()
	if !ok {
		return
	}

	if entry.Callback(view) {
		p.mu.Lock()
		delete(p.entries, key)
		p.mu.Unlock()
	}
}

// Drain removes all pending entries, invokes OnNodeLost() for each,
// and delivers the result via Callback.
// Called when the node is declared lost or the resumableSyncer is closed.
func (p *pendingSyncQueryViews) Drain() {
	p.mu.Lock()
	drained := make([]SyncView, 0, len(p.entries))
	for _, sv := range p.entries {
		drained = append(drained, sv)
	}
	p.entries = make(map[qviews.QueryViewKey]SyncView)
	p.unsent = nil
	p.mu.Unlock()

	for _, entry := range drained {
		resp := entry.OnNodeLost()
		entry.Callback(resp)
	}
}

// CollectProtos returns the protos of all pending entries.
// Used by resumableSyncer to re-push on stream reconnection.
func (p *pendingSyncQueryViews) CollectProtos() []*viewpb.QueryViewOfShard {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.entries) == 0 {
		return nil
	}

	protos := make([]*viewpb.QueryViewOfShard, 0, len(p.entries))
	for _, sv := range p.entries {
		protos = append(protos, sv.View.IntoProto())
	}
	return protos
}
