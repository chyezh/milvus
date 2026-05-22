package viewresource

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ snview.StreamingNodeResourceManager = (*Manager)(nil)

// Manager adapts the vchannel registry to snview.StreamingNodeResourceManager.
type Manager struct {
	registry Registry

	mu            sync.Mutex
	released      map[qviews.QueryViewKey]struct{}
	releaseNotify chan struct{}
}

func NewManager(registry Registry) *Manager {
	return &Manager{
		registry:      registry,
		released:      make(map[qviews.QueryViewKey]struct{}),
		releaseNotify: make(chan struct{}, 1),
	}
}

func (m *Manager) Acquire(req snview.AcquireResource) {
	go m.waitRuntimeLoop(req.Key, req.Meta, req.OnReady, req.OnUnrecoverable)
}

func (m *Manager) Recover(req snview.RecoverResource) {
	go m.waitRuntimeLoop(req.Key, req.Meta, req.OnRecoveringDone, req.OnUnrecoverable)
}

func (m *Manager) Release(req snview.ReleaseResource) {
	go func() {
		m.mu.Lock()
		m.released[req.Key] = struct{}{}
		m.mu.Unlock()
		m.notifyRelease()

		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
}

func (m *Manager) UpdateMinDataVersion(req snview.UpdateMinDataVersionResource) {
	m.registry.EvictBefore(req.CollectionID, req.VChannel, req.MinDataVersion)
}

func (m *Manager) ReleaseLoad(req snview.ReleaseLoadResource) {
	m.registry.ReleaseLoad(req.CollectionID, req.VChannel)
}

func (m *Manager) waitRuntimeLoop(
	key qviews.QueryViewKey,
	meta *viewpb.QueryViewMeta,
	onReady func(),
	onUnrecoverable func(),
) {
	if meta == nil || meta.GetVersion() == nil || meta.GetVersion().GetDataVersion() == nil {
		if onUnrecoverable != nil {
			onUnrecoverable()
		}
		return
	}

	desc := ViewResourceDescriptor{
		CollectionID:                  meta.GetCollectionId(),
		ReplicaID:                     meta.GetReplicaId(),
		VChannel:                      meta.GetVchannel(),
		Version:                       qviews.FromProtoQueryViewVersion(meta.GetVersion()),
		Settings:                      meta.GetSettings(),
		DeleteApplyStartAfterTimeTick: meta.GetDeleteApplyStartAfterTimetick(),
	}

	for {
		if m.isReleased(key) {
			if onUnrecoverable != nil {
				onUnrecoverable()
			}
			return
		}
		_, ready, err := m.registry.GetViewRuntime(desc)
		if err != nil {
			if onUnrecoverable != nil {
				onUnrecoverable()
			}
			return
		}
		if ready {
			if m.isReleased(key) {
				return
			}

			if onReady != nil {
				onReady()
			}
			return
		}
		select {
		case <-m.registry.NotifyReady():
		case <-m.releaseNotify:
		}
	}
}

func (m *Manager) isReleased(key qviews.QueryViewKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, released := m.released[key]
	return released
}

func (m *Manager) notifyRelease() {
	select {
	case m.releaseNotify <- struct{}{}:
	default:
	}
}
