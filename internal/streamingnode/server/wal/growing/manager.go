package growing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type Manager struct {
	vchannelViews map[string]*VChannelView
	segmentViews  map[int64]*SegmentView
	lifecycle     SegmentLifecycle
	packWriter    PackWriter
	onDataUpdated func()
	channelName   string
	catalog       RecoveryCatalog
	logger        *log.MLogger
	runtime       moduleapi.Runtime
	metaAndData   bool
	transformRows uint64

	lastPersistTask scheduler.TaskHandle
	lastCleanupTask scheduler.TaskHandle
}

type ManagerOption func(*Manager)

type runtimeConfig struct {
	lifecycle     SegmentLifecycle
	packWriter    PackWriter
	runtime       moduleapi.Runtime
	onDataUpdated func()
	flushPolicy   flushPolicy
	metaAndData   bool
	transformRows uint64
}

func firstRuntimeConfig(configs []runtimeConfig) runtimeConfig {
	if len(configs) == 0 {
		return runtimeConfig{}
	}
	return configs[0]
}

func WithPackWriter(writer PackWriter) ManagerOption {
	return func(manager *Manager) {
		manager.packWriter = writer
	}
}

func WithDataBarrierUpdatedCallback(callback func()) ManagerOption {
	return func(manager *Manager) {
		manager.onDataUpdated = callback
	}
}

func WithRecoveryCatalog(channelName string, catalog RecoveryCatalog) ManagerOption {
	return func(manager *Manager) {
		manager.channelName = channelName
		manager.catalog = catalog
	}
}

func WithModuleRuntime(logger *log.MLogger, runtime moduleapi.Runtime) ManagerOption {
	return func(manager *Manager) {
		manager.logger = logger
		manager.runtime = runtime
	}
}

func WithTransformLogBufferMaxRows(maxRows uint64) ManagerOption {
	return func(manager *Manager) {
		manager.transformRows = maxRows
	}
}

func NewManager(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	lifecycle SegmentLifecycle,
	opts ...ManagerOption,
) *Manager {
	if vchannels == nil {
		vchannels = make(map[string]*streamingpb.VChannelMeta)
	}
	if segments == nil {
		segments = make(map[int64]*streamingpb.SegmentAssignmentMeta)
	}
	manager := &Manager{
		vchannelViews: make(map[string]*VChannelView, len(vchannels)),
		segmentViews:  make(map[int64]*SegmentView, len(segments)),
		lifecycle:     lifecycle,
	}
	for _, opt := range opts {
		opt(manager)
	}
	manager.initializeRuntimeInfos(vchannels, segments)
	return manager
}

func (m *Manager) runtimeConfig() runtimeConfig {
	return runtimeConfig{
		lifecycle:     m.lifecycle,
		packWriter:    m.packWriter,
		runtime:       m.runtime,
		onDataUpdated: m.onDataUpdated,
		metaAndData:   m.metaAndData,
		transformRows: m.transformRows,
	}
}

func (m *Manager) initializeRuntimeInfos(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) {
	for vchannel, meta := range vchannels {
		m.vchannelViews[vchannel] = NewVChannelView(meta, m.runtimeConfig())
	}
	for _, meta := range segments {
		vchannelManager := m.vchannelViews[meta.GetVchannel()]
		segment := NewSegmentViewFromMeta(meta, segmentSchema(vchannelManager, meta), m.runtimeConfig())
		m.addSegmentView(segment)
	}
}

func segmentSchema(vchannel *VChannelView, meta *streamingpb.SegmentAssignmentMeta) *schemapb.CollectionSchema {
	if vchannel == nil {
		return nil
	}
	timetick := meta.GetStat().GetCreateSegmentTimeTick()
	if timetick == 0 {
		return nil
	}
	_, schema := vchannel.GetSchema(timetick)
	return schema
}

func (m *Manager) VChannel(vchannel string) *VChannelView {
	info := m.vchannelViews[vchannel]
	if info == nil || !info.IsActive() {
		return nil
	}
	return info
}

func (m *Manager) retainedVChannel(vchannel string) *VChannelView {
	return m.vchannelViews[vchannel]
}

func (m *Manager) VChannels() map[string]*VChannelView {
	return m.vchannelViews
}

func (m *Manager) Segments() map[int64]*SegmentView {
	return m.segmentViews
}

func (m *Manager) AddVChannel(meta *streamingpb.VChannelMeta) *VChannelView {
	info := newVChannelView(meta, 0, 0, true, m.runtimeConfig())
	m.vchannelViews[info.AssignmentMeta().GetVchannel()] = info
	m.attachRetainedSegments(info)
	return info
}

func (m *Manager) addSegmentView(segment *SegmentView) *SegmentView {
	segmentMeta := segment.AssignmentMeta()
	m.segmentViews[segmentMeta.GetSegmentId()] = segment
	vchannelManager := m.retainedVChannel(segmentMeta.GetVchannel())
	if vchannelManager == nil {
		return segment
	}
	vchannelMeta := vchannelManager.AssignmentMeta()
	if segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
		return segment
	}
	vchannelManager.addSegment(segment)
	return segment
}

func (m *Manager) attachRetainedSegments(vchannelManager *VChannelView) {
	vchannelMeta := vchannelManager.AssignmentMeta()
	for _, segment := range m.segmentViews {
		segmentMeta := segment.AssignmentMeta()
		if segmentMeta.GetVchannel() != vchannelMeta.GetVchannel() ||
			segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
			continue
		}
		segment.SetSchema(segmentSchema(vchannelManager, segmentMeta))
		vchannelManager.addSegment(segment)
	}
}

func (m *Manager) refreshRetainedSegmentSchemas(vchannelManager *VChannelView) {
	vchannelMeta := vchannelManager.AssignmentMeta()
	for _, segment := range m.segmentViews {
		segmentMeta := segment.AssignmentMeta()
		if segmentMeta.GetVchannel() != vchannelMeta.GetVchannel() ||
			segmentMeta.GetCollectionId() != vchannelMeta.GetCollectionInfo().GetCollectionId() {
			continue
		}
		segment.SetSchema(segmentSchema(vchannelManager, segmentMeta))
	}
}
