package vchannel

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/segment"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type PChannelManagerConfig struct {
	PChannel string

	VChannelMetas             map[string]*streamingpb.VChannelMeta
	Segments                  map[int64]*streamingpb.SegmentAssignmentMeta
	SegmentDataVersionSummary map[string]*streamingpb.SegmentDataVersionSummary
	TransformLogMetas         map[string]*streamingpb.VChannelTransformLogMeta

	Runtime                   moduleapi.Runtime
	Logger                    *mlog.Logger
	SegmentLifecycle          segment.Lifecycle
	SegmentPackWriter         segment.PackWriter
	TransformLogStore         waltransformlog.Store
	TransformLogMaterializer  waltransformlog.Materializer
	TransformLogMaxRows       uint64
	TransformLogMaxBytes      uint64
	TransformLogMaterialRows  uint64
	TransformLogMaterialBytes uint64
	OnSegmentSealed           func(walview.SegmentSealedEvent)
}

// PChannelRecoveryManager owns all vchannel recovery modules on one pchannel.
type PChannelRecoveryManager struct {
	mu       sync.Mutex
	pchannel string
	modules  map[string]*VChannelRecoveryModule
	dirty    map[string]struct{}

	config        PChannelManagerConfig
	metaAndData   bool
	streamManager wal.TransformLogStreamManager

	streamMu     sync.Mutex
	streamNotify chan struct{}
	streamSeq    uint64
	streamSeqByV map[string]uint64
}

func NewPChannelRecoveryManager(config PChannelManagerConfig) (*PChannelRecoveryManager, error) {
	if config.PChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("pchannel recovery manager pchannel is empty")
	}
	manager := &PChannelRecoveryManager{
		pchannel:     config.PChannel,
		modules:      make(map[string]*VChannelRecoveryModule),
		dirty:        make(map[string]struct{}),
		config:       config,
		streamNotify: make(chan struct{}),
		streamSeqByV: make(map[string]uint64),
	}
	manager.streamManager = waltransformlog.NewProviderStreamManager(manager)
	for _, vchannel := range manager.initialVChannels(config) {
		module, err := manager.newModuleLocked(vchannel)
		if err != nil {
			return nil, err
		}
		manager.modules[vchannel] = module
	}
	return manager, nil
}

func (m *PChannelRecoveryManager) initialVChannels(config PChannelManagerConfig) []string {
	index := make(map[string]struct{})
	for vchannel := range config.VChannelMetas {
		index[vchannel] = struct{}{}
	}
	for _, meta := range config.Segments {
		if meta.GetVchannel() != "" {
			index[meta.GetVchannel()] = struct{}{}
		}
	}
	for vchannel := range config.SegmentDataVersionSummary {
		index[vchannel] = struct{}{}
	}
	for vchannel := range config.TransformLogMetas {
		index[vchannel] = struct{}{}
	}
	vchannels := make([]string, 0, len(index))
	for vchannel := range index {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)
	return vchannels
}

func (m *PChannelRecoveryManager) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (m *PChannelRecoveryManager) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if m == nil || msg == nil {
		return moduleapi.ObserveResult{}
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	if m.shouldBroadcast(msg) {
		return m.observeBroadcastMessage(ctx, msg)
	}
	module := m.moduleForMessage(msg)
	if module == nil {
		return moduleapi.ObserveResult{}
	}
	result := module.ObserveMessage(ctx, msg)
	m.markDirtyIfNeeded(module.vchannel, result)
	return result
}

func (m *PChannelRecoveryManager) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	m.metaAndData = true
	modules := m.snapshotModulesLocked()
	m.mu.Unlock()

	snapshots := make(moduleapi.CompositeModuleSnapshot, 0, len(modules)*3)
	for _, module := range modules {
		snapshots = append(snapshots, moduleapi.FlattenModuleSnapshot(module.SwitchIntoMetaAndData())...)
	}
	return snapshots
}

func (m *PChannelRecoveryManager) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m == nil {
		return nil
	}
	modules := m.snapshotDirtyModules()
	snapshots := make([]moduleapi.DirtySnapshot, 0)
	stillDirty := make(map[string]struct{})
	for vchannel, module := range modules {
		moduleSnapshots := module.ConsumeDirtySnapshots()
		if len(moduleSnapshots) == 0 {
			continue
		}
		stillDirty[vchannel] = struct{}{}
		snapshots = append(snapshots, moduleSnapshots...)
	}
	m.mu.Lock()
	for vchannel := range m.dirty {
		if _, ok := modules[vchannel]; !ok {
			stillDirty[vchannel] = struct{}{}
		}
	}
	m.dirty = stillDirty
	m.mu.Unlock()
	return snapshots
}

func (m *PChannelRecoveryManager) Recover(ctx context.Context) error {
	for _, module := range m.snapshotModules() {
		if err := module.Recover(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *PChannelRecoveryManager) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	if m == nil {
		return nil
	}
	if scope.VChannel != "" && (scope.Type == moduleapi.ScopeVChannel || scope.Type == moduleapi.ScopePartition) {
		if module := m.Module(scope.VChannel); module != nil {
			return module.DataFrontier(scope)
		}
		return nil
	}
	barriers := make([]walcheckpoint.Barrier, 0)
	for _, module := range m.snapshotModules() {
		if barrier := module.DataFrontier(scope); barrier != nil {
			barriers = append(barriers, barrier)
		}
	}
	return walcheckpoint.NewCompositeBarrier(barriers...)
}

func (m *PChannelRecoveryManager) BuildWALView(
	ctx context.Context,
	vchannel string,
	baseSelector ResourceRecoveryBaseSelector,
	loadConfigProvider RecoveredLoadConfigProvider,
) (walview.VChannelWALView, bool) {
	module := m.Module(vchannel)
	if module == nil {
		return walview.VChannelWALView{}, false
	}
	return module.BuildWALView(ctx, baseSelector, loadConfigProvider)
}

func (m *PChannelRecoveryManager) Module(vchannel string) *VChannelRecoveryModule {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.modules[vchannel]
}

func (m *PChannelRecoveryManager) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	return m.streamManager.AcquireStream(ctx, pchannel)
}

func (m *PChannelRecoveryManager) Notify(vchannel string) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	m.streamSeq++
	m.streamSeqByV[vchannel] = m.streamSeq
	close(m.streamNotify)
	m.streamNotify = make(chan struct{})
}

func (m *PChannelRecoveryManager) TransformLogForStream(vchannel string) waltransformlog.TransformLog {
	module := m.Module(vchannel)
	if module == nil {
		return nil
	}
	return module.transformLog
}

func (m *PChannelRecoveryManager) ValidatePChannel(pchannel string) error {
	if pchannel == "" {
		return errors.Wrap(wal.ErrTransformLogInvalidReadOption, "pchannel is empty")
	}
	if m.pchannel != "" && m.pchannel != pchannel {
		return errors.Wrapf(wal.ErrTransformLogInvalidReadOption, "pchannel mismatch, expected %s, got %s", m.pchannel, pchannel)
	}
	return nil
}

func (m *PChannelRecoveryManager) StreamNotifyStateSince(seq uint64) (<-chan struct{}, uint64, []string) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	changed := make([]string, 0)
	for vchannel, vchannelSeq := range m.streamSeqByV {
		if vchannelSeq > seq {
			changed = append(changed, vchannel)
		}
	}
	sort.Strings(changed)
	return m.streamNotify, m.streamSeq, changed
}

func (m *PChannelRecoveryManager) shouldBroadcast(msg message.ImmutableMessage) bool {
	return msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *PChannelRecoveryManager) observeBroadcastMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	results := make([]moduleapi.ObserveResult, 0)
	for _, module := range m.snapshotModules() {
		result := module.ObserveMessage(ctx, msg)
		m.markDirtyIfNeeded(module.vchannel, result)
		results = append(results, result)
	}
	return moduleapi.ComposeBarriers(results)
}

func (m *PChannelRecoveryManager) moduleForMessage(msg message.ImmutableMessage) *VChannelRecoveryModule {
	vchannel := msg.VChannel()
	if vchannel == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	module := m.modules[vchannel]
	if module != nil || msg.MessageType() != message.MessageTypeCreateCollection {
		return module
	}
	module, err := m.newModuleLocked(vchannel)
	if err != nil {
		return nil
	}
	if m.metaAndData {
		module.SwitchIntoMetaAndData()
	}
	m.modules[vchannel] = module
	return module
}

func (m *PChannelRecoveryManager) newModuleLocked(vchannel string) (*VChannelRecoveryModule, error) {
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	for id, meta := range m.config.Segments {
		if meta.GetVchannel() == vchannel {
			segments[id] = meta
		}
	}
	module, err := NewModule(ModuleConfig{
		PChannel:                  m.pchannel,
		VChannel:                  vchannel,
		VChannelMeta:              m.config.VChannelMetas[vchannel],
		Segments:                  segments,
		SegmentDataVersionSummary: m.config.SegmentDataVersionSummary[vchannel],
		TransformLogMeta:          m.config.TransformLogMetas[vchannel],
		Runtime:                   m.config.Runtime,
		Logger:                    m.config.Logger,
		SegmentLifecycle:          m.config.SegmentLifecycle,
		SegmentPackWriter:         m.config.SegmentPackWriter,
		TransformLogStore:         m.config.TransformLogStore,
		TransformLogMaterializer:  m.config.TransformLogMaterializer,
		TransformLogMaxRows:       m.config.TransformLogMaxRows,
		TransformLogMaxBytes:      m.config.TransformLogMaxBytes,
		TransformLogMaterialRows:  m.config.TransformLogMaterialRows,
		TransformLogMaterialBytes: m.config.TransformLogMaterialBytes,
		OnSegmentSealed:           m.config.OnSegmentSealed,
	})
	if err != nil {
		return nil, err
	}
	module.transformStream = m
	module.onDirty = func() {
		m.markDirty(vchannel)
	}
	return module, nil
}

func (m *PChannelRecoveryManager) snapshotModules() []*VChannelRecoveryModule {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotModulesLocked()
}

func (m *PChannelRecoveryManager) snapshotModulesLocked() []*VChannelRecoveryModule {
	modules := make([]*VChannelRecoveryModule, 0, len(m.modules))
	vchannels := make([]string, 0, len(m.modules))
	for vchannel := range m.modules {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)
	for _, vchannel := range vchannels {
		modules = append(modules, m.modules[vchannel])
	}
	return modules
}

func (m *PChannelRecoveryManager) snapshotDirtyModules() map[string]*VChannelRecoveryModule {
	m.mu.Lock()
	defer m.mu.Unlock()
	modules := make(map[string]*VChannelRecoveryModule, len(m.dirty))
	for vchannel := range m.dirty {
		if module := m.modules[vchannel]; module != nil {
			modules[vchannel] = module
		}
	}
	return modules
}

func (m *PChannelRecoveryManager) markDirtyIfNeeded(vchannel string, result moduleapi.ObserveResult) {
	if result.Meta == nil && result.Data == nil {
		return
	}
	m.markDirty(vchannel)
}

func (m *PChannelRecoveryManager) markDirty(vchannel string) {
	if vchannel == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty[vchannel] = struct{}{}
}

var _ moduleapi.Module = (*PChannelRecoveryManager)(nil)
var _ moduleapi.DataFrontierProvider = (*PChannelRecoveryManager)(nil)
var _ wal.TransformLogStreamManager = (*PChannelRecoveryManager)(nil)
var _ waltransformlog.StreamManager = (*PChannelRecoveryManager)(nil)
var _ waltransformlog.StreamProvider = (*PChannelRecoveryManager)(nil)
