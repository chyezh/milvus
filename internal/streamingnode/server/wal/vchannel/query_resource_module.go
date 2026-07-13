package vchannel

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func defaultQueryRuntimeModuleBuilders(builders []QueryRuntimeModuleBuilder) []QueryRuntimeModuleBuilder {
	if len(builders) == 0 {
		return []QueryRuntimeModuleBuilder{NewGrowingRuntimeModuleBuilder(nil)}
	}
	return append([]QueryRuntimeModuleBuilder(nil), builders...)
}

func (m *VChannelRecoveryModule) AcquireQueryResource(req snview.AcquireResource) {
	epoch := m.registerQueryResourceRef(req)
	go m.waitQueryRuntimeReady(req.Key, epoch, req.OnReady)
}

func (m *VChannelRecoveryModule) ReleaseQueryResource(req snview.ReleaseResource) {
	var runtime *QueryRuntime
	var task BuildTask
	var advanceRuntime *QueryRuntime
	var advance qviews.DataVersion
	var hasAdvance bool

	m.mu.Lock()
	if _, ok := m.queryRefs[req.Key]; ok {
		delete(m.queryRefs, req.Key)
		m.notifyQueryStateChangedLocked()
		advance, hasAdvance = minQueryViewDataVersion(m.queryRefs)
		advanceRuntime = m.queryRuntime
	}
	if len(m.queryRefs) == 0 {
		runtime, task = m.takeQueryRuntimeLocked()
	}
	m.mu.Unlock()

	if hasAdvance && advanceRuntime != nil {
		advanceRuntime.Advance(advance)
	}
	go func() {
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
	cancelTask(task)
	closeRuntime(runtime)
}

func (m *VChannelRecoveryModule) QueryRuntime(key qviews.QueryViewKey) (*QueryRuntime, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queryRefs[key]; !ok {
		return nil, false
	}
	if m.queryTask != nil || m.queryRuntime == nil || m.queryErr != nil {
		return nil, false
	}
	return m.queryRuntime, true
}

func (m *VChannelRecoveryModule) CloseQueryResources() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.queryClosed = true
	m.notifyQueryStateChangedLocked()
	runtime, task := m.takeQueryRuntimeLocked()
	m.queryRefs = make(map[qviews.QueryViewKey]struct{})
	m.queryEpoch = make(map[qviews.QueryViewKey]uint64)
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
}

func (m *VChannelRecoveryModule) registerQueryResourceRef(req snview.AcquireResource) uint64 {
	if req.Meta == nil || req.Meta.GetVersion() == nil || req.Meta.GetVersion().GetDataVersion() == nil {
		panic("query view meta version is nil")
	}
	if req.Meta.GetVchannel() != m.vchannel {
		panic("query view vchannel does not match recovery module")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryClosed {
		panic("vchannel query resource is closed")
	}
	if _, ok := m.queryRefs[req.Key]; !ok {
		m.assertQueryAcquireMonotonicLocked(req.Key.QueryViewVersion.DataVersion)
		m.queryRefs[req.Key] = struct{}{}
		m.queryEpoch[req.Key]++
		if m.queryRuntime == nil && m.queryTask == nil {
			m.startQueryRuntimeBuildLocked(req.Meta)
		}
		m.notifyQueryStateChangedLocked()
	}
	return m.queryEpoch[req.Key]
}

func (m *VChannelRecoveryModule) assertQueryAcquireMonotonicLocked(version qviews.DataVersion) {
	for key := range m.queryRefs {
		if key.QueryViewVersion.DataVersion.GT(version) {
			panic("non-monotonic query view acquire")
		}
	}
}

func (m *VChannelRecoveryModule) startQueryRuntimeBuildLocked(meta *viewpb.QueryViewMeta) {
	if m.queryScheduler == nil {
		m.queryScheduler = NewScheduler(4)
	}
	if m.queryDispatcher == nil {
		m.queryDispatcher = newQueryRuntimeDispatcher(defaultLiveEventDispatchConcurrency)
	}
	runtime := newQueryRuntime(m.queryDispatcher, m.newQueryRuntimeModules()...)
	view, ok := m.queryWALViewLocked(meta)
	if !ok {
		panic("failed to build vchannel query resource view")
	}
	task := newResourceBuildTask(context.Background(), func(ctx context.Context) (*QueryRuntime, error) {
		if err := runtime.Initialize(ctx, view); err != nil {
			return runtime, err
		}
		return runtime, nil
	})
	m.queryRuntime = runtime
	m.queryTask = task
	m.queryErr = nil
	m.queryScheduler.Submit(task)
	go m.finishQueryRuntimeBuild(task)
}

func (m *VChannelRecoveryModule) newQueryRuntimeModules() []QueryRuntimeModule {
	modules := make([]QueryRuntimeModule, 0, len(m.queryRuntimeModuleBuilders))
	for _, builder := range m.queryRuntimeModuleBuilders {
		if builder == nil {
			continue
		}
		module, err := builder.NewRuntime()
		if err != nil {
			panic(errors.Wrap(err, "create query runtime module"))
		}
		if module != nil {
			modules = append(modules, module)
		}
	}
	return modules
}

func (m *VChannelRecoveryModule) finishQueryRuntimeBuild(task BuildTask) {
	runtime, err := task.Result()
	m.mu.Lock()
	if m.queryTask != task {
		m.mu.Unlock()
		closeRuntime(runtime)
		return
	}
	m.queryTask = nil
	if err != nil {
		if errors.Is(err, context.Canceled) {
			m.queryErr = err
		} else {
			m.mu.Unlock()
			panic(errors.Wrap(err, "initialize query runtime"))
		}
	} else {
		m.queryRuntime = runtime
		m.queryErr = nil
	}
	m.notifyQueryStateChangedLocked()
	if len(m.queryRefs) == 0 {
		runtime, task = m.takeQueryRuntimeLocked()
	} else {
		runtime, task = nil, nil
	}
	m.mu.Unlock()

	cancelTask(task)
	closeRuntime(runtime)
}

func (m *VChannelRecoveryModule) waitQueryRuntimeReady(key qviews.QueryViewKey, epoch uint64, onReady func()) {
	for {
		runtime, task, changed, ok := m.queryRuntimeForRef(key, epoch)
		if !ok {
			return
		}
		if changed != nil {
			<-changed
			continue
		}
		if task != nil {
			<-task.Done()
			_, err := task.Result()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				panic(errors.Wrap(err, "wait query runtime initialization"))
			}
			continue
		}
		advance, ok := m.oldestQueryDataVersionForRef(key, epoch)
		if !ok {
			return
		}
		runtime.Advance(advance)
		if onReady != nil && m.hasQueryResourceRef(key, epoch) {
			onReady()
		}
		return
	}
}

func (m *VChannelRecoveryModule) queryRuntimeForRef(key qviews.QueryViewKey, epoch uint64) (*QueryRuntime, BuildTask, <-chan struct{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryEpoch[key] != epoch {
		return nil, nil, nil, false
	}
	if _, ok := m.queryRefs[key]; !ok {
		return nil, nil, nil, false
	}
	if m.queryErr != nil && !errors.Is(m.queryErr, context.Canceled) {
		panic(errors.Wrap(m.queryErr, "query runtime initialization failed"))
	}
	if m.queryRuntime == nil && m.queryTask == nil {
		return nil, nil, m.queryChanged, true
	}
	return m.queryRuntime, m.queryTask, nil, true
}

func (m *VChannelRecoveryModule) oldestQueryDataVersionForRef(key qviews.QueryViewKey, epoch uint64) (qviews.DataVersion, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryEpoch[key] != epoch {
		return qviews.DataVersion{}, false
	}
	if _, ok := m.queryRefs[key]; !ok {
		return qviews.DataVersion{}, false
	}
	return minQueryViewDataVersion(m.queryRefs)
}

func (m *VChannelRecoveryModule) hasQueryResourceRef(key qviews.QueryViewKey, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryEpoch[key] != epoch {
		return false
	}
	_, ok := m.queryRefs[key]
	return ok
}

func (m *VChannelRecoveryModule) takeQueryRuntimeLocked() (*QueryRuntime, BuildTask) {
	runtime, task := m.queryRuntime, m.queryTask
	m.queryRuntime = nil
	m.queryTask = nil
	m.queryErr = nil
	return runtime, task
}

func (m *VChannelRecoveryModule) notifyQueryStateChangedLocked() {
	close(m.queryChanged)
	m.queryChanged = make(chan struct{})
}

func (m *VChannelRecoveryModule) observeQueryResourceMessage(ctx context.Context, msg message.ImmutableMessage) {
	m.observeQueryResourceEvent(ctx, walview.VChannelResourceEvent{Message: msg})
}

func (m *VChannelRecoveryModule) observeQueryResourceMessageLocked(ctx context.Context, msg message.ImmutableMessage) {
	m.observeQueryResourceEventLocked(ctx, walview.VChannelResourceEvent{Message: msg})
}

func (m *VChannelRecoveryModule) observeQueryResourceEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	m.mu.Lock()
	runtime := m.queryRuntime
	m.mu.Unlock()
	if runtime != nil {
		runtime.ObserveEvent(ctx, event)
	}
}

func (m *VChannelRecoveryModule) observeQueryResourceEventLocked(ctx context.Context, event walview.VChannelResourceEvent) {
	if m.queryRuntime != nil {
		m.queryRuntime.ObserveEvent(ctx, event)
	}
}

func (m *VChannelRecoveryModule) queryWALViewLocked(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
	if m == nil || m.vchannelView == nil || m.transformLog == nil || m.queryTransformLogStream == nil {
		return walview.VChannelWALView{}, false
	}
	vchannelSnapshot, ok := m.vchannelView.WALViewSnapshot()
	if !ok {
		return walview.VChannelWALView{}, false
	}
	baseTransformTimeTick := m.transformLog.LatestTimeTick()
	baseGrowingTimeTick := max(m.latestInsertTimeTick, baseTransformTimeTick)
	dataVersion := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
	segmentSnapshot := m.visibleSnapshot(baseGrowingTimeTick, dataVersion)
	deleteReplay := newDeleteReplayScanner(
		context.Background(),
		m.queryTransformLogStream,
		m.pchannel,
		m.vchannel,
		max(deleteReplayStartAfter(segmentSnapshot), meta.GetTransformStartAfterTimetick()),
		baseTransformTimeTick,
	)
	settings := cloneQueryViewSettings(meta.GetSettings())
	return walview.VChannelWALView{
		PChannel:              m.pchannel,
		VChannel:              m.vchannel,
		CollectionID:          vchannelSnapshot.CollectionID,
		BaseGrowingTimeTick:   baseGrowingTimeTick,
		BaseTransformTimeTick: baseTransformTimeTick,
		LoadConfig:            queryViewLoadConfig(meta, settings),
		Settings:              settings,
		Schema:                vchannelSnapshot.Schema,
		SegmentSnapshot:       segmentSnapshot,
		DeleteReplay:          deleteReplay,
	}, true
}

func cloneQueryViewSettings(settings *viewpb.QueryViewSettings) *viewpb.QueryViewSettings {
	if settings == nil {
		return &viewpb.QueryViewSettings{}
	}
	return proto.Clone(settings).(*viewpb.QueryViewSettings)
}

func queryViewLoadConfig(meta *viewpb.QueryViewMeta, settings *viewpb.QueryViewSettings) *streamingpb.VChannelLoadConfig {
	if settings == nil {
		settings = &viewpb.QueryViewSettings{}
	}
	loadFields := make([]*messagespb.LoadFieldConfig, 0, len(settings.GetRequiredFields()))
	for _, fieldID := range settings.GetRequiredFields() {
		loadFields = append(loadFields, &messagespb.LoadFieldConfig{FieldId: fieldID})
	}
	return &streamingpb.VChannelLoadConfig{
		Header: &messagespb.AlterLoadConfigMessageHeader{
			CollectionId: meta.GetCollectionId(),
			PartitionIds: append([]int64(nil), settings.GetRequiredPartitions()...),
			LoadFields:   loadFields,
		},
	}
}
