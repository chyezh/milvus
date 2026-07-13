package vchannel

import (
	"context"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/segment"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResourceRecoveryBaseSelector optionally selects the base DataVersion used to
// build the visible growing segment snapshot for this vchannel.
type ResourceRecoveryBaseSelector func(vchannel string) (qviews.DataVersion, bool)

// RecoveredLoadConfigProvider supplies load config recovered from QueryView
// state when the vchannel meta no longer owns a load-config anchor.
type RecoveredLoadConfigProvider func(vchannel string) *streamingpb.VChannelLoadConfig

// ModuleConfig contains the initial state and dependencies for one vchannel
// recovery module.
type ModuleConfig struct {
	PChannel string
	VChannel string

	VChannelMeta              *streamingpb.VChannelMeta
	Segments                  map[int64]*streamingpb.SegmentAssignmentMeta
	SegmentDataVersionSummary *streamingpb.SegmentDataVersionSummary
	TransformLogMeta          *streamingpb.VChannelTransformLogMeta

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

// VChannelRecoveryModule owns all recovery_storage state for one vchannel.
type VChannelRecoveryModule struct {
	pchannel string
	vchannel string

	runtime moduleapi.Runtime
	logger  *mlog.Logger

	vchannelView *VChannelView
	segments     map[int64]*segment.SegmentView

	segmentDataVersionSummary *streamingpb.SegmentDataVersionSummary
	latestInsertTimeTick      uint64

	transformLog     waltransformlog.TransformLog
	transformStream  waltransformlog.StreamManager
	flushTasks       []scheduler.TaskHandle
	materializeTasks []scheduler.TaskHandle

	segmentLifecycle  segment.Lifecycle
	segmentPackWriter segment.PackWriter
	onSegmentSealed   func(walview.SegmentSealedEvent)
	onDirty           func()

	metaAndData bool
}

// NewModule creates a single-vchannel recovery module.
func NewModule(config ModuleConfig) (*VChannelRecoveryModule, error) {
	if config.PChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("vchannel recovery module pchannel is empty")
	}
	if config.VChannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("vchannel recovery module vchannel is empty")
	}
	module := &VChannelRecoveryModule{
		pchannel:                  config.PChannel,
		vchannel:                  config.VChannel,
		runtime:                   config.Runtime,
		logger:                    config.Logger,
		segments:                  make(map[int64]*segment.SegmentView),
		segmentDataVersionSummary: cloneSegmentDataVersionSummary(config.SegmentDataVersionSummary),
		segmentLifecycle:          config.SegmentLifecycle,
		segmentPackWriter:         config.SegmentPackWriter,
		onSegmentSealed:           config.OnSegmentSealed,
	}
	if config.VChannelMeta != nil {
		module.vchannelView = NewVChannelViewFromMeta(config.VChannelMeta)
	}
	for id, meta := range config.Segments {
		if meta.GetVchannel() != config.VChannel {
			continue
		}
		var schema *schemapb.CollectionSchema
		if module.vchannelView != nil {
			schema = module.vchannelView.CreateSegmentSchema(meta.GetPartitionId(), meta.GetStat().GetCreateSegmentTimeTick())
		}
		module.segments[id] = segment.NewSegmentViewFromMetaWithOptions(meta, schema, module.segmentViewOptions(config)...)
	}
	module.transformLog = waltransformlog.New(waltransformlog.Config{
		VChannel:            config.VChannel,
		MaxRows:             config.TransformLogMaxRows,
		MaterializeMaxRows:  config.TransformLogMaterialRows,
		MaterializeMaxBytes: config.TransformLogMaterialBytes,
		Meta:                config.TransformLogMeta,
		Store:               config.TransformLogStore,
		Materializer:        config.TransformLogMaterializer,
	})
	module.transformStream = waltransformlog.NewStreamManager(config.PChannel, config.VChannel, module.transformLog)
	return module, nil
}

func (m *VChannelRecoveryModule) segmentViewOptions(config ModuleConfig) []segment.ViewOption {
	return []segment.ViewOption{
		segment.WithViewRuntime(config.Runtime),
		segment.WithViewLifecycle(config.SegmentLifecycle),
		segment.WithViewPackWriter(config.SegmentPackWriter),
		segment.WithViewSegmentSealedNotifier(config.OnSegmentSealed),
		segment.WithViewDataUpdatedNotifier(func() {
			m.markDirty()
			if m.runtime.Notifier != nil {
				m.runtime.Notifier.NotifyBarrierUpdated()
			}
		}),
	}
}

func (m *VChannelRecoveryModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameVChannel
}

func (m *VChannelRecoveryModule) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if m == nil || msg == nil || !m.shouldObserve(msg) {
		return moduleapi.ObserveResult{}
	}
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		return moduleapi.ObserveResult{}
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		return m.handleCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		return m.handleCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		return m.handleSchemaChangeMessage(message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		return m.handleAlterCollectionMessage(ctx, message.MustAsImmutableAlterCollectionMessageV2(msg))
	case message.MessageTypeDropCollection:
		return m.handleDropCollectionMessage(ctx, message.MustAsImmutableDropCollectionMessageV1(msg))
	case message.MessageTypeDropPartition:
		return m.handleDropPartitionMessage(ctx, message.MustAsImmutableDropPartitionMessageV1(msg))
	case message.MessageTypeTruncateCollection:
		return m.handleTruncateCollectionMessage(ctx, message.MustAsImmutableTruncateCollectionMessageV2(msg))
	case message.MessageTypeAlterLoadConfig:
		return m.handleAlterLoadConfigMessage(message.MustAsImmutableAlterLoadConfigMessageV2(msg))
	case message.MessageTypeDropLoadConfig:
		return m.handleDropLoadConfigMessage(message.MustAsImmutableDropLoadConfigMessageV2(msg))
	case message.MessageTypeCreateSegment:
		return m.handleCreateSegmentMessage(ctx, message.MustAsImmutableCreateSegmentMessageV2(msg))
	case message.MessageTypeInsert:
		return m.handleInsertMessage(ctx, message.MustAsImmutableInsertMessageV1(msg))
	case message.MessageTypeTxn:
		return m.handleTxnMessage(ctx, message.AsImmutableTxnMessage(msg))
	case message.MessageTypeFlush:
		return m.handleFlushMessage(ctx, message.MustAsImmutableFlushMessageV2(msg))
	case message.MessageTypeManualFlush:
		return m.handleManualFlushMessage(ctx, msg)
	case message.MessageTypeFlushAll:
		return m.handleFlushAllMessage(ctx, msg)
	case message.MessageTypeAlterWAL:
		return m.handleAlterWALMessage(ctx, msg)
	case message.MessageTypeDelete:
		return m.appendTransformLogMessage(msg)
	case message.MessageTypeRecoveryBarrier:
		return m.handleRecoveryBarrierMessage(msg)
	default:
		return moduleapi.ObserveResult{}
	}
}

func (m *VChannelRecoveryModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	if m == nil {
		return nil
	}
	m.metaAndData = true
	snapshots := make(moduleapi.CompositeModuleSnapshot, 0, 3)
	if m.vchannelView != nil {
		m.vchannelView.SwitchIntoMetaAndData()
		if m.vchannelView.IsActive() {
			snapshots = append(snapshots, &moduleapi.VChannelModuleSnapshot{
				VChannels: map[string]*streamingpb.VChannelMeta{m.vchannel: m.vchannelView.AssignmentMeta()},
			})
		}
	}
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	for id, view := range m.segments {
		view.SwitchIntoMetaAndData()
		if view.IsGrowing() {
			segments[id] = view.AssignmentMeta()
		}
	}
	if len(segments) > 0 || m.segmentDataVersionSummary != nil {
		snapshots = append(snapshots, &moduleapi.SegmentModuleSnapshot{
			Segments:             segments,
			DataVersionSummaries: m.segmentDataVersionSummaries(),
		})
	}
	if m.transformLog != nil {
		snapshots = append(snapshots, &moduleapi.TransformLogModuleSnapshot{
			TransformLogs: map[string]*streamingpb.VChannelTransformLogMeta{m.vchannel: m.transformLog.SnapshotMeta()},
		})
	}
	return snapshots
}

func (m *VChannelRecoveryModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m == nil {
		return nil
	}
	snapshots := make([]moduleapi.DirtySnapshot, 0)
	if m.vchannelView != nil {
		if meta := m.vchannelView.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := m.vchannelView
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameVChannel,
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
				moduleapi.SnapshotOpUpsert,
				snapshot,
				snapshot.GetCheckpointTimeTick(),
				0,
				func() { owner.MarkSnapshotPersisted(snapshot) },
			))
		}
	}
	for id, view := range m.segments {
		if meta := view.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := view
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameSegment,
				moduleapi.SnapshotKey{PChannel: m.pchannel, SegmentID: id},
				moduleapi.SnapshotOpUpsert,
				snapshot,
				snapshot.GetCheckpointTimeTick(),
				snapshot.GetDataCheckpointTimeTick(),
				func() { owner.MarkSnapshotPersisted(snapshot) },
			))
		}
	}
	if m.transformLog != nil {
		if meta := m.transformLog.ConsumeDirtyAndGetSnapshot(); meta != nil {
			owner := m.transformLog
			snapshot := meta
			snapshots = append(snapshots, newDirtySnapshot(
				moduleapi.ModuleNameTransformLog,
				moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: m.vchannel},
				moduleapi.SnapshotOpUpsert,
				snapshot,
				snapshot.GetCheckpointTimeTick(),
				snapshot.GetCheckpointTimeTick(),
				func() { owner.MarkSnapshotPersisted(snapshot) },
			))
		}
	}
	return snapshots
}

func (m *VChannelRecoveryModule) NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64) {
	// Snapshot-level persisted callbacks are owned by each DirtySnapshot.
}

func (m *VChannelRecoveryModule) Recover(ctx context.Context) error {
	if m == nil || m.transformLog == nil {
		return nil
	}
	_, err := m.transformLog.Recover(ctx, nil)
	return err
}

func (m *VChannelRecoveryModule) TransformLog() wal.TransformLogAccesser {
	if m == nil {
		return wal.NewTransformLogErrorAccesser(wal.ErrTransformLogVChannelUnavailable)
	}
	return m.transformStream
}

func (m *VChannelRecoveryModule) AcquireStream(ctx context.Context, pchannel string) (wal.TransformLogStream, error) {
	return m.TransformLog().AcquireStream(ctx, pchannel)
}

func (m *VChannelRecoveryModule) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	if m == nil || !m.matchesScope(scope) {
		return nil
	}
	return walcheckpoint.NewCompositeBarrier(
		walcheckpoint.BarrierFunc(func() uint64 { return m.segmentFrontierTimeTick(scope) }),
		walcheckpoint.BarrierFunc(func() uint64 { return m.transformFrontierTimeTick(scope.Kind) }),
	)
}

func (m *VChannelRecoveryModule) BuildWALView(
	ctx context.Context,
	baseSelector ResourceRecoveryBaseSelector,
	loadConfigProvider RecoveredLoadConfigProvider,
) (walview.VChannelWALView, bool) {
	if m == nil || m.vchannelView == nil || m.transformLog == nil {
		return walview.VChannelWALView{}, false
	}
	vchannelSnapshot, ok := m.vchannelView.WALViewSnapshot()
	if !ok {
		return walview.VChannelWALView{}, false
	}
	loadConfig := vchannelSnapshot.LoadConfig
	if loadConfig == nil && loadConfigProvider != nil {
		loadConfig = loadConfigProvider(m.vchannel)
	}
	if loadConfig == nil {
		return walview.VChannelWALView{}, false
	}
	baseTransformTimeTick := m.transformLog.LatestTimeTick()
	baseGrowingTimeTick := max(m.latestInsertTimeTick, baseTransformTimeTick)
	segmentSnapshot := m.visibleSnapshot(baseGrowingTimeTick, m.segmentSnapshotDataVersion())
	if baseSelector != nil {
		if base, ok := baseSelector(m.vchannel); ok {
			segmentSnapshot = m.visibleSnapshot(baseGrowingTimeTick, base)
		}
	}
	deleteReplay := newDeleteReplayScanner(
		ctx,
		m.transformStream,
		m.pchannel,
		m.vchannel,
		deleteReplayStartAfter(segmentSnapshot),
		baseTransformTimeTick,
	)
	return walview.VChannelWALView{
		PChannel:              m.pchannel,
		VChannel:              m.vchannel,
		CollectionID:          vchannelSnapshot.CollectionID,
		BaseGrowingTimeTick:   baseGrowingTimeTick,
		BaseTransformTimeTick: baseTransformTimeTick,
		LoadConfig:            loadConfig,
		Schema:                vchannelSnapshot.Schema,
		SegmentSnapshot:       segmentSnapshot,
		DeleteReplay:          deleteReplay,
	}, true
}

func (m *VChannelRecoveryModule) handleCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) moduleapi.ObserveResult {
	vchannelResult := moduleapi.ObserveResult{}
	if m.vchannelView == nil {
		m.vchannelView = NewVChannelViewFromCreateCollectionMessage(msg)
		if m.metaAndData {
			m.vchannelView.SwitchIntoMetaAndData()
		}
		vchannelResult.Meta = m.vchannelView.MetaBarrier()
	} else {
		replacement, result := m.vchannelView.ObserveCreateCollectionMessageV1(msg)
		if replacement != nil {
			m.vchannelView = replacement
			vchannelResult.Meta = m.vchannelView.MetaBarrier()
		} else {
			vchannelResult = result
		}
	}
	return composeObserveResults(vchannelResult, m.appendTransformLogMessage(msg))
}

func (m *VChannelRecoveryModule) handleCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveCreatePartitionMessageV1(msg)
}

func (m *VChannelRecoveryModule) handleSchemaChangeMessage(msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveSchemaChangeMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleAlterCollectionMessage(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveAlterCollectionMessageV2(msg))
	}
	if messageutil.IsSchemaChange(msg.Header()) {
		result = composeObserveResults(result, m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true }))
		result = composeObserveResults(result, m.flushTransformLogByTimeTick(msg.TimeTick()))
	}
	return result
}

func (m *VChannelRecoveryModule) handleDropCollectionMessage(ctx context.Context, msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveDropCollectionMessageV1(msg))
	}
	result = composeObserveResults(result, m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true }))
	result = composeObserveResults(result, m.materializeTransformLogByTimeTick(msg.TimeTick()))
	return result
}

func (m *VChannelRecoveryModule) handleDropPartitionMessage(ctx context.Context, msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveDropPartitionMessageV1(msg))
	}
	result = composeObserveResults(result, m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(view *segment.SegmentView) bool {
		return view.PartitionID() == msg.Header().GetPartitionId()
	}))
	result = composeObserveResults(result, m.flushTransformLogByTimeTick(msg.TimeTick()))
	return result
}

func (m *VChannelRecoveryModule) handleTruncateCollectionMessage(ctx context.Context, msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	if m.vchannelView != nil {
		result = composeObserveResults(result, m.vchannelView.ObserveTruncateCollectionMessageV2(msg))
	}
	result = composeObserveResults(result, m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true }))
	result = composeObserveResults(result, m.flushTransformLogByTimeTick(msg.TimeTick()))
	return result
}

func (m *VChannelRecoveryModule) handleAlterLoadConfigMessage(msg message.ImmutableAlterLoadConfigMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveAlterLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleDropLoadConfigMessage(msg message.ImmutableDropLoadConfigMessageV2) moduleapi.ObserveResult {
	if m.vchannelView == nil {
		return moduleapi.ObserveResult{}
	}
	return m.vchannelView.ObserveDropLoadConfigMessageV2(msg)
}

func (m *VChannelRecoveryModule) handleCreateSegmentMessage(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	id := msg.Header().GetSegmentId()
	view := m.segments[id]
	result := moduleapi.ObserveResult{}
	if view == nil {
		var schema *schemapb.CollectionSchema
		if m.vchannelView != nil {
			schema = m.vchannelView.CreateSegmentSchema(msg.Header().GetPartitionId(), msg.TimeTick())
		}
		if schema == nil {
			return result
		}
		view = segment.NewSegmentViewFromCreateSegmentMessageWithOptions(msg, schema, m.segmentOptions()...)
		m.segments[id] = view
		result.Meta = view.MetaBarrier()
	}
	return composeObserveResults(result, view.ObserveCreateSegmentMessageV2(ctx, msg))
}

func (m *VChannelRecoveryModule) handleInsertMessage(ctx context.Context, msg message.ImmutableInsertMessageV1) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, partition := range msg.Header().GetPartitions() {
		view := m.segments[partition.GetSegmentAssignment().GetSegmentId()]
		if view == nil {
			continue
		}
		result = composeObserveResults(result, view.ObserveInsertMessageV1(ctx, msg, partition))
	}
	m.markLatestInsertTimeTick(msg.VChannel(), msg.TimeTick(), result)
	return result
}

func (m *VChannelRecoveryModule) handleTxnMessage(ctx context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	if msg == nil {
		return moduleapi.ObserveResult{}
	}
	result := moduleapi.ObserveResult{}
	observed := make(map[int64]struct{})
	_ = msg.RangeOver(func(inner message.ImmutableMessage) error {
		if inner.MessageType() != message.MessageTypeInsert {
			return nil
		}
		insert := message.MustAsImmutableInsertMessageV1(inner)
		for _, partition := range insert.Header().GetPartitions() {
			id := partition.GetSegmentAssignment().GetSegmentId()
			if _, ok := observed[id]; ok {
				continue
			}
			view := m.segments[id]
			if view == nil {
				continue
			}
			observed[id] = struct{}{}
			result = composeObserveResults(result, view.ObserveTxnMessage(ctx, msg))
		}
		return nil
	})
	m.markLatestInsertTimeTick(msg.VChannel(), msg.TimeTick(), result)
	return composeObserveResults(result, m.appendTransformLogMessage(msg))
}

func (m *VChannelRecoveryModule) handleFlushMessage(ctx context.Context, msg message.ImmutableFlushMessageV2) moduleapi.ObserveResult {
	if segment := m.segments[msg.Header().GetSegmentId()]; segment != nil {
		return segment.Flush(ctx, msg.TimeTick())
	}
	return moduleapi.ObserveResult{}
}

func (m *VChannelRecoveryModule) handleManualFlushMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	result := m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true })
	return composeObserveResults(result, m.materializeTransformLogByTimeTick(msg.TimeTick()))
}

func (m *VChannelRecoveryModule) handleFlushAllMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	result := m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true })
	return composeObserveResults(result, m.materializeTransformLogByTimeTick(msg.TimeTick()))
}

func (m *VChannelRecoveryModule) handleAlterWALMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	result := m.flushSegmentsCreatedBefore(ctx, msg.TimeTick(), func(*segment.SegmentView) bool { return true })
	return composeObserveResults(result, m.flushTransformLogByTimeTick(msg.TimeTick()))
}

func (m *VChannelRecoveryModule) handleRecoveryBarrierMessage(msg message.ImmutableMessage) moduleapi.ObserveResult {
	if !m.metaAndData {
		return moduleapi.ObserveResult{}
	}
	return m.flushTransformLogByTimeTick(msg.TimeTick())
}

func (m *VChannelRecoveryModule) flushSegmentsCreatedBefore(ctx context.Context, timetick uint64, match func(*segment.SegmentView) bool) moduleapi.ObserveResult {
	result := moduleapi.ObserveResult{}
	for _, view := range m.segments {
		if !match(view) || view.CreateTimeTick() >= timetick {
			continue
		}
		result = composeObserveResults(result, view.Flush(ctx, timetick))
	}
	return result
}

func (m *VChannelRecoveryModule) appendTransformLogMessage(msg message.ImmutableMessage) moduleapi.ObserveResult {
	if m.transformLog == nil || msg.VChannel() == "" || !m.metaAndData {
		return moduleapi.ObserveResult{}
	}
	if msg.TimeTick() <= m.transformLog.DataCheckpointTimeTick() {
		return moduleapi.ObserveResult{}
	}
	result := m.transformLog.Append(msg, waltransformlog.AppendOption{})
	if !result.Appended {
		return moduleapi.ObserveResult{}
	}
	m.transformStream.Notify(m.vchannel)
	if result.ShouldFlush || isTransformBarrierMessage(msg) {
		m.submitTransformFlushTask(result.DataTimeTick)
	}
	return moduleapi.ObserveResult{Data: m.transformDataBarrier()}
}

func (m *VChannelRecoveryModule) flushTransformLogByTimeTick(timetick uint64) moduleapi.ObserveResult {
	if !m.metaAndData || timetick <= m.transformLog.DataCheckpointTimeTick() {
		return moduleapi.ObserveResult{}
	}
	if !m.transformLog.AppendBarrier(timetick).Appended && !m.transformLog.HasPendingWork() {
		return moduleapi.ObserveResult{}
	}
	m.transformStream.Notify(m.vchannel)
	m.submitTransformFlushTask(timetick)
	return moduleapi.ObserveResult{Data: m.transformDataBarrier()}
}

func (m *VChannelRecoveryModule) materializeTransformLogByTimeTick(timetick uint64) moduleapi.ObserveResult {
	result := m.flushTransformLogByTimeTick(timetick)
	if result.Data != nil {
		m.submitTransformMaterializeTask(timetick)
	}
	return result
}

func (m *VChannelRecoveryModule) submitTransformFlushTask(timetick uint64) {
	if m.runtime.Scheduler == nil {
		return
	}
	task := m.newTransformFlushTask(timetick)
	handle := m.runtime.Scheduler.Submit(task)
	m.flushTasks = append(m.flushTasks, handle)
}

func (m *VChannelRecoveryModule) submitTransformMaterializeTask(timetick uint64) {
	if m.runtime.Scheduler == nil {
		return
	}
	task := m.newTransformMaterializeTask(timetick)
	handle := m.runtime.Scheduler.Submit(task)
	m.materializeTasks = append(m.materializeTasks, handle)
}

func (m *VChannelRecoveryModule) segmentOptions() []segment.ViewOption {
	return []segment.ViewOption{
		segment.WithViewRuntime(m.runtime),
		segment.WithViewLifecycle(m.segmentLifecycle),
		segment.WithViewPackWriter(m.segmentPackWriter),
		segment.WithViewSegmentSealedNotifier(m.onSegmentSealed),
		segment.WithViewDataUpdatedNotifier(func() {
			m.markDirty()
			if m.runtime.Notifier != nil {
				m.runtime.Notifier.NotifyBarrierUpdated()
			}
		}),
	}
}

func (m *VChannelRecoveryModule) markDirty() {
	if m.onDirty != nil {
		m.onDirty()
	}
}

func (m *VChannelRecoveryModule) shouldObserve(msg message.ImmutableMessage) bool {
	return msg.VChannel() == m.vchannel || msg.VChannel() == "" || msg.IsPChannelLevel()
}

func (m *VChannelRecoveryModule) matchesScope(scope moduleapi.Scope) bool {
	switch scope.Type {
	case moduleapi.ScopeAll:
		return true
	case moduleapi.ScopeVChannel, moduleapi.ScopePartition:
		return scope.VChannel == "" || scope.VChannel == m.vchannel
	default:
		return false
	}
}

func (m *VChannelRecoveryModule) segmentDataVersionSummaries() map[string]*streamingpb.SegmentDataVersionSummary {
	if m.segmentDataVersionSummary == nil {
		return nil
	}
	return map[string]*streamingpb.SegmentDataVersionSummary{
		m.vchannel: cloneSegmentDataVersionSummary(m.segmentDataVersionSummary),
	}
}

func (m *VChannelRecoveryModule) visibleSnapshot(baseGrowingTimeTick uint64, dataVersion qviews.DataVersion) walview.VisibleSegmentSnapshot {
	snapshot := walview.VisibleSegmentSnapshot{
		VChannel:            m.vchannel,
		DataVersion:         dataVersion,
		BaseGrowingTimeTick: baseGrowingTimeTick,
	}
	for _, view := range m.segments {
		visible, ok := view.VisibleSnapshot(m.vchannel, dataVersion)
		if !ok {
			continue
		}
		if snapshot.CollectionID == 0 {
			snapshot.CollectionID = visible.Assignment.GetCollectionId()
		}
		snapshot.Segments = append(snapshot.Segments, visible)
	}
	return snapshot
}

func (m *VChannelRecoveryModule) segmentSnapshotDataVersion() qviews.DataVersion {
	dataVersion := segmentDataVersionSummary(m.segmentDataVersionSummary)
	for _, view := range m.segments {
		sealedVersion, ok := view.SealedDataVersion(m.vchannel)
		if ok && sealedVersion.GT(dataVersion) {
			dataVersion = sealedVersion
		}
	}
	return dataVersion
}

func (m *VChannelRecoveryModule) markLatestInsertTimeTick(vchannel string, timetick uint64, result moduleapi.ObserveResult) {
	if vchannel != m.vchannel || (result.Meta == nil && result.Data == nil) {
		return
	}
	if timetick > m.latestInsertTimeTick {
		m.latestInsertTimeTick = timetick
	}
}

func (m *VChannelRecoveryModule) segmentFrontierTimeTick(scope moduleapi.Scope) uint64 {
	frontier := uint64(math.MaxUint64)
	for _, view := range m.segments {
		if !view.MatchesScope(scope) {
			continue
		}
		if timetick := view.DurableFrontierTimeTick(); timetick < frontier {
			frontier = timetick
		}
	}
	return frontier
}

func (m *VChannelRecoveryModule) transformFrontierTimeTick(kind moduleapi.DataProgressKind) uint64 {
	if kind == moduleapi.DataProgressMaterialized {
		if m.transformLog.HasDirty() || m.hasPendingTransformMaterializeTask() {
			return m.transformLog.MaterializedBarrierTimeTick()
		}
		return math.MaxUint64
	}
	if m.transformLog.HasDirty() || m.transformLog.HasPendingWork() || m.hasPendingTransformFlushTask() {
		return m.transformLog.DataBarrierTimeTick()
	}
	return math.MaxUint64
}

func (m *VChannelRecoveryModule) transformDataBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(m.transformLog.DataBarrierTimeTick)
}

func (m *VChannelRecoveryModule) hasPendingTransformFlushTask() bool {
	m.flushTasks = compactPendingTasks(m.flushTasks)
	return len(m.flushTasks) > 0
}

func (m *VChannelRecoveryModule) hasPendingTransformMaterializeTask() bool {
	m.materializeTasks = compactPendingTasks(m.materializeTasks)
	return len(m.materializeTasks) > 0
}

func compactPendingTasks(tasks []scheduler.TaskHandle) []scheduler.TaskHandle {
	pending := tasks[:0]
	for _, task := range tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
	}
	return pending
}

func cloneSegmentDataVersionSummary(summary *streamingpb.SegmentDataVersionSummary) *streamingpb.SegmentDataVersionSummary {
	if summary == nil {
		return nil
	}
	return proto.Clone(summary).(*streamingpb.SegmentDataVersionSummary)
}

func segmentDataVersionSummary(summary *streamingpb.SegmentDataVersionSummary) qviews.DataVersion {
	if summary == nil || summary.GetDataVersion() == nil {
		return qviews.DataVersion{}
	}
	return qviews.FromProtoDataVersion(summary.GetDataVersion())
}

func deleteReplayStartAfter(snapshot walview.VisibleSegmentSnapshot) uint64 {
	if len(snapshot.Segments) == 0 {
		return 0
	}
	minCreateTimeTick := uint64(0)
	for _, segment := range snapshot.Segments {
		createTimeTick := segment.Assignment.GetStat().GetCreateSegmentTimeTick()
		if createTimeTick == 0 {
			continue
		}
		if minCreateTimeTick == 0 || createTimeTick < minCreateTimeTick {
			minCreateTimeTick = createTimeTick
		}
	}
	if minCreateTimeTick == 0 {
		return 0
	}
	return minCreateTimeTick - 1
}

func composeObserveResults(left moduleapi.ObserveResult, right moduleapi.ObserveResult) moduleapi.ObserveResult {
	return moduleapi.ComposeBarriers([]moduleapi.ObserveResult{left, right})
}

func isTransformBarrierMessage(msg message.ImmutableMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection,
		message.MessageTypeRecoveryBarrier,
		message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeFlushAll,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeAlterWAL:
		return true
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		return messageutil.IsSchemaChange(alter.Header())
	default:
		return false
	}
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

var _ moduleapi.Module = (*VChannelRecoveryModule)(nil)
var _ moduleapi.CheckpointPersistedObserver = (*VChannelRecoveryModule)(nil)
var _ moduleapi.DataFrontierProvider = (*VChannelRecoveryModule)(nil)
var _ wal.TransformLogStreamManager = (*VChannelRecoveryModule)(nil)
