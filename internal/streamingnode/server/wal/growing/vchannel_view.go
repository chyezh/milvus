package growing

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

// NewVChannelView creates a VChannelView from persisted vchannel meta.
func NewVChannelView(meta *streamingpb.VChannelMeta, configs ...runtimeConfig) *VChannelView {
	return newVChannelView(
		meta,
		meta.GetCheckpointTimeTick(),
		meta.GetDataCheckpointTimeTick(),
		false,
		firstRuntimeConfig(configs),
	)
}

func newVChannelView(
	meta *streamingpb.VChannelMeta,
	persistedMetaTimeTick uint64,
	persistedDataTimeTick uint64,
	dirty bool,
	config runtimeConfig,
) *VChannelView {
	return &VChannelView{
		meta:                  meta,
		persistedMetaTimeTick: persistedMetaTimeTick,
		persistedDataTimeTick: persistedDataTimeTick,
		dirty:                 dirty,
		segments:              make(map[int64]*SegmentView),
		lifecycle:             config.lifecycle,
		packWriter:            config.packWriter,
		runtime:               config.runtime,
		onDataUpdated:         config.onDataUpdated,
		metaAndData:           config.metaAndData,
		transformLogBuffer:    newTransformLogBuffer(config.transformRows),
	}
}

// NewVChannelMetaFromCreateCollectionMessage creates a new vchannel meta from a create collection message.
func NewVChannelMetaFromCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) *streamingpb.VChannelMeta {
	partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(msg.Header().PartitionIds))
	for _, partitionId := range msg.Header().PartitionIds {
		partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{
			PartitionId: partitionId,
			State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
		})
	}
	body := msg.MustBody()
	schema := messageutil.MustGetSchemaFromCreateCollectionMessageBody(body)
	return &streamingpb.VChannelMeta{
		Vchannel: msg.VChannel(),
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: msg.Header().CollectionId,
			Partitions:   partitions,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             schema,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: msg.TimeTick(),
				},
			},
		},
		CheckpointTimeTick:     msg.TimeTick(),
		LatestDataVersion:      &viewpb.DataVersion{},
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		DataCheckpointTimeTick: 0,
	}
}

// VChannelView tracks the metadata and durability state of a vchannel.
type VChannelView struct {
	mu sync.Mutex

	meta                  *streamingpb.VChannelMeta
	persistedMetaTimeTick uint64
	persistedDataTimeTick uint64
	dirty                 bool // whether the vchannel recovery info is dirty.

	segments      map[int64]*SegmentView
	lifecycle     SegmentLifecycle
	packWriter    PackWriter
	runtime       moduleapi.Runtime
	onDataUpdated func()
	metaAndData   bool

	transformLogBuffer TransformLogBuffer
	transformLogTasks  []scheduler.TaskHandle
}

func (info *VChannelView) AssignmentMeta() *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *VChannelView) Name() string {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetVchannel()
}

func (info *VChannelView) HasDirty() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.dirty
}

func (info *VChannelView) MetaTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedMetaTimeTick
}

func (info *VChannelView) MetaBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(info.MetaTimeTick)
}

func (info *VChannelView) PersistedDataTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.persistedDataTimeTick
}

func (info *VChannelView) DurableFrontierTimeTick() uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
		if info.dirty {
			return frontierBefore(info.meta.GetTombstoneTimeTick())
		}
		return math.MaxUint64
	}
	if tombstoneTimeTick := info.dirtyPartitionTombstoneFrontierLocked(); tombstoneTimeTick > 0 {
		return frontierBefore(tombstoneTimeTick)
	}
	if !info.hasPendingDataWorkLocked() {
		return math.MaxUint64
	}
	return info.persistedDataTimeTick
}

func (info *VChannelView) hasPendingDataWorkLocked() bool {
	if info.meta.GetDataCheckpointTimeTick() > info.persistedDataTimeTick {
		return true
	}
	if info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED &&
		info.meta.GetDataCheckpointTimeTick() < info.meta.GetCheckpointTimeTick() {
		return true
	}
	if collectionInfo := info.meta.GetCollectionInfo(); collectionInfo != nil {
		for _, partition := range collectionInfo.GetPartitions() {
			if partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_DROPPED &&
				info.meta.GetDataCheckpointTimeTick() < partition.GetTombstoneTimeTick() {
				return true
			}
		}
	}
	if !info.transformLogBuffer.IsEmpty() ||
		info.transformLogBuffer.IsFlushing() ||
		info.transformLogBuffer.FlushTargetTimeTick() > info.persistedDataTimeTick {
		return true
	}
	for _, task := range info.transformLogTasks {
		if task != nil && !task.Done() {
			return true
		}
	}
	return false
}

func (info *VChannelView) DataTimeTick() uint64 {
	return info.persistedDataTimeTick
}

func (info *VChannelView) TimeTick() uint64 {
	return info.DataTimeTick()
}

func (info *VChannelView) MarkMetaPersisted(timetick uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(timetick)
}

func (info *VChannelView) markMetaPersistedLocked(timetick uint64) {
	if timetick > info.persistedMetaTimeTick {
		info.persistedMetaTimeTick = timetick
	}
}

func (info *VChannelView) MarkDataPersisted(timetick uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markDataPersistedLocked(timetick)
}

func (info *VChannelView) markDataPersistedLocked(timetick uint64) {
	if timetick > info.persistedDataTimeTick {
		info.persistedDataTimeTick = timetick
	}
}

func (info *VChannelView) MarkSnapshotPersisted(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markMetaPersistedLocked(snapshot.GetCheckpointTimeTick())
	info.markDataPersistedLocked(snapshot.GetDataCheckpointTimeTick())
	info.dirty = !proto.Equal(info.meta, snapshot)
}

func (info *VChannelView) MarkDataCheckpoint(timetick uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.markDataCheckpointLocked(timetick)
}

func (info *VChannelView) notifyDataUpdated() {
	if info.onDataUpdated != nil {
		info.onDataUpdated()
	}
}

func (info *VChannelView) markDataCheckpointLocked(timetick uint64) {
	if timetick <= info.meta.GetDataCheckpointTimeTick() {
		return
	}
	info.meta.DataCheckpointTimeTick = timetick
	info.dirty = true
}

func (info *VChannelView) TryFinalizeTombstone() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.maybeMarkTombstonedLocked()
}

func (info *VChannelView) hasReadyTombstoneFinalize() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.vchannelTombstoneFinalizeReadyLocked() {
		return true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if info.partitionTombstoneFinalizeReadyLocked(partition) {
			return true
		}
	}
	return false
}

func (info *VChannelView) Segment(segmentID int64) *SegmentView {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.segments[segmentID]
}

func (info *VChannelView) addSegment(segment *SegmentView) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.segments[segment.AssignmentMeta().SegmentId] = segment
}

func (info *VChannelView) removeSegment(segmentID int64) {
	info.mu.Lock()
	defer info.mu.Unlock()
	delete(info.segments, segmentID)
}

func (info *VChannelView) SwitchIntoMetaAndData() {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.metaAndData = true
}

func (info *VChannelView) ObserveDeleteMessageV1(_ context.Context, msg message.ImmutableDeleteMessageV1) moduleapi.ObserveResult {
	return info.ObserveDeleteMessagesV1([]message.ImmutableDeleteMessageV1{msg})
}

func (info *VChannelView) ObserveDeleteMessagesV1(messages []message.ImmutableDeleteMessageV1) moduleapi.ObserveResult {
	var task scheduler.Task
	info.mu.Lock()
	if !info.metaAndData {
		info.mu.Unlock()
		return emptyObserveResult()
	}
	pendingDataTimeTick := info.transformLogBuffer.DataTimeTick()
	appended := false
	for _, msg := range messages {
		if msg.TimeTick() <= info.meta.GetDataCheckpointTimeTick() || msg.TimeTick() <= pendingDataTimeTick {
			continue
		}
		info.transformLogBuffer.AppendDelete(msg)
		appended = true
	}
	if !appended {
		info.mu.Unlock()
		return emptyObserveResult()
	}
	if info.transformLogBuffer.ShouldFlush() {
		task = info.startFlushTransformLogBufferTaskLocked(info.transformLogBuffer.DataTimeTick())
	}
	runtime := info.runtime
	info.mu.Unlock()
	if task != nil {
		runtime.Scheduler.Submit(task)
	}
	return dataBarrierResult(info)
}

func (info *VChannelView) FlushTransformLogBuffer(timetick uint64) moduleapi.ObserveResult {
	var task scheduler.Task
	info.mu.Lock()
	if !info.metaAndData ||
		info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED ||
		(info.transformLogBuffer.IsEmpty() && timetick <= info.meta.GetDataCheckpointTimeTick()) {
		info.mu.Unlock()
		return emptyObserveResult()
	}
	task = info.startFlushTransformLogBufferTaskLocked(timetick)
	runtime := info.runtime
	info.mu.Unlock()
	if task != nil {
		runtime.Scheduler.Submit(task)
	}
	return dataBarrierResult(info)
}

func (info *VChannelView) startFlushTransformLogBufferTaskLocked(timetick uint64) scheduler.Task {
	if !info.transformLogBuffer.StartFlush(timetick) {
		return nil
	}
	return info.newFlushTransformLogBufferTask()
}

func (info *VChannelView) newFlushTransformLogBufferTask() scheduler.Task {
	task := &flushTransformLogBufferTask{
		vchannel:     info,
		precondition: info.transformLogTaskPreconditionLocked(),
	}
	info.transformLogTasks = append(info.transformLogTasks, task)
	return task
}

func (info *VChannelView) transformLogTaskPreconditionLocked() scheduler.Precondition {
	pending := info.transformLogTasks[:0]
	preconditions := make([]scheduler.Precondition, 0, len(info.transformLogTasks))
	for _, task := range info.transformLogTasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
		preconditions = append(preconditions, scheduler.After(task))
	}
	info.transformLogTasks = pending
	return scheduler.All(preconditions...)
}

func (info *VChannelView) markDeleteDataDurable(timetick uint64) {
	if timetick <= info.meta.GetDataCheckpointTimeTick() {
		return
	}
	info.markDataCheckpointLocked(timetick)
}

// IsActive returns true if the vchannel is active.
func (info *VChannelView) IsActive() bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL
}

func (info *VChannelView) ShouldSkipReplay(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return shouldSkipTombstonedVChannelMeta(info.meta, timetick)
}

func (info *VChannelView) CanStartNewCollectionAt(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		info.meta.GetTombstoneTimeTick() > 0 &&
		timetick > info.meta.GetTombstoneTimeTick()
}

func (info *VChannelView) CanReplayAt(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if shouldSkipTombstonedVChannelMeta(info.meta, timetick) {
		return false
	}
	if info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return true
	}
	return timetick <= info.meta.GetCheckpointTimeTick()
}

func (info *VChannelView) ShouldSkipPartitionReplay(partitionID int64, timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() != partitionID {
			continue
		}
		return shouldSkipTombstonedPartitionMeta(partition, timetick)
	}
	return false
}

func (info *VChannelView) CanReplayPartitionAt(partitionID int64, timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.canReplayPartitionAtLocked(partitionID, timetick)
}

func (info *VChannelView) CanReplayExistingPartitionAt(partitionID int64, timetick uint64) bool {
	if partitionID == common.AllPartitionsID {
		return true
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.canReplayPartitionAtLocked(partitionID, timetick) && info.hasPartitionMetaLocked(partitionID)
}

func (info *VChannelView) canReplayPartitionAtLocked(partitionID int64, timetick uint64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() != partitionID {
			continue
		}
		switch partition.GetState() {
		case streamingpb.PartitionState_PARTITION_STATE_NORMAL:
			return true
		case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
			return timetick <= partition.GetTombstoneTimeTick()
		default:
			return false
		}
	}
	return true
}

func (info *VChannelView) HasPartitionMeta(partitionID int64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.hasPartitionMetaLocked(partitionID)
}

func (info *VChannelView) hasPartitionMetaLocked(partitionID int64) bool {
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return true
		}
	}
	return false
}

func (info *VChannelView) TombstonedCleanupPlan(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) (dropSnapshot *streamingpb.VChannelMeta, cleanupPartitions map[int64]uint64) {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.vchannelTombstonedCleanupReadyLocked(metaPhysicalTimeTick, dataPhysicalTimeTick) {
		return proto.Clone(info.meta).(*streamingpb.VChannelMeta), nil
	}
	if info.dirty {
		return nil, nil
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partitionTombstonedCleanupReady(partition, metaPhysicalTimeTick, dataPhysicalTimeTick) &&
			info.persistedMetaTimeTick >= partition.GetTombstoneTimeTick() &&
			info.persistedDataTimeTick >= partition.GetTombstoneTimeTick() {
			if cleanupPartitions == nil {
				cleanupPartitions = make(map[int64]uint64)
			}
			cleanupPartitions[partition.GetPartitionId()] = partition.GetTombstoneTimeTick()
		}
	}
	if len(cleanupPartitions) == 0 {
		return nil, nil
	}
	return nil, cleanupPartitions
}

func (info *VChannelView) vchannelTombstonedCleanupReadyLocked(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	tombstoneTimeTick := info.meta.GetTombstoneTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		!info.dirty &&
		info.persistedMetaTimeTick >= tombstoneTimeTick &&
		info.persistedDataTimeTick >= tombstoneTimeTick &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}

func (info *VChannelView) VChannelDropCleanupSnapshot(tombstoneTimeTick uint64) *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()

	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED ||
		tombstoneTimeTick == 0 ||
		info.meta.GetTombstoneTimeTick() != tombstoneTimeTick ||
		info.dirty ||
		info.persistedMetaTimeTick < tombstoneTimeTick ||
		info.persistedDataTimeTick < tombstoneTimeTick {
		return nil
	}
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *VChannelView) ApplyPartitionCleanup(partitionIDs map[int64]uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()

	cleanup := info.partitionCleanupPlanLocked(partitionIDs)
	if len(cleanup) == 0 {
		return false
	}
	partitions := info.meta.GetCollectionInfo().GetPartitions()
	remaining := partitions[:0]
	for _, partition := range partitions {
		if _, ok := cleanup[partition.GetPartitionId()]; ok {
			continue
		}
		remaining = append(remaining, partition)
	}
	info.meta.CollectionInfo.Partitions = remaining
	info.dirty = true
	return true
}

func (info *VChannelView) partitionCleanupPlan(partitionIDs map[int64]uint64) map[int64]uint64 {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.partitionCleanupPlanLocked(partitionIDs)
}

func (info *VChannelView) partitionCleanupPlanLocked(partitionIDs map[int64]uint64) map[int64]uint64 {
	actualCleanup := make(map[int64]uint64)
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if tombstoneTimeTick, ok := partitionIDs[partition.GetPartitionId()]; ok &&
			partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
			partition.GetTombstoneTimeTick() == tombstoneTimeTick {
			actualCleanup[partition.GetPartitionId()] = tombstoneTimeTick
		}
	}
	return actualCleanup
}

// GetSchema returns the schema of the vchannel at the given timetick.
// return nil if the schema is not found.
func (info *VChannelView) GetSchema(timetick uint64) (int, *schemapb.CollectionSchema) {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.getSchemaLocked(timetick)
}

func (info *VChannelView) getSchemaLocked(timetick uint64) (int, *schemapb.CollectionSchema) {
	if info.meta.GetCollectionInfo() == nil {
		return -1, nil
	}
	if timetick == 0 {
		// timetick 0 means the latest schema.
		timetick = math.MaxUint64
	}

	for i := len(info.meta.CollectionInfo.Schemas) - 1; i >= 0; i-- {
		schema := info.meta.CollectionInfo.Schemas[i]
		if schema.CheckpointTimeTick <= timetick {
			return i, schema.Schema
		}
	}
	return -1, nil
}

func (info *VChannelView) ObserveSchemaChangeMessageV2(msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	if info.hasSchemaVersionLocked(msg.TimeTick(), msg.MustBody().Schema) {
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}

	info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema:             msg.MustBody().Schema,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		CheckpointTimeTick: msg.TimeTick(),
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *VChannelView) ObserveAlterCollectionMessageV2(msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	schemaChange := messageutil.IsSchemaChange(msg.Header())
	if schemaChange {
		schema := msg.MustBody().Updates.Schema
		if info.hasSchemaVersionLocked(msg.TimeTick(), schema) {
			return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
		}
		info.meta.CollectionInfo.Schemas = append(info.meta.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
			Schema:             schema,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
			CheckpointTimeTick: msg.TimeTick(),
		})
	}
	if !schemaChange && msg.TimeTick() == info.meta.GetCheckpointTimeTick() {
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *VChannelView) hasSchemaVersionLocked(timetick uint64, schema *schemapb.CollectionSchema) bool {
	for _, existing := range info.meta.GetCollectionInfo().GetSchemas() {
		if existing.GetCheckpointTimeTick() == timetick && proto.Equal(existing.GetSchema(), schema) {
			return true
		}
	}
	return false
}

func (info *VChannelView) ObserveDropCollectionMessageV1(msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	if isVChannelClosed(info.meta.GetState()) {
		// make it idempotent, only the first drop collection message can be observed.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *VChannelView) ObserveDropPartitionMessageV1(msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			// make it idempotent, only the first drop partition message can be observed.
			if !isPartitionNormal(partition.GetState()) {
				return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
			}
			partition.State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
			partition.TombstoneTimeTick = msg.TimeTick()
			info.meta.CheckpointTimeTick = msg.TimeTick()
			info.dirty = true
			return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
		}
	}
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

func (info *VChannelView) ObserveCreatePartitionMessageV1(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	info.mu.Lock()
	defer info.mu.Unlock()
	if msg.TimeTick() < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage.
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	if info.meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
		return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
	}
	for _, partition := range info.meta.CollectionInfo.Partitions {
		if partition.PartitionId == msg.Header().PartitionId {
			if partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_NORMAL {
				partition.State = streamingpb.PartitionState_PARTITION_STATE_NORMAL
				partition.TombstoneTimeTick = 0
				info.meta.CheckpointTimeTick = msg.TimeTick()
				info.dirty = true
			}
			return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
		}
	}
	info.meta.CollectionInfo.Partitions = append(info.meta.CollectionInfo.Partitions, &streamingpb.PartitionInfoOfVChannel{
		PartitionId: msg.Header().PartitionId,
		State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
	})
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return moduleapi.ObserveResult{Meta: info.MetaBarrier()}
}

// ConsumeDirtyAndGetSnapshot returns the snapshot of the vchannel recovery info.
// It returns nil if the vchannel recovery info is not dirty.
func (info *VChannelView) ConsumeDirtyAndGetSnapshot() *streamingpb.VChannelMeta {
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.dirty {
		return nil
	}
	return proto.Clone(info.meta).(*streamingpb.VChannelMeta)
}

func (info *VChannelView) maybeMarkTombstonedLocked() bool {
	changed := false
	if info.vchannelTombstoneFinalizeReadyLocked() {
		tombstoneTimeTick := info.meta.GetCheckpointTimeTick()
		info.meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
		info.meta.TombstoneTimeTick = tombstoneTimeTick
		info.dirty = true
		changed = true
	}
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if !info.partitionTombstoneFinalizeReadyLocked(partition) {
			continue
		}
		partition.State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
		info.dirty = true
		changed = true
	}
	return changed
}

func (info *VChannelView) vchannelTombstoneFinalizeReadyLocked() bool {
	tombstoneTimeTick := info.meta.GetCheckpointTimeTick()
	return info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED &&
		tombstoneTimeTick > 0 &&
		info.meta.GetDataCheckpointTimeTick() >= tombstoneTimeTick &&
		info.coveredSegmentsTombstonedLocked(common.AllPartitionsID, tombstoneTimeTick)
}

func (info *VChannelView) partitionTombstoneFinalizeReadyLocked(partition *streamingpb.PartitionInfoOfVChannel) bool {
	tombstoneTimeTick := partition.GetTombstoneTimeTick()
	return partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_DROPPED &&
		tombstoneTimeTick > 0 &&
		info.meta.GetDataCheckpointTimeTick() >= tombstoneTimeTick &&
		info.coveredSegmentsTombstonedLocked(partition.GetPartitionId(), tombstoneTimeTick)
}

func (info *VChannelView) dirtyPartitionTombstoneFrontierLocked() uint64 {
	if !info.dirty {
		return 0
	}
	frontier := uint64(math.MaxUint64)
	for _, partition := range info.meta.GetCollectionInfo().GetPartitions() {
		if partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED {
			continue
		}
		tombstoneTimeTick := partition.GetTombstoneTimeTick()
		if tombstoneTimeTick < frontier {
			frontier = tombstoneTimeTick
		}
	}
	if frontier == uint64(math.MaxUint64) {
		return 0
	}
	return frontier
}

func (info *VChannelView) coveredSegmentsTombstonedLocked(partitionID int64, timetick uint64) bool {
	for _, segment := range info.segments {
		if !segment.CoveredByTombstone(info.meta.GetVchannel(), partitionID, timetick) {
			continue
		}
		if !segment.TombstonePersisted() {
			return false
		}
	}
	return true
}

func isVChannelClosed(state streamingpb.VChannelState) bool {
	return state == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED ||
		state == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
}

func isPartitionNormal(state streamingpb.PartitionState) bool {
	return state == streamingpb.PartitionState_PARTITION_STATE_NORMAL
}

func shouldSkipTombstonedVChannelMeta(meta *streamingpb.VChannelMeta, timetick uint64) bool {
	return meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED &&
		meta.GetTombstoneTimeTick() > 0 &&
		timetick <= meta.GetTombstoneTimeTick()
}

func shouldSkipTombstonedPartitionMeta(meta *streamingpb.PartitionInfoOfVChannel, timetick uint64) bool {
	return meta.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
		meta.GetTombstoneTimeTick() > 0 &&
		timetick <= meta.GetTombstoneTimeTick()
}

func partitionTombstonedCleanupReady(meta *streamingpb.PartitionInfoOfVChannel, metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) bool {
	tombstoneTimeTick := meta.GetTombstoneTimeTick()
	return meta.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
		tombstoneTimeTick > 0 &&
		metaPhysicalTimeTick > tombstoneTimeTick &&
		dataPhysicalTimeTick > tombstoneTimeTick
}
