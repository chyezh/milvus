package recovery

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/gsegment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	componentRecoveryStorage = "recovery-storage"

	recoveryStorageStatePersistRecovering = "persist-recovering"
	recoveryStorageStateStreamRecovering  = "stream-recovering"
	recoveryStorageStateWorking           = "working"
)

// RecoverRecoveryStorage creates a new recovery storage.
func RecoverRecoveryStorage(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	cp *utility.WALCheckpoint,
	lastTimeTickMessage message.ImmutableMessage,
) (RecoveryStorage, *RecoverySnapshot, error) {
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel(), cp)
	if err := rs.recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.Channel(), lastTimeTickMessage); err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}

	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}
	// recovery storage start work.
	rs.metrics.ObserveStateChange(recoveryStorageStateWorking)
	rs.SetLogger(resource.Resource().Logger().With(
		zap.Int64("nodeID", paramtable.GetNodeID()),
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.String("state", recoveryStorageStateWorking)))
	rs.truncator = recoveryStreamBuilder.RWWALImpls()
	go rs.backgroundTask()
	return rs, snapshot, nil
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo, cp *utility.WALCheckpoint) *recoveryStorageImpl {
	cfg := newConfig()
	return &recoveryStorageImpl{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		currentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		channel:                channel,
		checkpoint:             cloneWALCheckpoint(cp),
		observedCheckpoint:     cloneWALCheckpoint(cp),
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		gracefulClosed:         false,
		metrics:                newRecoveryStorageMetrics(channel),
		segmentManager: gsegment.NewSegmentManager(
			resource.Resource().ChunkManager(),
			gsegment.WithContextIDAllocator(context.Background(), resource.Resource().IDAllocator()),
		),
	}
}

// recoveryStorageImpl is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type recoveryStorageImpl struct {
	log.Binder
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	cfg                    *config
	mu                     sync.Mutex
	currentClusterID       string
	channel                types.PChannelInfo
	vchannels              map[string]*vchannelRecoveryInfo
	checkpoint             *WALCheckpoint
	observedCheckpoint     *WALCheckpoint
	checkpointHistory      []*WALCheckpoint
	dirtyCounter           int // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier        chan struct{}
	gracefulClosed         bool
	truncator              walimpls.WALImpls
	metrics                *recoveryMetrics
	scannerTaskNotifier    *syncutil.AsyncTaskNotifier[struct{}]
	scannerTaskMetrics     *scannerTaskMetrics
	lastDispatchTimeTick   uint64
	emptyTimeTickCounter   prometheus.Counter
	pendingPersistSnapshot *RecoverySnapshot
	// used to mark switch MQ msg found
	alterWALInfo *AlterWALInfo
	// pendingSalvageCheckpoint holds the salvage checkpoint captured during force promote.
	// Set under r.mu; consumed and persisted by the background task to avoid holding the lock.
	pendingSalvageCheckpoint *utility.ReplicateCheckpoint
	// segment manager for L0/L1 segment buffering and persistence
	segmentManager *gsegment.SegmentManager
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RecoveryMetrics{
		RecoveryTimeTick: r.checkpoint.MetaCheckpoint.TimeTick,
	}
}

// GetSchema gets the schema of the collection at the given timetick.
func (r *recoveryStorageImpl) GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vchannelInfo, ok := r.vchannels[vchannel]; ok {
		_, schema := vchannelInfo.GetSchema(timetick)
		if schema == nil {
			r.Logger().DPanic("schema not found, fallback to latest schema", zap.String("vchannel", vchannel), zap.Uint64("timetick", timetick))
			if _, schema = vchannelInfo.GetSchema(0); schema != nil {
				return schema, nil
			}
			return nil, errors.Errorf("critical error: schema not found, vchannel: %s, timetick: %d", vchannel, timetick)
		}
		return schema, nil
	}
	return nil, errors.Errorf("critical error: vchannel not found, vchannel: %s, timetick: %d", vchannel, timetick)
}

// ObserveMessage is called when a new message is observed.
func (r *recoveryStorageImpl) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if h := msg.BroadcastHeader(); h != nil {
		if err := streaming.WAL().Broadcast().Ack(ctx, msg); err != nil {
			r.Logger().Warn("failed to ack broadcast message", zap.Error(err))
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeMessage(msg)
	return nil
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.closeScannerTask()
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
	r.metrics.Close()
	r.segmentManager.Close()
}

// notifyPersist notifies a persist operation.
func (r *recoveryStorageImpl) notifyPersist() {
	select {
	case r.persistNotifier <- struct{}{}:
	default:
	}
}

// consumeDirtySnapshot consumes the dirty state and returns a snapshot to persist.
// A snapshot is always a consistent state (fully consume a message or a txn message) of the recovery storage.
func (r *recoveryStorageImpl) consumeDirtySnapshot() *RecoverySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceDataCheckpointIfSyncedLocked()

	managerSnapshots := r.segmentManager.GetDirtySnapshots()
	hasManagerSnapshots := len(managerSnapshots) > 0

	if r.dirtyCounter == 0 && r.pendingSalvageCheckpoint == nil && !hasManagerSnapshots {
		return nil
	}

	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)

	for segmentID, snapshot := range managerSnapshots {
		segments[segmentID] = snapshot
	}
	for _, vchannel := range r.vchannels {
		dirtySnapshot, shouldBeRemoved := vchannel.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.vchannels, vchannel.meta.Vchannel)
		}
		if dirtySnapshot != nil {
			vchannels[vchannel.meta.Vchannel] = dirtySnapshot
		}
	}
	// Atomically capture the salvage checkpoint alongside other dirty state.
	// Clearing it here (under r.mu) ensures it is only consumed once.
	salvageCP := r.pendingSalvageCheckpoint
	r.pendingSalvageCheckpoint = nil
	// clear the dirty counter.
	r.dirtyCounter = 0
	snapshotCheckpoint := r.observedCheckpoint.Clone()
	snapshotCheckpoint.DataCheckpoint = r.checkpoint.DataCheckpoint.Clone()
	r.checkpoint = snapshotCheckpoint.Clone()
	return &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         snapshotCheckpoint,
		SalvageCheckpoint:  salvageCP,
	}
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(msg message.ImmutableMessage) {
	if msg.TimeTick() <= r.checkpoint.MetaCheckpoint.TimeTick {
		if r.Logger().Level().Enabled(zap.DebugLevel) {
			r.Logger().Debug("skip the message before the checkpoint",
				log.FieldMessage(msg),
				zap.Uint64("checkpoint", r.checkpoint.MetaCheckpoint.TimeTick),
				zap.Uint64("incoming", msg.TimeTick()),
			)
		}
		return
	}
	r.handleMessage(msg)

	r.updateCheckpoint(msg)
	r.segmentManager.SealStaleChunks(
		msg.TimeTick(),
		paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second),
	)
	r.metrics.ObServeInMemMetrics(r.observedCheckpoint.MetaCheckpoint.TimeTick)

	if !msg.IsPersisted() {
		// only trigger persist when the message is persisted.
		return
	}
	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

func (r *recoveryStorageImpl) advanceDataCheckpointIfSyncedLocked() {
	if r.observedCheckpoint == nil {
		r.observedCheckpoint = cloneWALCheckpoint(r.checkpoint)
	}
	r.advanceDataCheckpointToLocked(r.segmentManager.SyncSafeTimeTick(r.observedCheckpoint.MetaCheckpoint.TimeTick))
}

// advanceCheckpointIfSyncedLocked is kept for tests that exercise checkpoint
// mutation directly. Production persistence advances the meta checkpoint in
// consumeDirtySnapshot and advances the data checkpoint through gsegment.
func (r *recoveryStorageImpl) advanceCheckpointIfSyncedLocked() {
	r.advanceDataCheckpointIfSyncedLocked()
	if r.observedCheckpoint == nil {
		return
	}
	dataCheckpoint := r.checkpoint.DataCheckpoint.Clone()
	r.checkpoint = r.observedCheckpoint.Clone()
	r.checkpoint.DataCheckpoint = dataCheckpoint
}

func (r *recoveryStorageImpl) advanceDataCheckpointToLocked(safeTimeTick uint64) {
	var candidate *WALCheckpoint
	prune := 0
	for idx, checkpoint := range r.checkpointHistory {
		if checkpoint.MetaCheckpoint.TimeTick > safeTimeTick {
			break
		}
		candidate = checkpoint
		prune = idx + 1
	}
	if candidate == nil || !dataCheckpointAhead(candidate, r.checkpoint.DataCheckpoint) {
		return
	}
	r.checkpoint.DataCheckpoint = &utility.Checkpoint{
		MessageID: candidate.MetaCheckpoint.MessageID,
		TimeTick:  candidate.MetaCheckpoint.TimeTick,
	}
	if prune > 0 {
		r.checkpointHistory = append([]*WALCheckpoint(nil), r.checkpointHistory[prune:]...)
	}
}

func dataCheckpointAhead(candidate *WALCheckpoint, current *utility.Checkpoint) bool {
	if candidate == nil || current == nil {
		return candidate != nil && current == nil
	}
	if candidate.MetaCheckpoint.TimeTick != current.TimeTick {
		return candidate.MetaCheckpoint.TimeTick > current.TimeTick
	}
	if candidate.MetaCheckpoint.MessageID == nil || current.MessageID == nil {
		return candidate.MetaCheckpoint.MessageID != current.MessageID
	}
	return !candidate.MetaCheckpoint.MessageID.LT(current.MessageID)
}

func cloneWALCheckpoint(checkpoint *WALCheckpoint) *WALCheckpoint {
	if checkpoint == nil {
		return nil
	}
	return checkpoint.Clone()
}

func (r *recoveryStorageImpl) appendObservedCheckpointLocked() {
	if r.observedCheckpoint == nil {
		return
	}
	r.checkpointHistory = append(r.checkpointHistory, r.observedCheckpoint.Clone())
}

// updateCheckpoint updates the checkpoint of the recovery storage.
func (r *recoveryStorageImpl) updateCheckpoint(msg message.ImmutableMessage) {
	if r.observedCheckpoint == nil {
		r.observedCheckpoint = cloneWALCheckpoint(r.checkpoint)
	}
	if r.observedCheckpoint.MetaCheckpoint == nil {
		r.observedCheckpoint.MetaCheckpoint = &utility.Checkpoint{}
	}
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		cfg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
		header := cfg.Header()

		// Check ignore field - if true, skip updating ReplicateConfig and ReplicateCheckpoint
		// This is used for incomplete switchover messages that should be ignored after force promote
		if header.Ignore {
			r.Logger().Info("AlterReplicateConfig message has ignore flag set, skipping checkpoint update",
				zap.Bool("forcePromote", header.ForcePromote))
		} else {
			r.observedCheckpoint.ReplicateConfig = header.ReplicateConfiguration
			clusterRole := replicateutil.MustNewConfigHelper(r.currentClusterID, header.ReplicateConfiguration).GetCurrentCluster()
			switch clusterRole.Role() {
			case replicateutil.RolePrimary:
				if header.GetForcePromote() && r.observedCheckpoint.ReplicateCheckpoint != nil {
					// Store for background task to persist; never call etcd while holding r.mu.
					r.pendingSalvageCheckpoint = r.observedCheckpoint.ReplicateCheckpoint
					r.notifyPersist()
				}
				r.observedCheckpoint.ReplicateCheckpoint = nil
			case replicateutil.RoleSecondary:
				// Update the replicate checkpoint if the cluster role is secondary.
				sourceClusterID := clusterRole.SourceCluster().GetClusterId()
				sourcePChannel := clusterRole.MustGetSourceChannel(r.channel.Name)
				if r.observedCheckpoint.ReplicateCheckpoint == nil || r.observedCheckpoint.ReplicateCheckpoint.ClusterID != sourceClusterID {
					r.observedCheckpoint.ReplicateCheckpoint = &utility.ReplicateCheckpoint{
						ClusterID: sourceClusterID,
						PChannel:  sourcePChannel,
						MessageID: nil,
						TimeTick:  0,
					}
				}
			}
		}
	}
	r.observedCheckpoint.MetaCheckpoint.MessageID = msg.LastConfirmedMessageID()
	r.observedCheckpoint.MetaCheckpoint.TimeTick = msg.TimeTick()
	if r.alterWALInfo != nil && r.alterWALInfo.FoundAlterWALMsg && (r.observedCheckpoint.AlterWalState == nil || r.observedCheckpoint.AlterWalState.Stage == streamingpb.AlterWALStage_NONE) {
		r.observedCheckpoint.AlterWalState = &streamingpb.AlterWALState{
			TargetWalName: r.alterWALInfo.TargetWALName,
			TimeTick:      r.alterWALInfo.AlterWALTs,
			Configs:       r.alterWALInfo.AlterWALConfig,
			Stage:         streamingpb.AlterWALStage_FLUSHING,
		}
	}

	// update the replicate checkpoint.
	replicateHeader := msg.ReplicateHeader()
	if replicateHeader == nil {
		r.appendObservedCheckpointLocked()
		return
	}
	if r.observedCheckpoint.ReplicateCheckpoint == nil {
		r.detectInconsistency(msg, "replicate checkpoint is nil when incoming replicate message")
		r.appendObservedCheckpointLocked()
		return
	}
	if replicateHeader.ClusterID != r.observedCheckpoint.ReplicateCheckpoint.ClusterID {
		r.detectInconsistency(msg,
			"replicate header cluster id mismatch",
			zap.String("expected", r.observedCheckpoint.ReplicateCheckpoint.ClusterID),
			zap.String("actual", replicateHeader.ClusterID))
		r.appendObservedCheckpointLocked()
		return
	}
	r.observedCheckpoint.ReplicateCheckpoint.MessageID = replicateHeader.LastConfirmedMessageID
	r.observedCheckpoint.ReplicateCheckpoint.TimeTick = replicateHeader.TimeTick
	r.appendObservedCheckpointLocked()
}

// The incoming message id is always sorted with timetick.
func (r *recoveryStorageImpl) handleMessage(msg message.ImmutableMessage) {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		// message on control channel except pchannel-level messages is just used to determine the DDL/DCL order,
		// will not affect the recovery storage, so skip it.
		return
	}

	if msg.VChannel() != "" && !msg.IsPChannelLevel() && msg.MessageType() != message.MessageTypeCreateCollection &&
		msg.MessageType() != message.MessageTypeDropCollection && r.vchannels[msg.VChannel()] == nil && !funcutil.IsControlChannel(msg.VChannel()) {
		r.detectInconsistency(msg, "vchannel not found")
	}

	switch msg.MessageType() {
	case message.MessageTypeInsert:
		immutableMsg := message.MustAsImmutableInsertMessageV1(msg)
		r.handleInsert(immutableMsg)
	case message.MessageTypeDelete:
		immutableMsg := message.MustAsImmutableDeleteMessageV1(msg)
		r.handleDelete(immutableMsg)
	case message.MessageTypeCreateSegment:
		immutableMsg := message.MustAsImmutableCreateSegmentMessageV2(msg)
		r.handleCreateSegment(immutableMsg)
	case message.MessageTypeFlush:
		immutableMsg := message.MustAsImmutableFlushMessageV2(msg)
		r.handleFlush(immutableMsg)
	case message.MessageTypeManualFlush:
		immutableMsg := message.MustAsImmutableManualFlushMessageV2(msg)
		r.handleManualFlush(immutableMsg)
	case message.MessageTypeFlushAll:
		immutableMsg := message.MustAsImmutableFlushAllMessageV2(msg)
		r.handleFlushAll(immutableMsg)
	case message.MessageTypeCreateCollection:
		immutableMsg := message.MustAsImmutableCreateCollectionMessageV1(msg)
		r.handleCreateCollection(immutableMsg)
	case message.MessageTypeDropCollection:
		immutableMsg := message.MustAsImmutableDropCollectionMessageV1(msg)
		r.handleDropCollection(immutableMsg)
	case message.MessageTypeCreatePartition:
		immutableMsg := message.MustAsImmutableCreatePartitionMessageV1(msg)
		r.handleCreatePartition(immutableMsg)
	case message.MessageTypeDropPartition:
		immutableMsg := message.MustAsImmutableDropPartitionMessageV1(msg)
		r.handleDropPartition(immutableMsg)
	case message.MessageTypeTxn:
		immutableMsg := message.AsImmutableTxnMessage(msg)
		r.handleTxn(immutableMsg)
	case message.MessageTypeImport:
		immutableMsg := message.MustAsImmutableImportMessageV1(msg)
		r.handleImport(immutableMsg)
	case message.MessageTypeSchemaChange:
		immutableMsg := message.MustAsImmutableSchemaChangeMessageV2(msg)
		r.handleSchemaChange(immutableMsg)
	case message.MessageTypeAlterCollection:
		immutableMsg := message.MustAsImmutableAlterCollectionMessageV2(msg)
		r.handleAlterCollection(immutableMsg)
	case message.MessageTypeTruncateCollection:
		immutableMsg := message.MustAsImmutableTruncateCollectionMessageV2(msg)
		r.handleTruncateCollection(immutableMsg)
	case message.MessageTypeTimeTick:
		// nothing, the time tick message make no recovery operation.
	case message.MessageTypeAlterWAL:
		immutableMsg := message.MustAsImmutableAlterWALMessageV2(msg)
		r.handleAlterWAL(immutableMsg)
	}
}

// handleAlterWAL handles the alter WAL message.
// Flushes all growing segments to ensure segment data does not span across different WAL implementations.
func (r *recoveryStorageImpl) handleAlterWAL(msg message.ImmutableAlterWALMessageV2) {
	header := msg.Header()

	// Flush all growing segments before WAL switch
	flushed := r.segmentManager.FlushAllSegments(msg.TimeTick())
	segmentIDs, rows, binarySize := segmentSnapshotLogFields(flushed)

	if len(segmentIDs) > 0 {
		r.Logger().Info("flush all growing segments for WAL switch",
			log.FieldMessage(msg),
			zap.Stringer("targetWALName", header.TargetWalName),
			zap.Int64s("segmentIDs", segmentIDs),
			zap.Uint64s("rows", rows),
			zap.Uint64s("binarySize", binarySize))
	} else {
		r.Logger().Info("no growing segments to flush for WAL switch",
			log.FieldMessage(msg),
			zap.Stringer("targetWALName", header.TargetWalName))
	}

	// Record alter WAL information for snapshot persistence
	r.alterWALInfo = &AlterWALInfo{
		FoundAlterWALMsg: true,
		TargetWALName:    header.TargetWalName,
		AlterWALConfig:   header.Config,
		AlterWALTs:       msg.TimeTick(),
	}
}

// handleInsert handles the insert message.
func (r *recoveryStorageImpl) handleInsert(msg message.ImmutableInsertMessageV1) {
	r.segmentManager.ObserveInsert(msg)
}

// handleDelete handles the delete message.
func (r *recoveryStorageImpl) handleDelete(msg message.ImmutableDeleteMessageV1) {
	var schema *schemapb.CollectionSchema
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		_, schema = vchannelInfo.GetSchema(msg.TimeTick())
	}
	r.segmentManager.ObserveDelete(msg, schema)
}

// handleCreateSegment handles the create segment message.
func (r *recoveryStorageImpl) handleCreateSegment(msg message.ImmutableCreateSegmentMessageV2) {
	// Skip segment creation if the vchannel does not exist (collection was dropped).
	// During WAL replay (e.g., Kafka offset reset), CreateSegment messages may appear
	// for collections whose vchannels have already been cleaned up.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		r.Logger().Warn("skip create segment for non-active vchannel",
			log.FieldMessage(msg),
			zap.String("vchannel", msg.VChannel()),
			zap.Int64("segmentID", msg.Header().SegmentId),
		)
		return
	}
	segmentMeta := NewSegmentAssignmentMetaFromCreateSegmentMessage(msg)

	if segmentMeta.GetStat().GetLevel() == datapb.SegmentLevel_L0 {
		segmentMeta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		}
		r.segmentManager.CreateL0Segment(segmentMeta)
	} else {
		segmentMeta.PersistedStorage = &streamingpb.SegmentAssignmentMeta_L1{
			L1: &streamingpb.L1SegmentPersistedStorage{},
		}

		var schema *schemapb.CollectionSchema
		if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
			_, schema = vchannelInfo.GetSchema(msg.TimeTick())
		}
		r.segmentManager.CreateL1Segment(segmentMeta, schema)
	}

	r.Logger().Info("create segment", log.FieldMessage(msg))
}

// handleFlush handles the flush message.
func (r *recoveryStorageImpl) handleFlush(msg message.ImmutableFlushMessageV2) {
	header := msg.Header()
	if snapshot, ok := r.segmentManager.FlushSegment(header.SegmentId, msg.TimeTick()); ok {
		r.Logger().Info("flush segment",
			log.FieldMessage(msg),
			zap.Uint64("rows", snapshot.GetStat().GetModifiedRows()),
			zap.Uint64("binarySize", snapshot.GetStat().GetModifiedBinarySize()))
	}
}

// handleManualFlush handles the manual flush message.
func (r *recoveryStorageImpl) handleManualFlush(msg message.ImmutableManualFlushMessageV2) {
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)
}

// handleFlushAll handles the flush all message.
func (r *recoveryStorageImpl) handleFlushAll(msg message.ImmutableFlushAllMessageV2) {
	r.logFlushedSegments(msg, r.segmentManager.FlushAllSegments(msg.TimeTick()))
}

// flushSegments flushes the segments in the recovery storage.
func (r *recoveryStorageImpl) flushSegments(msg message.ImmutableMessage, sealSegmentIDs map[int64]struct{}) {
	flushed := r.segmentManager.FlushSegments(sealSegmentIDs, msg.TimeTick())
	if len(flushed) != len(sealSegmentIDs) {
		r.detectInconsistency(msg, "flush segments not exist",
			zap.Int64s("wanted", segmentIDsFromSet(sealSegmentIDs)),
			zap.Int64s("actually", segmentIDsFromSnapshots(flushed)))
	}
	r.logFlushedSegments(msg, flushed)
}

func (r *recoveryStorageImpl) logFlushedSegments(msg message.ImmutableMessage, snapshots map[int64]*streamingpb.SegmentAssignmentMeta) {
	segmentIDs, rows, binarySize := segmentSnapshotLogFields(snapshots)
	r.Logger().Info("flush segments of collection by flush", log.FieldMessage(msg),
		zap.Uint64s("rows", rows),
		zap.Uint64s("binarySize", binarySize),
		zap.Int("flushedSegmentCount", len(segmentIDs)),
	)
}

func segmentSnapshotLogFields(snapshots map[int64]*streamingpb.SegmentAssignmentMeta) ([]int64, []uint64, []uint64) {
	segmentIDs := segmentIDsFromSnapshots(snapshots)
	rows := make([]uint64, 0, len(segmentIDs))
	binarySize := make([]uint64, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		stat := snapshots[segmentID].GetStat()
		rows = append(rows, stat.GetModifiedRows())
		binarySize = append(binarySize, stat.GetModifiedBinarySize())
	}
	return segmentIDs, rows, binarySize
}

func segmentIDsFromSnapshots(snapshots map[int64]*streamingpb.SegmentAssignmentMeta) []int64 {
	segmentIDs := make([]int64, 0, len(snapshots))
	for segmentID := range snapshots {
		segmentIDs = append(segmentIDs, segmentID)
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })
	return segmentIDs
}

func segmentIDsFromSet(segmentIDs map[int64]struct{}) []int64 {
	ids := make([]int64, 0, len(segmentIDs))
	for segmentID := range segmentIDs {
		ids = append(ids, segmentID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// handleCreateCollection handles the create collection message.
func (r *recoveryStorageImpl) handleCreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	if _, ok := r.vchannels[msg.VChannel()]; ok {
		return
	}
	r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromCreateCollectionMessage(msg)
	r.Logger().Info("create collection", log.FieldMessage(msg))
}

// handleDropCollection handles the drop collection message.
func (r *recoveryStorageImpl) handleDropCollection(msg message.ImmutableDropCollectionMessageV1) {
	// Always flush first: during WAL replay, CreateSegment/Insert messages may have recreated
	// GROWING segments after the vchannel was marked DROPPED (non-atomic etcd persistence or
	// Kafka offset compaction). Flushing unconditionally ensures idempotent replay.
	r.flushAllSegmentOfCollection(msg, msg.Header().CollectionId)
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok && vchannelInfo.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		vchannelInfo.ObserveDropCollection(msg)
	}
	r.Logger().Info("drop collection", log.FieldMessage(msg))
}

// flushAllSegmentOfCollection flushes all segments of the collection.
func (r *recoveryStorageImpl) flushAllSegmentOfCollection(msg message.ImmutableMessage, collectionID int64) {
	flushed := r.segmentManager.FlushSegmentsByCollection(collectionID, msg.TimeTick())
	segmentIDs, rows, _ := segmentSnapshotLogFields(flushed)
	r.Logger().Info("flush all segments of collection", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleCreatePartition handles the create partition message.
func (r *recoveryStorageImpl) handleCreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveCreatePartition(msg)
	r.Logger().Info("create partition", log.FieldMessage(msg))
}

// handleDropPartition handles the drop partition message.
func (r *recoveryStorageImpl) handleDropPartition(msg message.ImmutableDropPartitionMessageV1) {
	// Always flush first: same rationale as handleDropCollection — orphaned GROWING segments
	// may exist for this partition due to non-atomic etcd persistence or WAL offset reset.
	r.flushAllSegmentOfPartition(msg, msg.Header().PartitionId)
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok && vchannelInfo.meta.State != streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		vchannelInfo.ObserveDropPartition(msg)
	}
	r.Logger().Info("drop partition", log.FieldMessage(msg))
}

// flushAllSegmentOfPartition flushes all segments of the partition.
func (r *recoveryStorageImpl) flushAllSegmentOfPartition(msg message.ImmutableMessage, partitionID int64) {
	flushed := r.segmentManager.FlushSegmentsByPartition(partitionID, msg.TimeTick())
	segmentIDs, rows, _ := segmentSnapshotLogFields(flushed)
	r.Logger().Info("flush all segments of partition", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleTxn handles the txn message.
func (r *recoveryStorageImpl) handleTxn(msg message.ImmutableTxnMessage) {
	msg.RangeOver(func(im message.ImmutableMessage) error {
		r.handleMessage(im)
		return nil
	})
}

// handleImport handles the import message.
func (r *recoveryStorageImpl) handleImport(_ message.ImmutableImportMessageV1) {
}

// handleSchemaChange handles the schema change message.
func (r *recoveryStorageImpl) handleSchemaChange(msg message.ImmutableSchemaChangeMessageV2) {
	// when schema change happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().FlushedSegmentIds))
	for _, segmentID := range msg.Header().FlushedSegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)

	// persist the schema change into recovery info.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		vchannelInfo.ObserveSchemaChange(msg)
	}
}

// handlePutCollection handles the put collection message.
func (r *recoveryStorageImpl) handleAlterCollection(msg message.ImmutableAlterCollectionMessageV2) {
	// when put collection happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().FlushedSegmentIds))
	for _, segmentID := range msg.Header().FlushedSegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)

	// persist the schema change into recovery info.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		vchannelInfo.ObserveAlterCollection(msg)
	}
}

// handleTruncateCollection handles the truncate collection message.
func (r *recoveryStorageImpl) handleTruncateCollection(msg message.ImmutableTruncateCollectionMessageV2) {
	// when truncate collection happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)
}

// detectInconsistency detects the inconsistency in the recovery storage.
func (r *recoveryStorageImpl) detectInconsistency(msg message.ImmutableMessage, reason string, extra ...zap.Field) {
	fields := make([]zap.Field, 0, len(extra)+2)
	fields = append(fields, log.FieldMessage(msg), zap.String("reason", reason))
	fields = append(fields, extra...)
	// The log is not fatal in some cases.
	// because our meta is not atomic-updated, so these error may be logged if crashes when meta updated partially.
	r.Logger().Warn("inconsistency detected", fields...)
	r.metrics.ObserveInconsitentEvent()
}

// GetDataCheckpoint returns the current gsegment-gated data checkpoint.
func (r *recoveryStorageImpl) GetDataCheckpoint(ctx context.Context) *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceDataCheckpointIfSyncedLocked()
	if r.checkpoint == nil || r.checkpoint.DataCheckpoint == nil {
		return nil
	}
	checkpoint := r.checkpoint.Clone()
	checkpoint.MetaCheckpoint.MessageID = r.checkpoint.DataCheckpoint.MessageID
	checkpoint.MetaCheckpoint.TimeTick = r.checkpoint.DataCheckpoint.TimeTick
	return checkpoint
}
