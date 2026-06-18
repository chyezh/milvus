package adaptor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/idf"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
// Test Only
// Deprecated: Use NewOpenerAdaptor instead.
func adaptImplsToOpener(basicOpener walimpls.OpenerImpls, interceptorBuilders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: interceptorBuilders,
	}
	o.openerCache[message.WALNameTest] = basicOpener
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// NewOpenerAdaptor creates a new dynamic wal opener that can open different MQ types at runtime.
// It doesn't bind to a specific walName at construction time, instead it selects the appropriate
// wal implementation based on the walName in OpenOption when Open() is called.
func NewOpenerAdaptor(builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper that adapts walimpls.OpenerImpls to wal.Opener.
// It supports opening different WALImpls dynamically at runtime.
type openerAdaptorImpl struct {
	log.Binder

	lifetime            *typeutil.Lifetime
	mu                  sync.Mutex                               // protects openerCache
	openerCache         map[message.WALName]walimpls.OpenerImpls // cache of opened walimpls.OpenerImpls, dynamically created based on walName
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
}

var (
	walSwitchFlushCheckInterval = time.Second
	walSwitchFlushTimeout       = time.Minute
)

// Open opens a wal instance for the channel.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	// Determine which walName to use
	walName, err := o.determineWALName(ctx, opt)
	if err != nil {
		return nil, err
	}

	logger := log.Ctx(ctx).With(zap.String("channel", opt.Channel.String()), zap.Stringer("walName", walName))

	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(ctx, walName)
	if err != nil {
		logger.Warn("get or create underlying wal impls opener failed", zap.Error(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn("open wal impls failed", zap.Error(err))
		return nil, err
	}

	var wal wal.WAL
	switch opt.Channel.AccessMode {
	case types.AccessModeRW:
		wal, err = o.openRWWAL(ctx, l, opt)
	case types.AccessModeRO:
		wal, err = o.openROWAL(l)
	default:
		panic("unknown access mode")
	}
	if err != nil {
		logger.Warn("open wal failed", zap.Error(err))
		return nil, err
	}
	logger.Info("open wal done", zap.Stringer("walName", walName), zap.String("pchannel", opt.Channel.Name))
	return wal, nil
}

// determineWALName determines which walName to use for the given channel.
func (o *openerAdaptorImpl) determineWALName(ctx context.Context, opt *wal.OpenOption) (message.WALName, error) {
	walName := message.WALNameUnknown
	catalog := resource.Resource().StreamingNodeCatalog()
	cpProto, err := catalog.GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return message.WALNameUnknown, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	if cpProto != nil {
		checkpoint := utility.NewWALCheckpointFromProto(cpProto)
		log.Ctx(ctx).Info("get checkpoint from catalog",
			zap.String("channel", opt.Channel.Name),
			zap.Stringer("checkpoint", checkpoint.MessageID),
			zap.Uint64("checkpointTimeTick", checkpoint.TimeTick),
			zap.Stringer("currentWAL", checkpoint.MessageID.WALName()),
			zap.Any("AlterWalState", checkpoint.AlterWalState))
		walName = checkpoint.MessageID.WALName()
	}

	if walName == message.WALNameUnknown {
		// Use default WAL if already register
		walName = message.GetDefaultWALName()
	}

	if walName == message.WALNameUnknown {
		// Use wal selector to choose one
		walName = util.MustSelectWALName()
	}
	return walName, nil
}

// getOrCreateOpenerImpl gets an existing opener from cache or creates a new one.
func (o *openerAdaptorImpl) getOrCreateOpenerImpl(ctx context.Context, walName message.WALName) (walimpls.OpenerImpls, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Double-check after acquiring write lock
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Build and cache new opener
	builderImpl := registry.MustGetBuilder(walName)
	opener, err := builderImpl.Build()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build walimpls opener for %s", walName)
	}

	o.openerCache[walName] = opener
	log.Ctx(ctx).Info("created and cached new walimpls opener", zap.Stringer("walName", walName))
	return opener, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	cpProto, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	cp := utility.NewWALCheckpointFromProto(cpProto)

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, l, cp)
	if err != nil {
		roWAL.Close()
		return nil, errors.Wrap(err, "when building interceptor params")
	}
	queryViewCatalog := resource.Resource().StreamingNodeQueryViewCatalog()
	persistedViews, err := queryViewCatalog.ListQueryViews(ctx)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when loading streaming node query view meta")
	}
	pchannelViews := snview.FilterQueryViewsByPChannel(opt.Channel.Name, persistedViews)
	resourceBaseByVChannel := snview.OldestUpDataVersions(pchannelViews)
	recoveredLoadConfigs := snview.RecoveredLoadConfigs(pchannelViews)
	resMgr := viewresource.NewManager(nil, idf.NewFutureProvider(
		resource.Resource().MixCoordClient(),
		idf.WithChunkManager(resource.Resource().ChunkManager()),
	))
	rs, snapshot, err := recovery.RecoverRecoveryStorage(
		ctx,
		newRecoveryStreamBuilder(roWAL),
		cp,
		param.LastTimeTickMessage,
		recovery.WithLoadConfigListener(resMgr),
		recovery.WithResourceRecoveryBaseSelector(func(vchannel string) (qviews.DataVersion, bool) {
			version, ok := resourceBaseByVChannel[vchannel]
			return version, ok
		}),
		recovery.WithRecoveredLoadConfigProvider(func(vchannel string) *streamingpb.VChannelLoadConfig {
			return recoveredLoadConfigs[vchannel]
		}),
	)
	if err != nil {
		resMgr.Close()
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}
	param.RecoveryStorage = rs
	snHandler := snview.RecoverPChannelSNQueryViewHandler(opt.Channel.Name, queryViewCatalog, resMgr, pchannelViews)
	unregisterQueryViewHandler := resource.Resource().QueryViewRouter().Register(opt.Channel.Name, snHandler)

	// Handle alter WAL if found in snapshot
	// This flushes all remaining data and triggers WAL switch to the target implementation
	if snapshot.AlterWALInfo != nil && snapshot.AlterWALInfo.FoundAlterWALMsg {
		unregisterQueryViewHandler()
		rs.DetachLoadConfigListener()
		snHandler.CloseForHandoff()
		resMgr.Close()
		return o.handleAlterWAL(ctx, l, opt, roWAL, param, rs, snapshot)
	}

	param.LastConfirmedMessageID = determineLastConfirmedMessageID(param.LastTimeTickMessage.MessageID(), snapshot.TxnBuffer)
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})
	// Load salvage checkpoints from etcd (one per source cluster that was force-promoted from).
	var salvageCheckpoints []*utility.ReplicateCheckpoint
	if salvageCPProtos, err := resource.Resource().StreamingNodeCatalog().GetSalvageCheckpoint(ctx, param.ChannelInfo.Name); err != nil {
		log.Ctx(ctx).Info("failed to load salvage checkpoints", zap.Error(err))
	} else {
		for _, proto := range salvageCPProtos {
			salvageCheckpoints = append(salvageCheckpoints, utility.NewReplicateCheckpointFromProto(proto))
		}
	}

	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
			SalvageCheckpoints:     salvageCheckpoints,
		},
	); err != nil {
		unregisterQueryViewHandler()
		rs.DetachLoadConfigListener()
		snHandler.CloseForHandoff()
		resMgr.Close()
		return nil, err
	}

	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param)
	wal.queryViewHandler = snHandler
	wal.viewResourceManager = resMgr
	wal.unregisterQueryViewHandler = unregisterQueryViewHandler
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// determineLastConfirmedMessageID determines the last confirmed message id after recovery.
// The last confirmed message id is the minimum last confirmed message id of all uncommitted txn messages.
func determineLastConfirmedMessageID(lastTimeTickMessageID message.MessageID, txnBuffer *utility.TxnBuffer) message.MessageID {
	// From here, we can read all messages which timetick is greater than timetick of LastTimeTickMessage sent at these term.
	lastConfirmedMessageID := lastTimeTickMessageID
	for _, builder := range txnBuffer.GetUncommittedMessageBuilder() {
		if builder.LastConfirmedMessageID().LT(lastConfirmedMessageID) {
			// use the minimum last confirmed message id of all uncommitted txn messages to protect the `LastConfirmedMessageID` promise.
			lastConfirmedMessageID = builder.LastConfirmedMessageID()
		}
	}
	return lastConfirmedMessageID
}

// handleAlterWAL handles WAL switch operation in two stages:
// Stage 1 (FLUSHING): Flush all growing segments and wait for completion
// Stage 2 (ADVANCE_CHECKPOINT): Update vchannel checkpoints and pchannel consume checkpoint
// Returns an error to trigger WAL re-opening after successful switch
func (o *openerAdaptorImpl) handleAlterWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption,
	roWAL *roWALAdaptorImpl, param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot,
) (wal.WAL, error) {
	log.Ctx(ctx).Info("detected alter WAL message in snapshot",
		zap.String("channel", opt.Channel.String()),
		zap.Bool("foundAlterWAL", snapshot.AlterWALInfo.FoundAlterWALMsg),
		zap.Stringer("targetWAL", snapshot.AlterWALInfo.TargetWALName),
		zap.String("checkpointMessageID", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Any("alterWALConfig", snapshot.AlterWALInfo.AlterWALConfig))

	if snapshot.Checkpoint.AlterWalState != nil && snapshot.Checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_FLUSHING {
		flushingErr := o.handleAlterWALFlushingStage(ctx, opt, roWAL, param, rs, snapshot)
		if flushingErr != nil {
			return nil, errors.Wrap(flushingErr, "failed to handle alter WAL flushing stage")
		}
	}

	if snapshot.Checkpoint.AlterWalState != nil && snapshot.Checkpoint.AlterWalState.Stage == streamingpb.AlterWALStage_ADVANCE_CHECKPOINT {
		advanceCheckpointsErr := o.handleAlterWALAdvanceCheckpointsStage(ctx, opt, snapshot)
		if advanceCheckpointsErr != nil {
			return nil, errors.Wrap(advanceCheckpointsErr, "failed to handle alter WAL advance checkpoints stage")
		}
	}

	targetWALName := snapshot.AlterWALInfo.TargetWALName
	return nil, status.NewInner("WAL switch success: %s switch to %s finish, re-opening required", opt.Channel.Name, targetWALName)
}

func (o *openerAdaptorImpl) handleAlterWALFlushingStage(ctx context.Context, opt *wal.OpenOption, roWAL *roWALAdaptorImpl,
	param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot,
) error {
	// Wait for all data up to target time tick to be flushed
	targetTimeTick := snapshot.AlterWALInfo.AlterWALTs
	targetWALName := snapshot.AlterWALInfo.TargetWALName
	log.Ctx(ctx).Info("waiting for recovery data checkpoint before WAL switch",
		zap.String("channel", opt.Channel.Name),
		zap.Uint64("targetTimeTick", targetTimeTick))

	ticker := time.NewTicker(walSwitchFlushCheckInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(walSwitchFlushTimeout)
	defer timeout.Stop()

	// Periodically check recovery-owned data progress until target time tick is reached.
	var dataCP *utility.WALCheckpoint
	for dataCP == nil || dataCP.TimeTick < targetTimeTick {
		select {
		case <-ticker.C:
			dataCP = rs.GetDataCheckpoint(ctx)
			if dataCP == nil {
				log.Ctx(ctx).Info("waiting for recovery data checkpoint initialization")
				continue
			}
			if dataCP.TimeTick >= targetTimeTick {
				log.Ctx(ctx).Info("recovery data checkpoint reached, ready for WAL switch",
					zap.String("channel", opt.Channel.Name),
					zap.Uint64("dataCheckpointTS", dataCP.TimeTick),
					zap.Uint64("targetTimeTick", targetTimeTick),
					zap.Stringer("targetWAL", targetWALName))
				break
			}
			remaining := targetTimeTick - dataCP.TimeTick
			log.Ctx(ctx).Info("recovery data checkpoint in progress",
				zap.String("channel", opt.Channel.Name),
				zap.Uint64("currentTS", dataCP.TimeTick),
				zap.Uint64("targetTS", targetTimeTick),
				zap.Uint64("remainingTS", remaining))
		case <-timeout.C:
			log.Ctx(ctx).Warn("timeout waiting for flush completion",
				zap.String("channel", opt.Channel.Name),
				zap.Duration("timeout", walSwitchFlushTimeout))
			return status.NewInner("timeout waiting for flush completion during WAL switch")
		case <-ctx.Done():
			log.Ctx(ctx).Warn("context canceled while waiting for flush completion",
				zap.String("channel", opt.Channel.Name),
				zap.Error(ctx.Err()))
			return errors.Wrap(ctx.Err(), "context canceled during WAL switch flush waiting")
		}
	}

	// Close recovery storage and related resources to persist final state.
	log.Ctx(ctx).Info("closing recovery storage to persist WAL switch snapshot")
	if param.RecoveryStorage == nil {
		param.RecoveryStorage = rs
	}
	param.Clear()
	roWAL.Close()

	// Update checkpoint stage to ADVANCE_CHECKPOINT and persist to catalog
	snapshot.Checkpoint.DataCheckpoint = &utility.WALConsumeCheckpoint{
		MessageID: dataCP.MessageID,
		TimeTick:  dataCP.TimeTick,
	}
	snapshot.Checkpoint.AlterWalState.Stage = streamingpb.AlterWALStage_ADVANCE_CHECKPOINT

	catalog := resource.Resource().StreamingNodeCatalog()
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, snapshot.Checkpoint.IntoProto()); err != nil {
		log.Ctx(ctx).Warn("failed to persist checkpoint after flushing stage",
			zap.String("channel", opt.Channel.Name),
			zap.Error(err))
		return errors.Wrap(err, "failed to persist checkpoint after flushing stage")
	}

	log.Ctx(ctx).Info("checkpoint stage updated to ADVANCE_CHECKPOINT",
		zap.String("channel", opt.Channel.Name),
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTS", snapshot.Checkpoint.TimeTick))
	return nil
}

func (o *openerAdaptorImpl) handleAlterWALAdvanceCheckpointsStage(ctx context.Context, opt *wal.OpenOption, snapshot *recovery.RecoverySnapshot) error {
	// Update all vchannel checkpoints to new WAL initial position, then update pchannel checkpoint
	catalog := resource.Resource().StreamingNodeCatalog()
	vchannels, err := catalog.ListVChannel(ctx, opt.Channel.Name)
	if err != nil {
		return errors.Wrap(err, "failed to list vchannels")
	}

	// Build new WAL initial position
	newWALInitialTimeTick := snapshot.Checkpoint.TimeTick
	newWALInitialMsgID, newWALName := msgadaptor.MustGetEarliestMessageIDFromMQType(snapshot.Checkpoint.AlterWalState.TargetWalName)

	if len(vchannels) > 0 {
		// Get MixCoordClient to update vchannel checkpoints
		mixCoordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get mix coord client")
		}

		// Build checkpoint positions for all vchannels
		channelCheckpoints := make([]*msgpb.MsgPosition, 0, len(vchannels))
		for _, vchannel := range vchannels {
			msgIDBytes := newWALInitialMsgID.Serialize()
			vChannelName := vchannel.Vchannel
			pos := &msgpb.MsgPosition{
				ChannelName: vChannelName,
				MsgID:       msgIDBytes,
				Timestamp:   newWALInitialTimeTick,
				WALName:     newWALName,
			}
			channelCheckpoints = append(channelCheckpoints, pos)
		}

		// Batch update all vchannel checkpoints to DataCoord
		req := &datapb.UpdateChannelCheckpointRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ChannelCheckpoints: channelCheckpoints,
		}

		resp, err := mixCoordClient.UpdateChannelCheckpoint(ctx, req)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			log.Ctx(ctx).Warn("failed to update vchannel checkpoints",
				zap.String("channel", opt.Channel.Name),
				zap.Int("vchannelCount", len(channelCheckpoints)),
				zap.Error(err))
			return errors.Wrap(err, "failed to update vchannel checkpoints")
		}

		log.Ctx(ctx).Info("vchannel checkpoints updated to new WAL initial position",
			zap.String("channel", opt.Channel.Name),
			zap.Int("vchannelCount", len(channelCheckpoints)),
			zap.Uint64("newWALInitialTS", newWALInitialTimeTick))

		// Verify checkpoint updates
		for _, vchannel := range vchannels {
			resp2, err2 := mixCoordClient.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel.Vchannel})
			if err2 != nil {
				log.Ctx(ctx).Warn("failed to verify vchannel checkpoint update",
					zap.String("vchannel", vchannel.Vchannel),
					zap.Error(err2))
				return errors.Wrap(err2, "failed to verify vchannel checkpoint update")
			}
			log.Ctx(ctx).Info("verified vchannel checkpoint update",
				zap.String("vchannel", vchannel.Vchannel),
				zap.Binary("seekPositionMsgID", resp2.Info.SeekPosition.MsgID))
		}
	} else {
		log.Ctx(ctx).Info("no vchannels found, skipping vchannel checkpoint update",
			zap.String("channel", opt.Channel.Name))
	}

	// Update pchannel checkpoint: reset alterWALState and set position to new WAL initial position
	finalCheckpoint := snapshot.Checkpoint.Clone()
	finalCheckpoint.AlterWalState = nil
	finalCheckpoint.MessageID = msgadaptor.MustGetMessageIDFromMQWrapperID(newWALInitialMsgID)
	finalCheckpoint.DataCheckpoint = &utility.WALConsumeCheckpoint{
		MessageID: finalCheckpoint.MessageID,
		TimeTick:  finalCheckpoint.TimeTick,
	}
	if finalCheckpoint.ReplicateCheckpoint != nil {
		finalCheckpoint.ReplicateCheckpoint.MessageID = finalCheckpoint.MessageID
	}

	// Persist final checkpoint to catalog
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, finalCheckpoint.IntoProto()); err != nil {
		log.Ctx(ctx).Warn("failed to persist checkpoint after advance checkpoint stage",
			zap.String("channel", opt.Channel.Name),
			zap.Error(err))
		return errors.Wrap(err, "failed to persist checkpoint after advance checkpoint stage")
	}

	// Register default WAL name for delegator to track seek position changes
	message.RegisterDefaultWALName(finalCheckpoint.MessageID.WALName())

	log.Ctx(ctx).Info("pchannel checkpoint updated to new WAL initial position",
		zap.String("channel", opt.Channel.Name),
		zap.String("newCheckpoint", finalCheckpoint.MessageID.String()),
		zap.String("newWAL", finalCheckpoint.MessageID.WALName().String()),
		zap.Uint64("newCheckpointTS", finalCheckpoint.TimeTick))

	return nil
}

// openROWAL opens a read only wal instance for the channel.
func (o *openerAdaptorImpl) openROWAL(l walimpls.WALImpls) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	wal := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerAdaptorImpl) Close() {
	o.lifetime.SetState(typeutil.LifetimeStateStopped)
	o.lifetime.Wait()

	o.Logger().Info("wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info("close wal by opener", zap.Int64("id", id), zap.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info("closing underlying walimpls opener", zap.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info("wal opener closed")
}
