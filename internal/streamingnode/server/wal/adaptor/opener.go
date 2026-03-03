package adaptor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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


	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(ctx, walName)
	if err != nil {
		log.Warn(ctx, "get or create underlying wal impls opener failed", log.Err(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		log.Warn(ctx, "open wal impls failed", log.Err(err))
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
		log.Warn(ctx, "open wal failed", log.Err(err))
		return nil, err
	}
	log.Info(ctx, "open wal done", log.Stringer("walName", walName), log.String("pchannel", opt.Channel.Name))
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
		log.Info(ctx, "get checkpoint from catalog",
			log.String("channel", opt.Channel.Name),
			log.Stringer("checkpoint", checkpoint.MessageID),
			log.Uint64("checkpointTimeTick", checkpoint.TimeTick),
			log.Stringer("currentWAL", checkpoint.MessageID.WALName()),
			log.Any("AlterWalState", checkpoint.AlterWalState))
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
	log.Info(ctx, "created and cached new walimpls opener", log.Stringer("walName", walName))
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
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), cp, param.LastTimeTickMessage)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}

	// Handle alter WAL if found in snapshot
	// This flushes all remaining data and triggers WAL switch to the target implementation
	if snapshot.AlterWALInfo != nil && snapshot.AlterWALInfo.FoundAlterWALMsg {
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
	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
		},
	); err != nil {
		return nil, err
	}

	// start the flusher to flush and generate recovery info.
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              param.WAL,
			RecoveryStorage:  rs,
			ChannelInfo:      l.Channel(),
			RecoverySnapshot: snapshot,
		})
	}
	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param, flusher)
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
	log.Info(ctx, "detected alter WAL message in snapshot",
		log.String("channel", opt.Channel.String()),
		log.Bool("foundAlterWAL", snapshot.AlterWALInfo.FoundAlterWALMsg),
		log.Stringer("targetWAL", snapshot.AlterWALInfo.TargetWALName),
		log.String("checkpointMessageID", snapshot.Checkpoint.MessageID.String()),
		log.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		log.Any("alterWALConfig", snapshot.AlterWALInfo.AlterWALConfig))

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
	return nil, errors.Errorf("WAL switch success: %s switch to %s finish, re-opening required", opt.Channel.Name, targetWALName)
}

func (o *openerAdaptorImpl) handleAlterWALFlushingStage(ctx context.Context, opt *wal.OpenOption, roWAL *roWALAdaptorImpl,
	param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot,
) error {
	// Start flusher to flush all growing segments
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		f := syncutil.NewFuture[wal.WAL]()
		f.Set(roWAL)
		roWAL.ForceRecovery(true)
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              f,
			RecoveryStorage:  rs,
			ChannelInfo:      roWAL.Channel(),
			RecoverySnapshot: snapshot,
		})
	}

	// Wait for all data up to target time tick to be flushed
	targetTimeTick := snapshot.AlterWALInfo.AlterWALTs
	targetWALName := snapshot.AlterWALInfo.TargetWALName
	log.Info(ctx, "waiting for flush completion before WAL switch",
		log.String("channel", opt.Channel.Name),
		log.Uint64("targetTimeTick", targetTimeTick))

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	const defaultWALSwitchFlushTimeout = 1 * time.Minute

	// Periodically check flush progress until target time tick is reached
	var flusherCP *utility.WALCheckpoint
	for flusherCP == nil || flusherCP.TimeTick < targetTimeTick {
		select {
		case <-ticker.C:
			flusherCP = rs.GetFlusherCheckpointByTimeTick(ctx)
			if flusherCP == nil {
				log.Info(ctx, "waiting for flusher checkpoint initialization")
				continue
			}
			if flusherCP.TimeTick >= targetTimeTick {
				log.Info(ctx, "flush completed, ready for WAL switch",
					log.String("channel", opt.Channel.Name),
					log.Uint64("flusherCheckpointTS", flusherCP.TimeTick),
					log.Uint64("targetTimeTick", targetTimeTick),
					log.Stringer("targetWAL", targetWALName))
				break
			}
			remaining := targetTimeTick - flusherCP.TimeTick
			log.Info(ctx, "flush in progress",
				log.String("channel", opt.Channel.Name),
				log.Uint64("currentTS", flusherCP.TimeTick),
				log.Uint64("targetTS", targetTimeTick),
				log.Uint64("remainingTS", remaining))
		case <-time.After(defaultWALSwitchFlushTimeout):
			log.Warn(ctx, "timeout waiting for flush completion",
				log.String("channel", opt.Channel.Name),
				log.Duration("timeout", defaultWALSwitchFlushTimeout))
			return errors.Newf("timeout waiting for flush completion during WAL switch")
		case <-ctx.Done():
			log.Warn(ctx, "context canceled while waiting for flush completion",
				log.String("channel", opt.Channel.Name),
				log.Err(ctx.Err()))
			return errors.Wrap(ctx.Err(), "context canceled during WAL switch flush waiting")
		}
	}

	// Close recovery storage and related resources to persist final state
	log.Info(ctx, "closing recovery storage to persist WAL switch snapshot")
	rs.Close()
	if flusher != nil {
		flusher.Close()
	}
	param.Clear()
	roWAL.Close()

	// Update checkpoint stage to ADVANCE_CHECKPOINT and persist to catalog
	snapshot.Checkpoint.AlterWalState.Stage = streamingpb.AlterWALStage_ADVANCE_CHECKPOINT

	catalog := resource.Resource().StreamingNodeCatalog()
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, snapshot.Checkpoint.IntoProto()); err != nil {
		log.Warn(ctx, "failed to persist checkpoint after flushing stage",
			log.String("channel", opt.Channel.Name),
			log.Err(err))
		return errors.Wrap(err, "failed to persist checkpoint after flushing stage")
	}

	log.Info(ctx, "checkpoint stage updated to ADVANCE_CHECKPOINT",
		log.String("channel", opt.Channel.Name),
		log.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		log.Uint64("checkpointTS", snapshot.Checkpoint.TimeTick))
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
			log.Warn(ctx, "failed to update vchannel checkpoints",
				log.String("channel", opt.Channel.Name),
				log.Int("vchannelCount", len(channelCheckpoints)),
				log.Err(err))
			return errors.Wrap(err, "failed to update vchannel checkpoints")
		}

		log.Info(ctx, "vchannel checkpoints updated to new WAL initial position",
			log.String("channel", opt.Channel.Name),
			log.Int("vchannelCount", len(channelCheckpoints)),
			log.Uint64("newWALInitialTS", newWALInitialTimeTick))

		// Verify checkpoint updates
		for _, vchannel := range vchannels {
			resp2, err2 := mixCoordClient.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel.Vchannel})
			if err2 != nil {
				log.Warn(ctx, "failed to verify vchannel checkpoint update",
					log.String("vchannel", vchannel.Vchannel),
					log.Err(err2))
				return errors.Wrap(err2, "failed to verify vchannel checkpoint update")
			}
			log.Info(ctx, "verified vchannel checkpoint update",
				log.String("vchannel", vchannel.Vchannel),
				log.Binary("seekPositionMsgID", resp2.Info.SeekPosition.MsgID))
		}
	} else {
		log.Info(ctx, "no vchannels found, skipping vchannel checkpoint update",
			log.String("channel", opt.Channel.Name))
	}

	// Update pchannel checkpoint: reset alterWALState and set position to new WAL initial position
	finalCheckpoint := snapshot.Checkpoint.Clone()
	finalCheckpoint.AlterWalState = nil
	finalCheckpoint.MessageID = msgadaptor.MustGetMessageIDFromMQWrapperID(newWALInitialMsgID)
	if finalCheckpoint.ReplicateCheckpoint != nil {
		finalCheckpoint.ReplicateCheckpoint.MessageID = finalCheckpoint.MessageID
	}

	// Persist final checkpoint to catalog
	if err := catalog.SaveConsumeCheckpoint(ctx, opt.Channel.Name, finalCheckpoint.IntoProto()); err != nil {
		log.Warn(ctx, "failed to persist checkpoint after advance checkpoint stage",
			log.String("channel", opt.Channel.Name),
			log.Err(err))
		return errors.Wrap(err, "failed to persist checkpoint after advance checkpoint stage")
	}

	// Register default WAL name for delegator to track seek position changes
	message.RegisterDefaultWALName(finalCheckpoint.MessageID.WALName())

	log.Info(ctx, "pchannel checkpoint updated to new WAL initial position",
		log.String("channel", opt.Channel.Name),
		log.String("newCheckpoint", finalCheckpoint.MessageID.String()),
		log.String("newWAL", finalCheckpoint.MessageID.WALName().String()),
		log.Uint64("newCheckpointTS", finalCheckpoint.TimeTick))

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

	o.Logger().Info(nil, "wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info(nil, "close wal by opener", log.Int64("id", id), log.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info(nil, "closing underlying walimpls opener", log.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info(nil, "wal opener closed")
}
