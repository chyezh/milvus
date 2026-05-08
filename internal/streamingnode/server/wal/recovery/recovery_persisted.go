package recovery

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recoverRecoveryInfoFromMeta retrieves the recovery info for the given channel.
func (r *recoveryStorageImpl) recoverRecoveryInfoFromMeta(ctx context.Context, channelInfo types.PChannelInfo, lastTimeTickMessage message.ImmutableMessage) error {
	r.metrics.ObserveStateChange(recoveryStorageStatePersistRecovering)
	r.SetLogger(resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", channelInfo.String()),
		zap.String("state", recoveryStorageStatePersistRecovering),
	))

	catalog := resource.Resource().StreamingNodeCatalog()
	var segmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	if r.checkpoint == nil {
		// There's no checkpoint for current pchannel, so we need to initialize the recover info.
		cpProto, err := r.initializeRecoverInfo(ctx, channelInfo, lastTimeTickMessage)
		if err != nil {
			return errors.Wrap(err, "failed to initialize checkpoint")
		}
		r.checkpoint = utility.NewWALCheckpointFromProto(cpProto)
	}
	r.Logger().Info("recover checkpoint done",
		zap.String("checkpoint", r.checkpoint.MetaCheckpoint.MessageID.String()),
		zap.Uint64("timetick", r.checkpoint.MetaCheckpoint.TimeTick),
		zap.Int64("magic", r.checkpoint.Magic),
	)

	fVChannel := conc.Go(func() (struct{}, error) {
		var err error
		vchannels, err := catalog.ListVChannel(ctx, channelInfo.Name)
		if err != nil {
			return struct{}{}, errors.Wrap(err, "failed to get vchannel from catalog")
		}
		r.vchannels = newVChannelRecoveryInfoFromVChannelMeta(vchannels)
		r.Logger().Info("recovery vchannel info done", zap.Int("vchannels", len(r.vchannels)))
		return struct{}{}, nil
	})

	fSegment := conc.Go(func() (struct{}, error) {
		var err error
		segmentAssign, err := catalog.ListSegmentAssignment(ctx, channelInfo.Name)
		if err != nil {
			return struct{}{}, errors.Wrap(err, "failed to get segment assignment from catalog")
		}
		segmentAssignments = make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segmentAssign))
		for _, assignment := range segmentAssign {
			segmentAssignments[assignment.GetSegmentId()] = assignment
		}
		r.Logger().Info("recover segment info done", zap.Int("segments", len(segmentAssignments)))
		return struct{}{}, nil
	})
	if err := conc.BlockOnAll(fVChannel, fSegment); err != nil {
		return err
	}
	if r.checkpoint.DataCheckpoint == nil {
		dataCheckpoint, err := r.recoverDataCheckpointFromCoordinator(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to recover data checkpoint from coordinator")
		}
		r.checkpoint.DataCheckpoint = dataCheckpoint
		if err := resource.Resource().StreamingNodeCatalog().SaveConsumeCheckpoint(ctx, channelInfo.Name, r.checkpoint.IntoProto()); err != nil {
			return errors.Wrap(err, "failed to save recovered data checkpoint")
		}
		r.Logger().Info("recover data checkpoint from coordinator done",
			zap.String("checkpoint", dataCheckpoint.MessageID.String()),
			zap.Uint64("timetick", dataCheckpoint.TimeTick))
	}
	r.observedCheckpoint = cloneWALCheckpoint(r.checkpoint)
	r.segmentManager.RecoverFromSnapshotWithSchema(ctx, segmentAssignments, r)
	return nil
}

func (r *recoveryStorageImpl) recoverDataCheckpointFromCoordinator(ctx context.Context) (*utility.Checkpoint, error) {
	if len(r.vchannels) == 0 {
		return &utility.Checkpoint{
			MessageID: r.checkpoint.MetaCheckpoint.MessageID,
			TimeTick:  r.checkpoint.MetaCheckpoint.TimeTick,
		}, nil
	}
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get mix coord client")
	}
	var minimum *utility.Checkpoint
	for vchannel := range r.vchannels {
		resp, err := coord.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{
			Vchannel: vchannel,
		})
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return nil, errors.Wrapf(err, "failed to get channel recovery info for %s", vchannel)
		}
		position := resp.GetInfo().GetSeekPosition()
		if position == nil {
			return nil, errors.Errorf("coordinator returned nil seek position for %s", vchannel)
		}
		checkpoint, err := r.newDataCheckpointFromPosition(position)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert seek position for %s", vchannel)
		}
		if minimum == nil || dataCheckpointLess(checkpoint, minimum) {
			minimum = checkpoint
		}
	}
	if minimum == nil {
		return nil, errors.New("coordinator returned no data checkpoint")
	}
	return minimum, nil
}

func (r *recoveryStorageImpl) newDataCheckpointFromPosition(position *msgpb.MsgPosition) (*utility.Checkpoint, error) {
	if len(position.GetMsgID()) == 0 {
		return nil, errors.New("data checkpoint position has empty message id")
	}
	walName := message.WALName(position.GetWALName())
	if walName == message.WALNameUnknown && r.checkpoint != nil && r.checkpoint.MetaCheckpoint.MessageID != nil {
		walName = r.checkpoint.MetaCheckpoint.MessageID.WALName()
	}
	return &utility.Checkpoint{
		MessageID: msgadaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(walName, position.GetMsgID()),
		TimeTick:  position.GetTimestamp(),
	}, nil
}

func dataCheckpointLess(left, right *utility.Checkpoint) bool {
	if left.TimeTick != right.TimeTick {
		return left.TimeTick < right.TimeTick
	}
	if left.MessageID == nil || right.MessageID == nil {
		return right.MessageID != nil
	}
	return left.MessageID.LT(right.MessageID)
}

// initializeRecoverInfo initializes the recover info for the given channel.
// before first streaming service is enabled, there's no recovery info for channel.
// we should initialize the recover info for the channel.
// !!! This function will only call once for each channel when the streaming service is enabled.
func (r *recoveryStorageImpl) initializeRecoverInfo(ctx context.Context, channelInfo types.PChannelInfo, untilMessage message.ImmutableMessage) (*streamingpb.WALCheckpoint, error) {
	// The message that is not generated by the streaming service is not managed by the recovery storage at streamingnode.
	// So we ignore it, just use the global milvus metainfo to initialize the recovery storage.
	// !!! It's not a strong guarantee that keep the consistency of old arch and new arch.
	r.Logger().Info("checkpoint not found in catalog, may upgrading from old arch, initializing it...", log.FieldMessage(untilMessage))

	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "when wait for rootcoord client ready")
	}
	resp, err := coord.GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
		Pchannel: channelInfo.Name,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel info from rootcoord")
	}
	schemas, err := r.fetchLatestSchemaFromCoord(ctx, resp)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch latest schema from coord")
	}

	// save the vchannel recovery info into the catalog
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(resp.GetCollections()))
	for _, collection := range resp.GetCollections() {
		if collection.State == etcdpb.CollectionState_CollectionDropping {
			// Drop the already dropping collection before streaming arch enabled.
			// Otherwise, the dropping collection message will be lost,
			// and the data of collection can not be dropped.
			coordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
			if err != nil {
				return nil, err
			}
			resp, err := coordClient.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{
				Base:        commonpbutil.NewMsgBase(commonpbutil.WithSourceID(paramtable.GetNodeID())),
				ChannelName: collection.Vchannel,
			})
			if err = merr.CheckRPCCall(resp, err); err != nil {
				return nil, errors.Wrap(err, "failed to drop virtual channel")
			}
			continue
		}
		partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(collection.Partitions))
		for _, partition := range collection.Partitions {
			partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{PartitionId: partition.PartitionId})
		}
		if schemas[collection.CollectionId] == nil {
			panic(fmt.Sprintf("schema not found for collection, %d", collection.CollectionId))
		}
		vchannels[collection.Vchannel] = &streamingpb.VChannelMeta{
			Vchannel: collection.Vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collection.CollectionId,
				Partitions:   partitions,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             schemas[collection.CollectionId].Schema,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 0, // The recovery info from old arch should be set as zero.
						// because we don't have the version before streaming service is enabled.
						// all message will happen after the recovery info is initialized.
					},
				},
			},
			CheckpointTimeTick: 0, // same as schema above.
		}
	}

	// SaveVChannels saves the vchannels into the catalog.
	if err := resource.Resource().StreamingNodeCatalog().SaveVChannels(ctx, channelInfo.Name, vchannels); err != nil {
		return nil, errors.Wrap(err, "failed to save vchannels to catalog")
	}
	// Use the first timesync message as the initial checkpoint.
	checkpoint := &streamingpb.WALCheckpoint{
		MetaMessageId: untilMessage.LastConfirmedMessageID().IntoProto(),
		MetaTimeTick:  untilMessage.TimeTick(),
		RecoveryMagic: utility.RecoveryMagicStreamingInitialized,
	}
	if err := resource.Resource().StreamingNodeCatalog().SaveConsumeCheckpoint(ctx, channelInfo.Name, checkpoint); err != nil {
		return nil, errors.Wrap(err, "failed to save checkpoint to catalog")
	}
	r.Logger().Info("initialize checkpoint done",
		zap.Int("vchannels", len(vchannels)),
		zap.String("checkpoint", checkpoint.MetaMessageId.String()),
		zap.Uint64("timetick", checkpoint.MetaTimeTick),
		zap.Int64("magic", checkpoint.RecoveryMagic),
		zap.Any("alterWALState", checkpoint.AlterWalState),
	)
	return checkpoint, nil
}

// fetchLatestSchemaFromCoord fetches the latest schema from coord.
func (r *recoveryStorageImpl) fetchLatestSchemaFromCoord(ctx context.Context, resp *rootcoordpb.GetPChannelInfoResponse) (map[int64]*streamingpb.CollectionSchemaOfVChannel, error) {
	rc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get coord client")
	}

	futures := make([]*conc.Future[*milvuspb.DescribeCollectionResponse], 0, len(resp.GetCollections()))
	for _, collection := range resp.GetCollections() {
		if collection.State == etcdpb.CollectionState_CollectionDropping {
			continue
		}
		future := conc.Go(func() (*milvuspb.DescribeCollectionResponse, error) {
			resp, err := rc.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionID: collection.CollectionId,
			})
			if err = merr.CheckRPCCall(resp, err); err != nil {
				return nil, errors.Wrap(err, "failed to describe collection")
			}
			return resp, nil
		})
		futures = append(futures, future)
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		return nil, errors.Wrap(err, "failed to describe collection")
	}

	schemas := make(map[int64]*streamingpb.CollectionSchemaOfVChannel, len(futures))
	for _, future := range futures {
		resp := future.Value()
		collectionID := resp.CollectionID
		schemas[collectionID] = &streamingpb.CollectionSchemaOfVChannel{
			Schema: resp.Schema,
		}
	}
	return schemas, nil
}
