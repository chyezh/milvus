package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/pkg/errors"
)

// GetRecoveryInfo retrieves the recovery info for the given channel.
func GetRecoveryInfo(ctx context.Context, channelInfo types.PChannelInfo) (*RecoverInfos, error) {
	catalog := resource.Resource().StreamingNodeCatalog()
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	if checkpoint == nil {
		// There's no checkpoint for current pchannel, so we need to initialize the recover info.
		if checkpoint, err = initializeRecoverInfo(ctx, channelInfo); err != nil {
			return nil, err
		}
	}
	vchannels, err := catalog.ListVChannel(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	segmentAssign, err := catalog.ListSegmentAssignment(ctx, channelInfo.Name)
	if err != nil {
		return nil, err
	}
	return &RecoverInfos{
		VChannels:          vchannels,
		SegmentAssignments: segmentAssign,
		Checkpoint:         checkpoint,
	}, nil
}

// initializeRecoverInfo initializes the recover info for the given channel.
// before first streaming service is enabled, there's no recovery info for channel.
// we should initialize the recover info for the channel.
func initializeRecoverInfo(ctx context.Context, channelInfo types.PChannelInfo) (*streamingpb.WALCheckpoint, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "when wait for rootcoord client ready")
	}
	resp, err := coord.GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
		Pchannel: channelInfo.Name,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	// save the vchannel recovery info into the catalog
	vchannels := make([]*streamingpb.VChannelMeta, 0, len(resp.GetCollections()))
	for _, collection := range resp.GetCollections() {
		partitions := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(collection.Partitions))
		for _, partition := range collection.Partitions {
			partitions = append(partitions, &streamingpb.PartitionInfoOfVChannel{PartitionId: partition.PartitionId})
		}
		vchannel := &streamingpb.VChannelMeta{
			Vchannel: collection.Vchannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: collection.CollectionId,
				Partitions:   partitions,
			},
		}
		vchannels = append(vchannels, vchannel)
	}

	// SaveVChannels saves the vchannels into the catalog.
	if err := resource.Resource().StreamingNodeCatalog().SaveVChannels(ctx, channelInfo.Name, vchannels); err != nil {
		return nil, err
	}
	checkpoint := &streamingpb.WALCheckpoint{
		MessageID:            nil,
		WriteAheadCheckpoint: nil,
		RecoveryMagic:        recoveryMagicStreamingInitialized,
	}
	if err := resource.Resource().StreamingNodeCatalog().SaveConsumeCheckpoint(ctx, channelInfo.Name, &streamingpb.WALCheckpoint{
		MessageID:            nil,
		WriteAheadCheckpoint: nil,
		RecoveryMagic:        recoveryMagicStreamingInitialized,
	}); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

// RecoverInfos is a helper for recovering the status of the WAL.
type RecoverInfos struct {
	VChannels          []*streamingpb.VChannelMeta
	SegmentAssignments []*streamingpb.SegmentAssignmentMeta
	Checkpoint         *streamingpb.WALCheckpoint
}
