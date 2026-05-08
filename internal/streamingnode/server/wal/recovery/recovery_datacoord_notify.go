package recovery

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (rs *recoveryStorageImpl) notifyDataCoordSaveBinlogPaths(ctx context.Context, snapshot *RecoverySnapshot) error {
	requests := buildSaveBinlogPathRequests(snapshot)
	if len(requests) == 0 {
		return nil
	}

	mixCoordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}
	for _, req := range requests {
		if err := rs.retryOperationWithBackoff(ctx,
			rs.Logger().With(
				zap.Int64("segmentID", req.GetSegmentID()),
				zap.Int64("collectionID", req.GetCollectionID()),
				zap.String("vchannel", req.GetChannel()),
				zap.String("op", "notifyDataCoordSaveBinlogPaths")),
			func(ctx context.Context) error {
				resp, err := mixCoordClient.SaveBinlogPaths(ctx, req)
				return merr.CheckRPCCall(resp, err)
			}); err != nil {
			return err
		}
	}
	return nil
}

func buildSaveBinlogPathRequests(snapshot *RecoverySnapshot) []*datapb.SaveBinlogPathsRequest {
	if snapshot == nil || len(snapshot.SegmentAssignments) == 0 {
		return nil
	}
	requests := make([]*datapb.SaveBinlogPathsRequest, 0, len(snapshot.SegmentAssignments))
	for _, meta := range snapshot.SegmentAssignments {
		req := buildSaveBinlogPathRequest(meta)
		if req != nil {
			requests = append(requests, req)
		}
	}
	return requests
}

func buildSaveBinlogPathRequest(meta *streamingpb.SegmentAssignmentMeta) *datapb.SaveBinlogPathsRequest {
	switch storage := meta.GetPersistedStorage().(type) {
	case *streamingpb.SegmentAssignmentMeta_L1:
		fieldBinlogs, statsBinlogs, bm25Binlogs := flattenL1Binlogs(storage.L1)
		if mergedStats := storage.L1.GetMergedStatsBinlog(); mergedStats != nil {
			statsBinlogs = append(statsBinlogs, proto.Clone(mergedStats).(*datapb.FieldBinlog))
		}
		if len(fieldBinlogs) == 0 &&
			len(statsBinlogs) == 0 &&
			len(bm25Binlogs) == 0 &&
			meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
			return nil
		}
		return newSaveBinlogPathRequest(meta, datapb.SegmentLevel_L1, fieldBinlogs, statsBinlogs, nil, bm25Binlogs, storage.L1.GetManifestPath())
	case *streamingpb.SegmentAssignmentMeta_L0:
		deltalogs := cloneFieldBinlogs(storage.L0.GetDeltaBinlog())
		if len(deltalogs) == 0 &&
			meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
			return nil
		}
		return newSaveBinlogPathRequest(meta, datapb.SegmentLevel_L0, nil, nil, deltalogs, nil, "")
	default:
		return nil
	}
}

func flattenL1Binlogs(storage *streamingpb.L1SegmentPersistedStorage) (
	fieldBinlogs []*datapb.FieldBinlog,
	statsBinlogs []*datapb.FieldBinlog,
	bm25Binlogs []*datapb.FieldBinlog,
) {
	if storage == nil {
		return nil, nil, nil
	}
	for _, binlogs := range storage.GetBinlogs() {
		fieldBinlogs = append(fieldBinlogs, cloneFieldBinlogs(binlogs.GetFieldBinlog())...)
		statsBinlogs = append(statsBinlogs, cloneFieldBinlogs(binlogs.GetStatsBinlog())...)
		bm25Binlogs = append(bm25Binlogs, cloneFieldBinlogs(binlogs.GetBm25Binlog())...)
	}
	return fieldBinlogs, statsBinlogs, bm25Binlogs
}

func newSaveBinlogPathRequest(
	meta *streamingpb.SegmentAssignmentMeta,
	level datapb.SegmentLevel,
	fieldBinlogs []*datapb.FieldBinlog,
	statsBinlogs []*datapb.FieldBinlog,
	deltalogs []*datapb.FieldBinlog,
	bm25Binlogs []*datapb.FieldBinlog,
	manifestPath string,
) *datapb.SaveBinlogPathsRequest {
	return &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentID:           meta.GetSegmentId(),
		CollectionID:        meta.GetCollectionId(),
		PartitionID:         meta.GetPartitionId(),
		Field2BinlogPaths:   fieldBinlogs,
		Field2StatslogPaths: statsBinlogs,
		Field2Bm25LogPaths:  bm25Binlogs,
		Deltalogs:           deltalogs,
		CheckPoints: []*datapb.CheckPoint{{
			SegmentID: meta.GetSegmentId(),
			NumOfRows: int64(meta.GetStat().GetModifiedRows()),
			Position: &msgpb.MsgPosition{
				ChannelName: meta.GetVchannel(),
				Timestamp:   meta.GetCheckpointTimeTick(),
			},
		}},
		Flushed:         meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		Channel:         meta.GetVchannel(),
		SegLevel:        level,
		StorageVersion:  meta.GetStorageVersion(),
		WithFullBinlogs: true,
		ManifestPath:    manifestPath,
	}
}

func cloneFieldBinlogs(in []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(in) == 0 {
		return nil
	}
	out := make([]*datapb.FieldBinlog, 0, len(in))
	for _, binlog := range in {
		out = append(out, proto.Clone(binlog).(*datapb.FieldBinlog))
	}
	return out
}
