package broker

import (
	"context"
	"math"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type dataCoordBroker struct {
	client   types.MixCoordClient
	serverID int64
}

func (dc *dataCoordBroker) AssignSegmentID(ctx context.Context, reqs ...*datapb.SegmentIDRequest) ([]typeutil.UniqueID, error) {
	req := &datapb.AssignSegmentIDRequest{
		NodeID:            dc.serverID,
		PeerRole:          typeutil.ProxyRole,
		SegmentIDRequests: reqs,
	}

	resp, err := dc.client.AssignSegmentID(ctx, req)

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to call datacoord AssignSegmentID", log.Err(err))
		return nil, err
	}

	return lo.Map(resp.GetSegIDAssignments(), func(result *datapb.SegmentIDAssignment, _ int) typeutil.UniqueID {
		return result.GetSegID()
	}), nil
}

func (dc *dataCoordBroker) ReportTimeTick(ctx context.Context, msgs []*msgpb.DataNodeTtMsg) error {

	req := &datapb.ReportDataNodeTtMsgsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
			commonpbutil.WithSourceID(dc.serverID),
		),
		Msgs: msgs,
	}

	resp, err := dc.client.ReportDataNodeTtMsgs(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to report datanodeTtMsgs", log.Err(err))
		return err
	}
	return nil
}

func (dc *dataCoordBroker) GetSegmentInfo(ctx context.Context, ids []int64) ([]*datapb.SegmentInfo, error) {
	getSegmentInfo := func(ids []int64) (*datapb.GetSegmentInfoResponse, error) {
		ctx, cancel := context.WithTimeout(ctx, paramtable.Get().DataCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
		defer cancel()

		infoResp, err := dc.client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
				commonpbutil.WithSourceID(dc.serverID),
			),
			SegmentIDs:       ids,
			IncludeUnHealthy: true,
		})
		if err := merr.CheckRPCCall(infoResp, err); err != nil {
			log.Warn(ctx, "Fail to get SegmentInfo by ids from datacoord", log.Int64s("segments", ids), log.Err(err))
			return nil, err
		}

		err = binlog.DecompressMultiBinLogs(infoResp.GetInfos())
		if err != nil {
			log.Warn(ctx, "Fail to DecompressMultiBinLogs", log.Int64s("segments", ids), log.Err(err))
			return nil, err
		}
		return infoResp, nil
	}

	ret := make([]*datapb.SegmentInfo, 0, len(ids))
	batchSize := 1000
	startIdx := 0
	for startIdx < len(ids) {
		endIdx := int(math.Min(float64(startIdx+batchSize), float64(len(ids))))

		resp, err := getSegmentInfo(ids[startIdx:endIdx])
		if err != nil {
			log.Warn(ctx, "Fail to get SegmentInfo", log.Int("total segment num", len(ids)), log.Int("returned num", startIdx))
			return nil, err
		}
		ret = append(ret, resp.GetInfos()...)
		startIdx += batchSize
	}

	return ret, nil
}

func (dc *dataCoordBroker) UpdateChannelCheckpoint(ctx context.Context, channelCPs []*msgpb.MsgPosition) error {
	req := &datapb.UpdateChannelCheckpointRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(dc.serverID),
		),
		ChannelCheckpoints: channelCPs,
	}

	resp, err := dc.client.UpdateChannelCheckpoint(ctx, req)
	if err = merr.CheckRPCCall(resp, err); err != nil {
		channels := lo.Map(channelCPs, func(pos *msgpb.MsgPosition, _ int) string {
			return pos.GetChannelName()
		})
		channelTimes := lo.Map(channelCPs, func(pos *msgpb.MsgPosition, _ int) time.Time {
			return tsoutil.PhysicalTime(pos.GetTimestamp())
		})
		log.Warn(ctx, "failed to update channel checkpoint", log.Strings("channelNames", channels),
			log.Times("channelCheckpointTimes", channelTimes), log.Err(err))
		return err
	}
	return nil
}

func (dc *dataCoordBroker) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {

	resp, err := dc.client.SaveBinlogPaths(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to SaveBinlogPaths", log.Err(err))
		return err
	}

	return nil
}

func (dc *dataCoordBroker) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {

	resp, err := dc.client.DropVirtualChannel(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to DropVirtualChannel", log.Err(err))
		return resp, err
	}

	return resp, nil
}

func (dc *dataCoordBroker) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	resp, err := dc.client.ImportV2(ctx, in)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to ImportV2", log.Err(err))
		return resp, err
	}

	return resp, nil
}
