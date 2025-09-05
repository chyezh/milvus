package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func (c *Core) broadcastDropCollectionV1(ctx context.Context, req *milvuspb.DropCollectionRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewDatabaseNameResourceKey(req.GetDbName()),
		message.NewCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
	if err != nil {
		return err
	}

	dropCollectionTask := &dropCollectionTask{
		Core: c,
		Req:  req,
	}
	if err := dropCollectionTask.Prepare(ctx); err != nil {
		return err
	}

	channels := make([]string, 0, len(dropCollectionTask.vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for _, vchannel := range dropCollectionTask.vchannels {
		channels = append(channels, vchannel)
	}
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(dropCollectionTask.header).
		WithBody(dropCollectionTask.body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// dropCollectionV1AckCallback is called when the drop collection message is acknowledged
func (c *DDLCallback) dropCollectionV1AckCallback(ctx context.Context, msgs ...message.ImmutableDropCollectionMessageV1) error {
	for _, msg := range msgs {
		collectionID := msg.Header().CollectionId
		if funcutil.IsControlChannel(msg.VChannel()) {
			// when the control channel is acknowledged, we should do the following steps:
			if err := c.broker.ReleaseCollection(ctx, collectionID); err != nil {
				return err
			}
			if err := c.meta.DropCollection(ctx, collectionID, msg.TimeTick()); err != nil {
				return err
			}
			if err := c.broker.DropCollectionIndex(ctx, collectionID, nil); err != nil {
				return err
			}
			continue
		}
		// Drop virtual channel data when the vchannel is acknowledged.
		resp, err := c.mixCoord.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ChannelName: msg.VChannel(),
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
	}
	// cleanup the proxy cache.
	if err := c.Core.ExpireMetaCache(ctx,
		msgs[0].MustBody().DbName,
		[]string{msgs[0].MustBody().CollectionName},
		msgs[0].Header().CollectionId,
		"",
		msgs[0].TimeTick(),
		proxyutil.SetMsgType(commonpb.MsgType_DropCollection)); err != nil {
		return err
	}
	return nil
}
