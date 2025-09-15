package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(in.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetCollectionName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetAlias()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasCreatable(ctx, in.GetDbName(), in.GetAlias(), in.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, in.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewPutAliasMessageBuilderV2().
		WithHeader(&message.PutAliasMessageHeader{
			DbId:           db.ID,
			DbName:         in.GetDbName(),
			CollectionId:   collection.CollectionID,
			Alias:          in.GetAlias(),
			CollectionName: in.GetCollectionName(),
		}).
		WithBody(&message.PutAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastAlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(in.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetCollectionName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetAlias()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasAlterable(ctx, in.GetDbName(), in.GetAlias(), in.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, in.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewPutAliasMessageBuilderV2().
		WithHeader(&message.PutAliasMessageHeader{
			DbId:           db.ID,
			DbName:         in.GetDbName(),
			CollectionId:   collection.CollectionID,
			Alias:          in.GetAlias(),
			CollectionName: in.GetCollectionName(),
		}).
		WithBody(&message.PutAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) putAliasMessageV2AckCallback(ctx context.Context, result message.BroadcastResultPutAliasMessageV2) error {
	expiredCollNames := []string{result.Message.Header().Alias, result.Message.Header().CollectionName}
	// If the alias is already aliased to another collection, we need to expire the cache of the old collection.
	if coll, _ := c.meta.GetCollectionByName(ctx, result.Message.Header().DbName, result.Message.Header().Alias, typeutil.MaxTimestamp); coll != nil {
		expiredCollNames = append(expiredCollNames, coll.Name)
	}
	if err := c.meta.PutAlias(ctx, result); err != nil {
		return err
	}
	if err := c.ExpireMetaCache(
		ctx,
		result.Message.Header().DbName,
		expiredCollNames,
		0,
		"",
		result.GetControlChannelResult().TimeTick,
		proxyutil.SetMsgType(commonpb.MsgType_AlterAlias)); err != nil {
		return err
	}
	return nil
}

func (c *Core) broadcastDropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(in.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(in.GetDbName(), in.GetAlias()))
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	db, err := c.meta.GetDatabaseByName(ctx, in.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	msg := message.NewDropAliasMessageBuilderV2().
		WithHeader(&message.DropAliasMessageHeader{
			DbId:   db.ID,
			DbName: in.GetDbName(),
			Alias:  in.GetAlias(),
		}).
		WithBody(&message.DropAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropAliasMessageV2AckCallback(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error {
	if err := c.meta.DropAlias(ctx, result); err != nil {
		return err
	}
	if err := c.ExpireMetaCache(
		ctx,
		result.Message.Header().DbName,
		[]string{result.Message.Header().Alias},
		0,
		"",
		result.GetControlChannelResult().TimeTick,
		proxyutil.SetMsgType(commonpb.MsgType_DropAlias)); err != nil {
		return err
	}
	return nil
}
