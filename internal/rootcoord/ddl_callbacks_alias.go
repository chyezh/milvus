package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *DDLCallback) registerAliasCallbacks() {
	registry.RegisterPutAliasMessageV2AckCallback(c.putAliasMessageV2AckCallback)
	registry.RegisterDropAliasMessageV2AckCallback(c.dropAliasMessageV2AckCallback)
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
