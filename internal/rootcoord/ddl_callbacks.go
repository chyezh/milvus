package rootcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(core *Core) {
	ddlCallback := &DDLCallback{
		Core: core,
	}

	ddlCallback.registerDatabaseCallbacks()
	ddlCallback.registerCollectionCallbacks()
	ddlCallback.registerPartitionCallbacks()
	ddlCallback.registerRBACCallbacks()
	ddlCallback.registerAliasCallbacks()
}

// registerRBACCallbacks registers the rbac callbacks.
func (c *DDLCallback) registerRBACCallbacks() {
	registry.RegisterPutUserV2AckCallback(c.putUserV2AckCallback)
	registry.RegisterDropUserV2AckCallback(c.dropUserV2AckCallback)
	registry.RegisterPutRoleV2AckCallback(c.putRoleV2AckCallback)
	registry.RegisterDropRoleV2AckCallback(c.dropRoleV2AckCallback)
	registry.RegisterPutUserRoleV2AckCallback(c.putUserRoleV2AckCallback)
	registry.RegisterDropUserRoleV2AckCallback(c.dropUserRoleV2AckCallback)
	registry.RegisterGrantPrivilegeV2AckCallback(c.grantPrivilegeV2AckCallback)
	registry.RegisterRevokePrivilegeV2AckCallback(c.revokePrivilegeV2AckCallback)
	registry.RegisterPutPrivilegeGroupV2AckCallback(c.putPrivilegeGroupV2AckCallback)
	registry.RegisterDropPrivilegeGroupV2AckCallback(c.dropPrivilegeGroupV2AckCallback)
	registry.RegisterRestoreRBACV2AckCallback(c.restoreRBACV2AckCallback)
}

// registerAliasCallbacks registers the alias callbacks.
func (c *DDLCallback) registerAliasCallbacks() {
	registry.RegisterPutAliasV2AckCallback(c.putAliasV2AckCallback)
	registry.RegisterDropAliasV2AckCallback(c.dropAliasV2AckCallback)
}

// registerDatabaseCallbacks registers the database callbacks.
func (c *DDLCallback) registerDatabaseCallbacks() {
	registry.RegisterCreateDatabaseV2AckCallback(c.createDatabaseV1AckCallback)
	registry.RegisterPutDatabaseV2AckCallback(c.putDatabaseV1AckCallback)
	registry.RegisterDropDatabaseV2AckCallback(c.dropDatabaseV1AckCallback)
}

// registerCollectionCallbacks registers the collection callbacks.
func (c *DDLCallback) registerCollectionCallbacks() {
	registry.RegisterCreateCollectionV1AckCallback(c.createCollectionV1AckCallback)
	registry.RegisterDropCollectionV1AckCallback(c.dropCollectionV1AckCallback)
	registry.RegisterPutCollectionV2AckCallback(c.putCollectionV2AckCallback)
}

// registerPartitionCallbacks registers the partition callbacks.
func (c *DDLCallback) registerPartitionCallbacks() {
	registry.RegisterCreatePartitionV1AckCallback(c.createPartitionV1AckCallback)
	registry.RegisterDropPartitionV1AckCallback(c.dropPartitionV1AckCallback)
}

// DDLCallback is the callback of ddl.
type DDLCallback struct {
	*Core
}

// CacheExpirationsGetter is the getter of cache expirations.
type CacheExpirationsGetter interface {
	GetCacheExpirations() *message.CacheExpirations
}

// ExpireCaches handles the cache
func (c *DDLCallback) ExpireCaches(ctx context.Context, expirations any, timetick uint64) error {
	var cacheExpirations *message.CacheExpirations
	if g, ok := expirations.(CacheExpirationsGetter); ok {
		cacheExpirations = g.GetCacheExpirations()
	} else if g, ok := expirations.(*message.CacheExpirations); ok {
		cacheExpirations = g
	} else if g, ok := expirations.(*ce.CacheExpirationsBuilder); ok {
		cacheExpirations = g.Build()
	} else {
		panic(fmt.Sprintf("invalid getter type: %T", expirations))
	}
	for _, cacheExpiration := range cacheExpirations.CacheExpirations {
		if err := c.expireCache(ctx, cacheExpiration, timetick); err != nil {
			return err
		}
	}
	return nil
}

func (c *DDLCallback) expireCache(ctx context.Context, cacheExpiration *message.CacheExpiration, timetick uint64) error {
	switch cacheExpiration.Cache.(type) {
	case *messagespb.CacheExpiration_LegacyProxyCollectionMetaCache:
		legacyProxyCollectionMetaCache := cacheExpiration.GetLegacyProxyCollectionMetaCache()
		return c.Core.ExpireMetaCache(ctx, legacyProxyCollectionMetaCache.DbName, []string{legacyProxyCollectionMetaCache.CollectionName}, legacyProxyCollectionMetaCache.CollectionId, legacyProxyCollectionMetaCache.PartitionName, timetick, proxyutil.SetMsgType(legacyProxyCollectionMetaCache.MsgType))
	}
	return nil
}
