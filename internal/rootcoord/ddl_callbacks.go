package rootcoord

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
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
	registry.RegisterPutAliasMessageV2AckCallback(c.putAliasMessageV2AckCallback)
	registry.RegisterDropAliasMessageV2AckCallback(c.dropAliasMessageV2AckCallback)
}

// DDLCallback is the callback of ddl.
type DDLCallback struct {
	*Core
}
