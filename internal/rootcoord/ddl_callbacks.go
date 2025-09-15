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
