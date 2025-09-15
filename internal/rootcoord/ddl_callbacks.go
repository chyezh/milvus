package rootcoord

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(core *Core) {
	ddlCallback := &DDLCallback{
		Core: core,
	}
	registry.RegisterCreateCollectionV1AckCallback(ddlCallback.createCollectionV1AckCallback)
	registry.RegisterDropCollectionV1AckCallback(ddlCallback.dropCollectionV1AckCallback)

	ddlCallback.registerDatabaseCallbacks()
	ddlCallback.registerRBACCallbacks()
	ddlCallback.registerAliasCallbacks()
}

// registerDatabaseCallbacks registers the database callbacks.
func (c *DDLCallback) registerDatabaseCallbacks() {
	registry.RegisterCreateDatabaseV2AckCallback(c.createDatabaseV1AckCallback)
	registry.RegisterPutDatabaseV2AckCallback(c.putDatabaseV1AckCallback)
	registry.RegisterDropDatabaseV2AckCallback(c.dropDatabaseV1AckCallback)
}

// DDLCallback is the callback of ddl.
type DDLCallback struct {
	*Core
}
