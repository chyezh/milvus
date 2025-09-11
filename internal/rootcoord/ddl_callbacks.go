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

	ddlCallback.registerRBACCallbacks()
	ddlCallback.registerAliasCallbacks()
}

// DDLCallback is the callback of ddl.
type DDLCallback struct {
	*Core
}
