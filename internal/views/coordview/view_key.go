package coordview

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// viewKey uniquely identifies an outstanding sync entry.
// Composed of (replicaID, vchannel, version).
type viewKey struct {
	replicaID    int64
	vchannel     string
	dataVersion  qviews.DataVersion
	queryVersion int64
}

func viewKeyFromView(v qviews.QueryViewAtWorkNode) viewKey {
	ver := v.Version()
	sid := v.ShardID()
	return viewKey{
		replicaID:    sid.ReplicaID,
		vchannel:     sid.VChannel,
		dataVersion:  ver.DataVersion,
		queryVersion: ver.QueryVersion,
	}
}

func (k viewKey) String() string {
	return fmt.Sprintf("%d-%s-%s/%d", k.replicaID, k.vchannel, k.dataVersion, k.queryVersion)
}
