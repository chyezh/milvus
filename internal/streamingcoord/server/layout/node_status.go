package channel

import (
	"errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var ErrFreeze = errors.New("balancer: node is freezed")

type VersionedNodeStatusMap struct {
	Version int64
	status  *NodeStatus
}

// NodeStatus is the status of a log node.
type NodeStatus struct {
	ServerID int64
	Address  string
	Channels map[string]types.PChannelInfo
	Error    error
}
