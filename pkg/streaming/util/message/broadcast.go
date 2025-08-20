package message

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"google.golang.org/protobuf/proto"
)

// newBroadcastHeaderFromProto creates a BroadcastHeader from proto.
func newBroadcastHeaderFromProto(proto *messagespb.BroadcastHeader) *BroadcastHeader {
	rks := make(typeutil.Set[ResourceKey], len(proto.GetResourceKeys()))
	for _, key := range proto.GetResourceKeys() {
		rks.Insert(NewResourceKeyFromProto(key))
	}
	return &BroadcastHeader{
		BroadcastID:  proto.GetBroadcastId(),
		VChannels:    proto.GetVchannels(),
		ResourceKeys: rks,
	}
}

type BroadcastHeader struct {
	BroadcastID  uint64
	VChannels    []string
	ResourceKeys typeutil.Set[ResourceKey]
}

// BroadcastResult is the result of broadcast operation.
type BroadcastResult[H proto.Message, B proto.Message] struct {
	Message SpecializedBroadcastMessage[H, B]
	Results map[string]*AppendResult
}

// AppendResult is the result of append operation.
type AppendResult struct {
	MessageID              MessageID
	LastConfirmedMessageID MessageID
	TimeTick               uint64
}
