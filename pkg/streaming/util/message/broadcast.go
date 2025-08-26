package message

import (
	"fmt"
	"strconv"

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

// NewResourceKeyFromProto creates a ResourceKey from proto.
func NewResourceKeyFromProto(proto *messagespb.ResourceKey) ResourceKey {
	return ResourceKey{
		Domain: proto.Domain,
		Key:    proto.Key,
		Shared: proto.Shared,
	}
}

// newProtoFromResourceKey creates a set of proto from ResourceKey.
func newProtoFromResourceKey(keys ...ResourceKey) []*messagespb.ResourceKey {
	deduplicated := typeutil.NewSet(keys...)
	protos := make([]*messagespb.ResourceKey, 0, len(keys))
	for key := range deduplicated {
		protos = append(protos, &messagespb.ResourceKey{
			Domain: key.Domain,
			Key:    key.Key,
			Shared: key.Shared,
		})
	}
	return protos
}

type ResourceKey struct {
	Domain messagespb.ResourceDomain
	Key    string
	Shared bool
}

func (r ResourceKey) String() string {
	if r.Shared {
		return fmt.Sprintf("%s:%s@R", r.Domain.String(), r.Key)
	}
	return fmt.Sprintf("%s:%s@X", r.Domain.String(), r.Key)
}

// NewImportJobIDResourceKey creates a key for import job resource.
func NewImportJobIDResourceKey(importJobID int64) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainImportJobID,
		Key:    strconv.FormatInt(importJobID, 10),
	}
}

// NewCollectionNameResourceKey creates a key for collection name resource.
func NewCollectionNameResourceKey(dbName string, collectionName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainCollectionName,
		Key:    fmt.Sprintf("%s/%s", dbName, collectionName),
	}
}

// NewDatabaseNameResourceKey creates a key for database name resource.
func NewDatabaseNameResourceKey(dbName string) ResourceKey {
	return ResourceKey{
		Domain: messagespb.ResourceDomain_ResourceDomainDatabaseName,
		Key:    dbName,
	}
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
