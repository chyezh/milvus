package queryview

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

const queryViewKeyPrefix = "qv/"

// QueryViewCatalog provides ETCD persistence for Coord-owned query views.
// StreamingNode persists WAL-bound query view meta through StreamingNodeCataLog.
type QueryViewCatalog interface {
	// ListQueryViews loads all persisted query views.
	ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error)

	// SaveQueryViews atomically persists a batch of query views.
	// - State == Dropped: deletes the key from ETCD.
	// - Otherwise: puts the key with serialized value (ready_segment_ids cleared).
	SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error
}

type queryViewCatalog struct {
	metaKV kv.MetaKv
	prefix string
}

// NewQueryViewCatalog creates a new QueryViewCatalog backed by ETCD.
// The provided MetaKv is wrapped with ReliableWriteMetaKv to ensure
// write operations only fail on context cancellation.
func NewQueryViewCatalog(metaKV kv.MetaKv) QueryViewCatalog {
	return &queryViewCatalog{
		metaKV: kv.NewReliableWriteMetaKv(metaKV),
		prefix: queryViewKeyPrefix,
	}
}

func (c *queryViewCatalog) ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	_, values, err := c.metaKV.LoadWithPrefix(ctx, c.prefix)
	if err != nil {
		return nil, err
	}
	views := make([]*viewpb.QueryViewOfShard, 0, len(values))
	for _, val := range values {
		view := &viewpb.QueryViewOfShard{}
		if err := proto.Unmarshal([]byte(val), view); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

func (c *queryViewCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	if len(views) == 0 {
		return nil
	}

	saves := make(map[string]string, len(views))
	removals := make([]string, 0)

	for _, view := range views {
		key, err := c.buildKey(view.Meta)
		if err != nil {
			return err
		}
		if view.Meta.State == viewpb.QueryViewState_QueryViewStateDropped {
			delete(saves, key)
			removals = append(removals, key)
		} else {
			data, err := marshalForPersistence(view)
			if err != nil {
				return err
			}
			removals = removeString(removals, key)
			saves[key] = string(data)
		}
	}

	return c.metaKV.MultiSaveAndRemove(ctx, saves, removals)
}

// buildKey constructs the ETCD key for a query view.
// Format: {prefix}{collection_id}/{replica_id}/{vchannel_offset}
func (c *queryViewCatalog) buildKey(meta *viewpb.QueryViewMeta) (string, error) {
	if meta == nil {
		return "", merr.WrapErrServiceInternalMsg("query view meta is nil")
	}
	vchannelOffset, err := queryViewVChannelOffset(meta.GetCollectionId(), meta.GetVchannel())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%d/%d/%d",
		c.prefix,
		meta.GetCollectionId(),
		meta.GetReplicaId(),
		vchannelOffset,
	), nil
}

// marshalForPersistence serializes a QueryViewOfShard for ETCD storage.
// It clears ready_segment_ids (runtime-only state rebuilt from node responses).
func marshalForPersistence(view *viewpb.QueryViewOfShard) ([]byte, error) {
	clone := proto.Clone(view).(*viewpb.QueryViewOfShard)
	for _, qn := range clone.QueryNode {
		for _, p := range qn.Partitions {
			p.ReadySegmentIds = nil
		}
	}
	return proto.Marshal(clone)
}

func queryViewVChannelOffset(collectionID int64, vchannel string) (int64, error) {
	channel, err := metautil.ParseChannel(vchannel, metautil.NewDynChannelMapper())
	if err != nil {
		return 0, merr.WrapErrServiceInternalErr(err, "invalid query view vchannel %s", vchannel)
	}
	if channel.CollectionID() != collectionID {
		return 0, merr.WrapErrServiceInternalMsg(
			"query view vchannel %s collection %d does not match meta collection %d",
			vchannel,
			channel.CollectionID(),
			collectionID,
		)
	}
	return channel.ShardIdx(), nil
}

func removeString(values []string, value string) []string {
	for idx := 0; idx < len(values); {
		if values[idx] == value {
			values = append(values[:idx], values[idx+1:]...)
			continue
		}
		idx++
	}
	return values
}
