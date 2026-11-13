package streamingnode

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ metastore.WALQueryViewRetentionCatalog = (*catalog)(nil)

// A View is identified by its exact shard/version, not its mutable desired
// state. The remaining metadata is immutable and checked on every operation.
func queryViewRetentionIdentity(meta *viewpb.QueryViewMeta) (string, string, *viewpb.QueryViewMeta, error) {
	if meta.GetCollectionId() <= 0 || meta.GetReplicaId() <= 0 || meta.GetVersion().GetDataVersion() == nil {
		return "", "", nil, merr.WrapErrServiceInternalMsg("WAL QueryView retention requires an exact view identity")
	}
	channel, collection, _, err := funcutil.ParseVChannel(meta.Vchannel)
	if err != nil {
		return "", "", nil, err
	}
	if collection != meta.CollectionId {
		return "", "", nil, merr.WrapErrServiceInternalMsg("WAL QueryView retention collection does not match its channel")
	}
	version := meta.Version
	identity := fmt.Sprintf("%d/%s/%d/%d/%d", meta.ReplicaId, meta.Vchannel, version.DataVersion.StreamingVersion, version.DataVersion.CompactVersion, version.QueryVersion)
	normalized := proto.CloneOf(meta)
	normalized.State = viewpb.QueryViewState(0)
	normalized.Control = nil
	return channel, retentionKey(channel, "query-views", identity), normalized, nil
}

func (c *catalog) loadQueryViewRetention(ctx context.Context, channel, key string, meta *viewpb.QueryViewMeta) (*streamingpb.WALQueryViewRetention, error) {
	value, err := c.loadRetentionValue(ctx, key)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return &streamingpb.WALQueryViewRetention{View: meta}, nil
	}
	stored := &streamingpb.WALQueryViewRetention{}
	if err := proto.Unmarshal([]byte(value), stored); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL QueryView retention")
	}
	if !proto.Equal(stored.View, meta) || len(stored.Runtimes) > maxReplicaRuntimeObligations {
		return nil, merr.WrapErrDataIntegrityMsg("WAL QueryView retention identity or holder budget is invalid")
	}
	seen := make(map[string]struct{}, len(stored.Runtimes))
	for _, runtime := range stored.Runtimes {
		if err := validateRetentionRuntime(runtime); err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid WAL QueryView retention holder")
		}
		if runtime.Pchannel != channel {
			return nil, merr.WrapErrDataIntegrityMsg("WAL QueryView retention holder belongs to another channel")
		}
		key := replicaRuntimeKey(runtime)
		if _, ok := seen[key]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate WAL QueryView retention holder")
		}
		seen[key] = struct{}{}
	}
	return stored, nil
}

func (c *catalog) RetainWALQueryView(ctx context.Context, runtime *viewpb.StreamingWorkNode, meta *viewpb.QueryViewMeta) error {
	return c.updateQueryViewHolder(ctx, runtime, meta, false)
}

func (c *catalog) ReleaseWALQueryView(ctx context.Context, runtime *viewpb.StreamingWorkNode, meta *viewpb.QueryViewMeta) error {
	return c.updateQueryViewHolder(ctx, runtime, meta, true)
}

func (c *catalog) updateQueryViewHolder(ctx context.Context, runtime *viewpb.StreamingWorkNode, meta *viewpb.QueryViewMeta, release bool) error {
	channel, key, normalized, err := queryViewRetentionIdentity(meta)
	if err != nil {
		return err
	}
	if err := validateRetentionRuntime(runtime); err != nil {
		return err
	}
	if runtime.Pchannel != channel {
		return merr.WrapErrServiceInternalMsg("WAL runtime cannot retain a View of another channel")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	stored, err := c.loadQueryViewRetention(ctx, channel, key, normalized)
	if err != nil {
		return err
	}
	releasedKey := retentionKey(channel, "query-view-released", key+"/"+replicaRuntimeKey(runtime))
	saves := make(map[string]string)
	if release {
		stored.Runtimes = slices.DeleteFunc(stored.Runtimes, func(holder *viewpb.StreamingWorkNode) bool { return proto.Equal(holder, runtime) })
		// Fence release-before-acquire and delayed retries, even while the
		// runtime continues to serve other Views.
		saves[releasedKey] = "released"
	} else {
		if stored.Retiring {
			return merr.WrapErrServiceUnavailable("WAL QueryView is retiring; recapture the baseline")
		}
		if err := c.checkRuntimeRetention(ctx, channel, runtime); err != nil {
			return err
		}
		released, err := c.loadRetentionValue(ctx, releasedKey)
		if err != nil {
			return err
		}
		if released != "" {
			return merr.WrapErrServiceUnavailable("WAL runtime already drained this QueryView")
		}
		if !slices.ContainsFunc(stored.Runtimes, func(holder *viewpb.StreamingWorkNode) bool { return proto.Equal(holder, runtime) }) {
			if len(stored.Runtimes) >= maxReplicaRuntimeObligations {
				return merr.WrapErrServiceUnavailable("WAL QueryView holder budget exhausted")
			}
			stored.Runtimes = append(stored.Runtimes, proto.CloneOf(runtime))
		}
	}
	encoded, err := proto.Marshal(stored)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL QueryView retention")
	}
	saves[key] = string(encoded)
	return c.commitRetention(ctx, channel, epoch, saves, nil)
}

func (c *catalog) RetireWALQueryView(ctx context.Context, meta *viewpb.QueryViewMeta) (bool, error) {
	channel, key, normalized, err := queryViewRetentionIdentity(meta)
	if err != nil {
		return false, err
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return false, err
	}
	stored, err := c.loadQueryViewRetention(ctx, channel, key, normalized)
	if err != nil {
		return false, err
	}
	if !stored.Retiring {
		stored.Retiring = true
		encoded, err := proto.Marshal(stored)
		if err != nil {
			return false, merr.WrapErrDataIntegrity(err, "encode retiring WAL QueryView")
		}
		if err := c.commitRetention(ctx, channel, epoch, map[string]string{key: string(encoded)}, nil); err != nil {
			return false, err
		}
	}
	// Admission is now closed permanently. Concurrent releases can only make
	// the result more permissive; no CAS or lease inferred from time is needed.
	for _, holder := range stored.Runtimes {
		runtime, err := c.loadReplicaRuntime(ctx, holder)
		if err != nil {
			return false, err
		}
		if runtime == nil {
			return false, merr.WrapErrDataIntegrityMsg("WAL QueryView holder has no runtime obligation")
		}
		if runtime.State != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			return false, nil
		}
	}
	return true, nil
}
