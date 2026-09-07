package resolver

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var ErrShardResolverClosed = errors.New("shard resolver is closed")

var _ ShardResolver = (*ShardResolverImpl)(nil)

// ShardReplicas contains the primary replica of a shard (vchannel).
// The primary replica owns the pchannel WAL (AccessModeRW). The replica ID is
// not discovered from channel assignment anymore; it is UnknownReplicaID until
// the real one is learned from the query plan (Phase 1 response).
type ShardReplicas struct {
	VChannel       string
	PrimaryShardID qviews.ShardID // The primary replica (owns WAL).
}

// ShardResolver resolves shard topology for a collection.
type ShardResolver interface {
	// ResolveVChannels returns all vchannels of a collection.
	// Used by the collection-level client to determine the shard fanout.
	// It blocks until the first assignment discovery snapshot is ready.
	ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error)

	// ResolveShard returns the primary replica of a single shard identified by
	// vchannel. Used by the shard-level client for consistency routing.
	// It blocks until the first assignment discovery snapshot is ready.
	ResolveShard(ctx context.Context, collectionID int64, vchannel string) (*ShardReplicas, error)
}

// CollectionVChannelProvider supplies the collection → vchannel mapping.
// It is implemented by the proxy via its GetCollection flow (metacache).
type CollectionVChannelProvider interface {
	// GetCollectionVChannels returns the vchannels of a collection.
	GetCollectionVChannels(ctx context.Context, collectionID int64) ([]string, error)
}

// NewShardResolverImpl creates a ShardResolverImpl.
// vchannels must not be nil.
func NewShardResolverImpl(w types.AssignmentDiscoverWatcher, vchannels CollectionVChannelProvider) *ShardResolverImpl {
	t := &ShardResolverImpl{
		taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		w:            w,
		vchannels:    vchannels,
		cond:         syncutil.NewContextCond(&sync.Mutex{}),
	}
	go t.watch()
	return t
}

type ShardResolverImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]

	w         types.AssignmentDiscoverWatcher
	vchannels CollectionVChannelProvider
	cond      *syncutil.ContextCond

	closed bool
	cache  *shardResolverCache
}

type shardResolverCache struct {
	// assignedPChannels contains the pchannels present in the latest assignment
	// snapshot (primary or secondary). A collection is queryable only after its
	// pchannels appear in the assignment, so this is used to fast-fail
	// collections that are not loaded yet.
	assignedPChannels map[string]struct{}
}

func (t *ShardResolverImpl) ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error) {
	cache, err := t.getCache(ctx)
	if err != nil {
		return nil, err
	}
	vchannels, err := t.vchannels.GetCollectionVChannels(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	loaded := make([]string, 0, len(vchannels))
	for _, vchannel := range vchannels {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		if _, ok := cache.assignedPChannels[pchannel]; ok {
			loaded = append(loaded, vchannel)
		}
	}
	if len(loaded) == 0 {
		return nil, merr.WrapErrCollectionNotLoaded(collectionID)
	}
	return loaded, nil
}

func (t *ShardResolverImpl) ResolveShard(ctx context.Context, collectionID int64, vchannel string) (*ShardReplicas, error) {
	cache, err := t.getCache(ctx)
	if err != nil {
		return nil, err
	}
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	if _, ok := cache.assignedPChannels[pchannel]; !ok {
		return nil, merr.WrapErrServiceInternalMsg("shard replicas not found: collection=%d, vchannel=%s", collectionID, vchannel)
	}
	return &ShardReplicas{
		VChannel: vchannel,
		PrimaryShardID: qviews.ShardID{
			ReplicaID: qviews.UnknownReplicaID,
			VChannel:  vchannel,
		},
	}, nil
}

func (t *ShardResolverImpl) Close() {
	t.cond.LockAndBroadcast()
	if t.closed {
		t.cond.L.Unlock()
		return
	}
	t.closed = true
	t.taskNotifier.Cancel()
	t.cond.L.Unlock()
	t.taskNotifier.BlockUntilFinish()
}

func (t *ShardResolverImpl) watch() {
	defer t.taskNotifier.Finish(struct{}{})
	_ = t.w.AssignmentDiscover(t.taskNotifier.Context(), func(assignments *types.VersionedStreamingNodeAssignments) error {
		cache := buildShardResolverCache(assignments)
		t.cond.LockAndBroadcast()
		t.cache = &cache
		t.cond.L.Unlock()
		return nil
	})
}

func (t *ShardResolverImpl) getCache(ctx context.Context) (shardResolverCache, error) {
	t.cond.L.Lock()
	for t.cache == nil && !t.closed {
		if err := t.cond.Wait(ctx); err != nil {
			// ContextCond.Wait does not re-acquire the lock when it returns an error.
			return shardResolverCache{}, err
		}
	}
	defer t.cond.L.Unlock()
	if t.cache != nil {
		return *t.cache, nil
	}
	return shardResolverCache{}, ErrShardResolverClosed
}

func buildShardResolverCache(assignments *types.VersionedStreamingNodeAssignments) shardResolverCache {
	cache := shardResolverCache{
		assignedPChannels: make(map[string]struct{}),
	}
	for _, assignment := range assignments.Assignments {
		for pchannel := range assignment.Channels {
			cache.assignedPChannels[pchannel] = struct{}{}
		}
		for pchannel := range assignment.SecondaryChannels {
			cache.assignedPChannels[pchannel] = struct{}{}
		}
	}
	return cache
}
