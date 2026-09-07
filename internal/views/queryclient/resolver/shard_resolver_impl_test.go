package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestShardResolverImplResolvesPrimaryShard(t *testing.T) {
	const collectionID int64 = 100
	vchannel0 := funcutil.GetVirtualChannel("p0", collectionID, 0)
	vchannel1 := funcutil.GetVirtualChannel("p1", collectionID, 1)

	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignmentForNodes(
					1, "localhost:1", []string{"p0"},
					2, "localhost:2", []string{"p1"},
				),
			},
		},
		&staticVChannelProvider{vchannels: map[int64][]string{
			collectionID: {vchannel0, vchannel1},
		}},
	)
	defer resolver.Close()

	ctx := context.Background()
	vchannels, err := resolver.ResolveVChannels(ctx, collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{vchannel0, vchannel1}, vchannels)

	replicas, err := resolver.ResolveShard(ctx, collectionID, vchannel0)
	require.NoError(t, err)
	assert.Equal(t, vchannel0, replicas.VChannel)
	// The client only knows the primary replica; the real replica ID is learned
	// from the query plan.
	assert.Equal(t, qviews.ShardID{ReplicaID: qviews.UnknownReplicaID, VChannel: vchannel0}, replicas.PrimaryShardID)
}

func TestShardResolverImplFiltersUnassignedVChannels(t *testing.T) {
	const collectionID int64 = 100
	vchannel0 := funcutil.GetVirtualChannel("p0", collectionID, 0)
	vchannel1 := funcutil.GetVirtualChannel("p1", collectionID, 1)

	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
			},
		},
		// p1 is not assigned yet; only p0 is queryable.
		&staticVChannelProvider{vchannels: map[int64][]string{
			collectionID: {vchannel0, vchannel1},
		}},
	)
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{vchannel0}, vchannels)
}

func TestShardResolverImplReturnsNotLoadedForUnassignedCollection(t *testing.T) {
	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
			},
		},
		&staticVChannelProvider{vchannels: map[int64][]string{
			100: {funcutil.GetVirtualChannel("p0", 100, 0)},
		}},
	)
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), 200)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	assert.Nil(t, vchannels)
}

func TestShardResolverImplPassesThroughProviderError(t *testing.T) {
	const collectionID int64 = 100
	providerErr := context.DeadlineExceeded
	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
			},
		},
		&staticVChannelProvider{err: providerErr},
	)
	defer resolver.Close()

	_, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.ErrorIs(t, err, providerErr)
}

func TestShardResolverImplReturnsNotLoadedForEmptyProviderResult(t *testing.T) {
	const collectionID int64 = 100
	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
			},
		},
		&staticVChannelProvider{vchannels: map[int64][]string{}},
	)
	defer resolver.Close()

	_, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
}

func TestShardResolverImplRejectsShardOfUnassignedPChannel(t *testing.T) {
	const collectionID int64 = 100
	vchannel0 := funcutil.GetVirtualChannel("p0", collectionID, 0)
	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
			},
		},
		&staticVChannelProvider{vchannels: map[int64][]string{
			collectionID: {vchannel0},
		}},
	)
	defer resolver.Close()

	_, err := resolver.ResolveShard(context.Background(), collectionID, funcutil.GetVirtualChannel("p9", collectionID, 9))
	assert.Error(t, err)
}

func TestShardResolverImplReplacesCacheOnAssignmentUpdate(t *testing.T) {
	const collectionID int64 = 100
	oldVChannel := funcutil.GetVirtualChannel("p0", collectionID, 0)
	newVChannel := funcutil.GetVirtualChannel("p1", collectionID, 1)
	resolver := NewShardResolverImpl(
		&staticAssignmentWatcher{
			assignments: []*types.VersionedStreamingNodeAssignments{
				versionedAssignment(1, "localhost:1", "p0"),
				versionedAssignment(1, "localhost:1", "p1"),
			},
		},
		&staticVChannelProvider{vchannels: map[int64][]string{
			collectionID: {oldVChannel, newVChannel},
		}},
	)
	defer resolver.Close()

	vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
	require.NoError(t, err)
	assert.Equal(t, []string{newVChannel}, vchannels)

	_, err = resolver.ResolveShard(context.Background(), collectionID, oldVChannel)
	assert.Error(t, err)
}

func TestShardResolverImplBlocksUntilFirstServiceDiscoverySuccess(t *testing.T) {
	const collectionID int64 = 100
	vchannel := funcutil.GetVirtualChannel("p0", collectionID, 0)
	watcher := newWaitableAssignmentWatcher(versionedAssignment(1, "localhost:1", "p0"))
	resolver := NewShardResolverImpl(watcher, &staticVChannelProvider{vchannels: map[int64][]string{
		collectionID: {vchannel},
	}})
	defer resolver.Close()

	resultCh := make(chan []string, 1)
	errCh := make(chan error, 1)
	go func() {
		vchannels, err := resolver.ResolveVChannels(context.Background(), collectionID)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- vchannels
	}()

	select {
	case result := <-resultCh:
		t.Fatalf("ResolveVChannels returned before service discovery was ready: %v", result)
	case err := <-errCh:
		t.Fatalf("ResolveVChannels failed before service discovery was ready: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	watcher.ready()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case vchannels := <-resultCh:
		assert.Equal(t, []string{vchannel}, vchannels)
	case <-time.After(time.Second):
		t.Fatal("ResolveVChannels did not return after service discovery became ready")
	}
}

func TestShardResolverImplReturnsContextErrorWhileWaitingForReady(t *testing.T) {
	watcher := newWaitableAssignmentWatcher(versionedAssignment(1, "localhost:1", "p0"))
	resolver := NewShardResolverImpl(watcher, &staticVChannelProvider{})
	defer resolver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := resolver.ResolveVChannels(ctx, 100)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

type staticVChannelProvider struct {
	vchannels map[int64][]string
	err       error
}

func (p *staticVChannelProvider) GetCollectionVChannels(_ context.Context, collectionID int64) ([]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	return append([]string(nil), p.vchannels[collectionID]...), nil
}

type staticAssignmentWatcher struct {
	assignments []*types.VersionedStreamingNodeAssignments
}

func (w *staticAssignmentWatcher) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	for _, assignment := range w.assignments {
		if err := cb(assignment); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func (w *staticAssignmentWatcher) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, err error) error {
	return nil
}

type waitableAssignmentWatcher struct {
	assignment *types.VersionedStreamingNodeAssignments
	readyCh    chan struct{}
}

func newWaitableAssignmentWatcher(assignment *types.VersionedStreamingNodeAssignments) *waitableAssignmentWatcher {
	return &waitableAssignmentWatcher{
		assignment: assignment,
		readyCh:    make(chan struct{}),
	}
}

func (w *waitableAssignmentWatcher) ready() {
	close(w.readyCh)
}

func (w *waitableAssignmentWatcher) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	select {
	case <-w.readyCh:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if err := cb(w.assignment); err != nil {
		return err
	}
	<-ctx.Done()
	return context.Cause(ctx)
}

func (w *waitableAssignmentWatcher) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, err error) error {
	return nil
}

func versionedAssignment(
	serverID int64,
	address string,
	pchannel string,
) *types.VersionedStreamingNodeAssignments {
	return &types.VersionedStreamingNodeAssignments{
		StreamingVersion: &streamingpb.StreamingVersion{},
		Version:          typeutil.VersionInt64Pair{Global: serverID, Local: 1},
		Assignments: map[int64]types.StreamingNodeAssignment{
			serverID: {
				NodeInfo:          types.StreamingNodeInfo{ServerID: serverID, Address: address},
				Channels:          channelMap([]string{pchannel}),
				SecondaryChannels: map[string]types.PChannelInfo{},
			},
		},
	}
}

func versionedAssignmentForNodes(
	serverID1 int64,
	address1 string,
	pchannels1 []string,
	serverID2 int64,
	address2 string,
	pchannels2 []string,
) *types.VersionedStreamingNodeAssignments {
	assignments := map[int64]types.StreamingNodeAssignment{
		serverID1: {
			NodeInfo:          types.StreamingNodeInfo{ServerID: serverID1, Address: address1},
			Channels:          channelMap(pchannels1),
			SecondaryChannels: map[string]types.PChannelInfo{},
		},
		serverID2: {
			NodeInfo:          types.StreamingNodeInfo{ServerID: serverID2, Address: address2},
			Channels:          channelMap(pchannels2),
			SecondaryChannels: map[string]types.PChannelInfo{},
		},
	}
	return &types.VersionedStreamingNodeAssignments{
		StreamingVersion: &streamingpb.StreamingVersion{},
		Version:          typeutil.VersionInt64Pair{Global: serverID1, Local: 1},
		Assignments:      assignments,
	}
}

func channelMap(pchannels []string) map[string]types.PChannelInfo {
	channels := make(map[string]types.PChannelInfo, len(pchannels))
	for _, pchannel := range pchannels {
		channels[pchannel] = types.PChannelInfo{Name: pchannel, Term: 1, AccessMode: types.AccessModeRW}
	}
	return channels
}
