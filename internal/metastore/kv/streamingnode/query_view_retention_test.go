package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func retainedQueryView() *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: 1, ReplicaId: 2, Vchannel: "p_1v0", LoadInfoVersion: 3,
		Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 4}, QueryVersion: 5},
		State:   viewpb.QueryViewState_QueryViewStateUp,
	}
}

func TestWALQueryViewWaitsForEveryRuntime(t *testing.T) {
	c, coord, store := retentionCatalogs(t)
	ctx := t.Context()
	view := retainedQueryView()
	a, b := retentionRuntime(), retentionRuntime()
	b.RuntimeId++
	require.Error(t, c.RetainWALQueryView(ctx, a, view))
	for _, runtime := range []*viewpb.StreamingWorkNode{a, b} {
		require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
		require.NoError(t, c.RetainWALQueryView(ctx, runtime, view))
		require.NoError(t, c.RetainWALQueryView(ctx, runtime, view))
	}
	// Desired state/control do not change the immutable resource obligation.
	view.State = viewpb.QueryViewState_QueryViewStateDown
	view.Control = &viewpb.QueryViewControl{PreemptServingLease: true}
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, &streamingpb.WALCheckpoint{Term: 8})))
	drained, err := coord.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.False(t, drained, "replacing the source does not drain its followers")
	require.Error(t, c.RetainWALQueryView(ctx, a, view), "retirement closes even idempotent admission")
	require.NoError(t, c.ReleaseWALQueryView(ctx, a, view))
	drained, err = coord.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.False(t, drained)
	require.NoError(t, c.ReleaseWALReplicaRuntime(ctx, b))
	drained, err = coord.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.True(t, drained, "draining the entire runtime releases all its Views")
	// Recovery uses only persisted obligations, never a process-local count.
	c = NewCataLog(store).(*catalog)
	drained, err = c.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.True(t, drained)
	require.NoError(t, c.ReleaseWALQueryView(ctx, b, view))
	require.NoError(t, c.ReleaseWALQueryView(ctx, b, view))
	view.Version.QueryVersion++
	drained, err = coord.RetireWALQueryView(ctx, view)
	require.NoError(t, err)
	require.True(t, drained, "a View without followers still fences future admission")
}

func TestWALQueryViewReleaseBeforeAcquireAndVersionIsolation(t *testing.T) {
	c, _, _ := retentionCatalogs(t)
	ctx := t.Context()
	runtime, view := retentionRuntime(), retainedQueryView()
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	require.NoError(t, c.ReleaseWALQueryView(ctx, runtime, view))
	require.Error(t, c.RetainWALQueryView(ctx, runtime, view))
	view.Version.QueryVersion++
	require.NoError(t, c.RetainWALQueryView(ctx, runtime, view))
	view.LoadInfoVersion++
	require.ErrorIs(t, c.RetainWALQueryView(ctx, runtime, view), merr.ErrDataIntegrity)
}

func TestWALQueryViewAdmissionRaces(t *testing.T) {
	for _, operation := range []string{"retire", "release-view", "release-runtime", "retire-races-acquire"} {
		t.Run(operation, func(t *testing.T) {
			mockey.PatchConvey("durable admission and drain serialize", t, func() {
				c, other, _ := retentionCatalogs(t)
				ctx := t.Context()
				runtime, view := retentionRuntime(), retainedQueryView()
				require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
				once := false
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(actual *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if actual == c && !once {
						once = true
						switch operation {
						case "retire":
							drained, err := other.RetireWALQueryView(ctx, view)
							require.NoError(t, err)
							require.True(t, drained)
						case "release-view":
							require.NoError(t, other.ReleaseWALQueryView(ctx, runtime, view))
						case "release-runtime":
							require.NoError(t, other.ReleaseWALReplicaRuntime(ctx, runtime))
						case "retire-races-acquire":
							require.NoError(t, other.RetainWALQueryView(ctx, runtime, view))
						}
					}
					return original(actual, ctx, channel, epoch, saves, removes, guards...)
				}).Build()
				if operation == "retire-races-acquire" {
					drained, err := c.RetireWALQueryView(ctx, view)
					require.Error(t, err)
					require.False(t, drained)
					drained, err = c.RetireWALQueryView(ctx, view)
					require.NoError(t, err)
					require.False(t, drained)
				} else {
					require.Error(t, c.RetainWALQueryView(ctx, runtime, view))
					require.Error(t, c.RetainWALQueryView(ctx, runtime, view))
				}
				require.True(t, once)
			})
		})
	}
}

func TestWALQueryViewIdentityAndCorruption(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	runtime, view := retentionRuntime(), retainedQueryView()
	for _, change := range []func(*viewpb.QueryViewMeta){
		func(v *viewpb.QueryViewMeta) { v.CollectionId = 0 },
		func(v *viewpb.QueryViewMeta) { v.ReplicaId = 0 },
		func(v *viewpb.QueryViewMeta) { v.Version = nil },
		func(v *viewpb.QueryViewMeta) { v.Vchannel = "invalid" },
		func(v *viewpb.QueryViewMeta) { v.CollectionId = 2 },
	} {
		v := proto.CloneOf(view)
		change(v)
		require.Error(t, c.RetainWALQueryView(ctx, runtime, v))
		_, err := c.RetireWALQueryView(ctx, v)
		require.Error(t, err)
	}
	require.Error(t, c.RetainWALQueryView(ctx, nil, view))
	other := proto.CloneOf(runtime)
	other.Pchannel = "other"
	require.Error(t, c.RetainWALQueryView(ctx, other, view))
	_, key, normalized, err := queryViewRetentionIdentity(view)
	require.NoError(t, err)
	for _, raw := range []string{"invalid", queryViewRetentionBytes(t, &streamingpb.WALQueryViewRetention{Retiring: true})} {
		require.NoError(t, store.Save(ctx, key, raw))
		_, err := c.RetireWALQueryView(ctx, view)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	for _, holders := range [][]*viewpb.StreamingWorkNode{{nil}, {other}, {runtime, runtime}, make([]*viewpb.StreamingWorkNode, maxReplicaRuntimeObligations+1)} {
		require.NoError(t, store.Save(ctx, key, queryViewRetentionBytes(t, &streamingpb.WALQueryViewRetention{View: normalized, Runtimes: holders})))
		_, err := c.RetireWALQueryView(ctx, view)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	}
	require.NoError(t, store.Save(ctx, key, queryViewRetentionBytes(t, &streamingpb.WALQueryViewRetention{View: normalized, Runtimes: []*viewpb.StreamingWorkNode{runtime}})))
	_, err = c.RetireWALQueryView(ctx, view)
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "missing runtime cannot be inferred drained")
	require.NoError(t, store.Save(ctx, replicaRuntimeKey(runtime), "invalid"))
	_, err = c.RetireWALQueryView(ctx, view)
	require.Error(t, err)
}

func TestWALQueryViewHolderBudget(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	runtime, view := retentionRuntime(), retainedQueryView()
	require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
	_, key, normalized, err := queryViewRetentionIdentity(view)
	require.NoError(t, err)
	holders := make([]*viewpb.StreamingWorkNode, maxReplicaRuntimeObligations)
	for i := range holders {
		holders[i] = proto.CloneOf(runtime)
		holders[i].RuntimeId += uint64(i + 1)
	}
	require.NoError(t, store.Save(ctx, key, queryViewRetentionBytes(t, &streamingpb.WALQueryViewRetention{View: normalized, Runtimes: holders})))
	require.Error(t, c.RetainWALQueryView(ctx, runtime, view))
}

func TestWALQueryViewReadFailures(t *testing.T) {
	for _, failing := range []string{"epoch", "view", "release"} {
		t.Run(failing, func(t *testing.T) {
			mockey.PatchConvey("read failures preserve obligations", t, func() {
				c, _, _ := retentionCatalogs(t)
				ctx := t.Context()
				runtime, view := retentionRuntime(), retainedQueryView()
				require.NoError(t, c.CreateWALReplicaRuntime(ctx, runtime))
				_, key, _, err := queryViewRetentionIdentity(view)
				require.NoError(t, err)
				failKey := map[string]string{
					"epoch": retentionPrefix("p") + "epoch", "view": key,
					"release": retentionKey("p", "query-view-released", key+"/"+replicaRuntimeKey(runtime)),
				}[failing]
				failure := merr.WrapErrServiceUnavailable("catalog unavailable")
				var original func(*catalog, context.Context, string) (string, error)
				mockey.Mock((*catalog).loadRetentionValue).Origin(&original).To(func(actual *catalog, ctx context.Context, key string) (string, error) {
					if key == failKey {
						return "", failure
					}
					return original(actual, ctx, key)
				}).Build()
				require.ErrorIs(t, c.RetainWALQueryView(ctx, runtime, view), failure)
				if failing != "release" {
					_, err = c.RetireWALQueryView(ctx, view)
					require.ErrorIs(t, err, failure)
				}
			})
		})
	}
}

func queryViewRetentionBytes(t *testing.T, message proto.Message) string {
	t.Helper()
	encoded, err := proto.Marshal(message)
	require.NoError(t, err)
	return string(encoded)
}
