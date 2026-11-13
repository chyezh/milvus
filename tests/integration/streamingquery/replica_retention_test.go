package streamingquery

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// Run after terminating one RO in an explicitly isolated live workspace. This
// invokes the same bounded catalog maintenance as DataCoord's GC tick; it does
// not change the GC interval or authorize deletion of objects still in use.
func TestReleasedReplicaSharedRootsAtLiveCluster(t *testing.T) {
	endpoint, namespace, lost := os.Getenv("WAL_RETENTION_TEST_ETCD"), os.Getenv("WAL_RETENTION_TEST_NAMESPACE"), os.Getenv("WAL_RETENTION_TEST_LOST_NODE")
	if endpoint == "" || namespace == "" || lost == "" {
		t.Skip("requires explicit isolated etcd namespace and terminated RO node ID")
	}
	require.NotContains(t, namespace, "/")
	nodeID, err := strconv.ParseInt(lost, 10, 64)
	require.NoError(t, err)
	require.Positive(t, nodeID)
	client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoint, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	store := etcdkv.NewEtcdKV(client, namespace+"/meta")
	session, err := store.Has(ctx, "session/streamingnode-"+lost)
	require.NoError(t, err)
	require.False(t, session, "fault injection must finish before reclaiming its roots")
	catalog := streamingnode.NewCataLog(store).(metastore.WALSharedObjectRetentionCatalog)
	before, err := catalog.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	var old []*streamingpb.WALObjectRetentionRoot
	for _, root := range before {
		if root.Runtime.NodeId == nodeID {
			old = append(old, root)
		}
	}
	require.NotEmpty(t, old, "need retained roots from the crashed replica, before the normal GC sweep")
	require.Eventually(t, func() bool {
		if err := catalog.SweepReleasedSharedWALRoots(ctx, 128); err != nil {
			return false // Normal concurrent root admission can win the epoch CAS.
		}
		roots, err := catalog.ListSharedWALObjectRoots(ctx)
		if err != nil {
			return false
		}
		for _, root := range roots {
			if root.Runtime.NodeId == nodeID {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond)
	for _, root := range old {
		require.Error(t, catalog.RetainSharedWALObjects(ctx, root), "a delayed capture cannot resurrect the crashed runtime")
	}
	after, err := catalog.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, after, "surviving replicas must retain their dependencies")
	for _, root := range after {
		operation, err := catalog.BeginSharedWALObjectDeletion(ctx, root.ObjectKeys[0])
		require.NoError(t, err)
		require.Nil(t, operation, "live replica still fences physical deletion")
	}
	t.Logf("reclaimed %d crashed-node roots; %d live shared roots remain protected", len(old), len(after))
}
