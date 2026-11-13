package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type runtimeRetentionCatalogStub struct {
	metastore.StreamingNodeCataLog
	metastore.WALReplicaRuntimeCatalog
	metastore.WALQueryViewRetentionCatalog
}

func (*runtimeRetentionCatalogStub) CreateWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode) error {
	panic("mockey")
}

func (*runtimeRetentionCatalogStub) InstallWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode, *streamingpb.WALReplicaCursor) error {
	panic("mockey")
}

func (*runtimeRetentionCatalogStub) ReleaseWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode) error {
	panic("mockey")
}

func TestReplicaRuntimeRetentionOwnsCandidateAndDrainedRuntime(t *testing.T) {
	for _, failure := range []string{"create", "cancel", "release", "none"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("cleanup is independent of an expired bootstrap context", t, func() {
				catalog := &runtimeRetentionCatalogStub{}
				resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
				var created *viewpb.StreamingWorkNode
				mockey.Mock((*runtimeRetentionCatalogStub).CreateWALReplicaRuntime).To(func(_ *runtimeRetentionCatalogStub, _ context.Context, runtime *viewpb.StreamingWorkNode) error {
					created = proto.CloneOf(runtime)
					if failure == "create" {
						return merr.ErrServiceUnavailable
					}
					return nil
				}).Build()
				release := mockey.Mock((*runtimeRetentionCatalogStub).ReleaseWALReplicaRuntime).To(func(_ *runtimeRetentionCatalogStub, ctx context.Context, runtime *viewpb.StreamingWorkNode) error {
					require.NoError(t, ctx.Err())
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
					require.EqualValues(t, 99, runtime.RuntimeId)
					if failure == "release" {
						return merr.ErrServiceUnavailable
					}
					return nil
				}).Build()
				mockey.Mock((*runtimeRetentionCatalogStub).InstallWALReplicaRuntime).To(func(_ *runtimeRetentionCatalogStub, _ context.Context, runtime *viewpb.StreamingWorkNode, cursor *streamingpb.WALReplicaCursor) error {
					require.True(t, proto.Equal(created, runtime))
					require.EqualValues(t, 7, cursor.Term)
					return nil
				}).Build()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if failure == "cancel" {
					cancel()
				}
				option := replicaOpenOption()
				retention, err := createReplicaRuntimeRetention(ctx, option)
				if failure == "create" || failure == "cancel" {
					require.Error(t, err)
					require.Nil(t, retention)
				} else {
					require.NoError(t, err)
					require.NoError(t, retention.install(ctx, &streamingpb.WALReplicaCursor{Term: 7}))
					option.Replica.Node.ServerId++
					require.EqualValues(t, 12, retention.runtime.NodeId)
					cancel()
					retention.Close()
				}
				if failure == "release" {
					require.Equal(t, 3, release.Times())
				} else {
					require.Equal(t, 1, release.Times())
				}
			})
		})
	}
}

func TestReplicaRuntimeRetentionRejectsUnownedCandidates(t *testing.T) {
	resource.InitForTest(t)
	o := NewOpenerAdaptor(nil).(*openerAdaptorImpl)
	defer o.Close()
	for _, change := range []func(*wal.OpenOption){
		func(o *wal.OpenOption) { o.Replica = nil },
		func(o *wal.OpenOption) { o.Replica.Node = nil },
		func(o *wal.OpenOption) { o.Replica.ReplicaId = 0 },
		func(o *wal.OpenOption) { o.Replica.ReplicaRevision = 0 },
		func(o *wal.OpenOption) {
			o.Replica.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
		},
		func(o *wal.OpenOption) { o.RuntimeID = 0 },
	} {
		option := replicaOpenOption()
		change(option)
		_, err := o.OpenReadOnlyReplica(t.Context(), option)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	_, err := o.OpenReadOnlyReplica(t.Context(), replicaOpenOption())
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, o.replicaClient, "no transport may open before candidate registration")
}
