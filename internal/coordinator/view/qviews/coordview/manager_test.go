package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_syncer"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/kv/predicates"
	"github.com/milvus-io/milvus/pkg/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestManagerAPI(t *testing.T) {
	syncerChan := make(chan []events.SyncerEvent, 10)
	mockSyncer := mock_syncer.NewMockCoordSyncer(t)
	mockSyncer.EXPECT().Sync(mock.Anything).Return().Maybe()
	mockSyncer.EXPECT().Receiver().Return(syncerChan)

	kv := mock_kv.NewMockMetaKv(t)
	kvStepForward := make(chan error, 1)
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).RunAndReturn(func(m map[string]string, s []string, p ...predicates.Predicate) error {
		return <-kvStepForward
	})
	recoveryStorage := recovery.NewRecovery(kv)
	qvm := NewQueryViewManager(mockSyncer, recoveryStorage)

	// Add replica into QueryViewManager
	applyReplicasResult := qvm.AddReplicas(AddReplicasRequest{
		Shards: []AddShardRequest{{
			ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
		}},
	})

	// The result should be blocked until first view ready.
	testShouldBlock(t, applyReplicasResult)

	applyViewResult := qvm.Apply(&QueryViewAtCoordBuilder{
		inner: &viewpb.QueryViewOfShard{
			Meta: &viewpb.QueryViewMeta{
				CollectionId: 1,
				ReplicaId:    1,
				Vchannel:     "v1",
				Version: &viewpb.QueryViewVersion{
					DataVersion: 1,
				},
				Settings: &viewpb.QueryViewSettings{},
			},
			QueryNode:     []*viewpb.QueryViewOfQueryNode{},
			StreamingNode: &viewpb.QueryViewOfStreamingNode{},
		},
	})

	// The result should be blocked until the view is persisted.
	testShouldBlock(t, applyViewResult)
	kvStepForward <- nil

	// Now the view is at preparing state.
	result := applyViewResult.Get()
	assert.NoError(t, result.Err)
	assert.NotNil(t, result.Version)
	assert.Equal(t, result.Version.QueryVersion, 1)
	assert.Equal(t, result.Version.DataVersion, 1)

	// The add replica operation should be blocked until first view is up.
	testShouldBlock(t, applyReplicasResult)

	// There are no querynodes in the view, so the view only wait for streaming node ready.
}

func testShouldBlock[T any](t *testing.T, f *syncutil.Future[T]) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := f.GetWithContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
