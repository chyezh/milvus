package coordview

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_recovery"
	"github.com/milvus-io/milvus/internal/mocks/coordinator/view/qviews/mock_syncer"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestManagerAPI(t *testing.T) {
	syncerChan := make(chan []events.SyncerEvent, 10)
	mockSyncer := mock_syncer.NewMockCoordSyncer(t)
	mockSyncer.EXPECT().Receiver().Return(syncerChan)

	recoveryChan := make(chan events.RecoveryEvent, 10)
	recoveryStorage := mock_recovery.NewMockRecoveryStorage(t)
	recoveryStorage.EXPECT().Event().Return(recoveryChan)
	recoveryStorage.EXPECT().SwapPreparing(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
	qvm := NewQueryViewManager(mockSyncer, recoveryStorage)

	// Add replica into QueryViewManager
	result := qvm.AddReplicas(AddReplicasRequest{
		Shards: []AddShardRequest{{
			ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
		}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := result.GetWithContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	result2 := qvm.Apply(&QueryViewAtCoordBuilder{
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

	result2.Get()
}
