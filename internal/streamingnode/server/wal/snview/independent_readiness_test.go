package snview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestReplicaRemembersUpUntilItsOwnResourcesAreReady(t *testing.T) {
	sm := newSNQueryViewStateMachine(buildTestMeta(), nil, nil)
	sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
	require.Equal(t, qviews.QueryViewStatePreparing, sm.State())
	require.Nil(t, sm.ConsumePersist())
	sm.OnCoordStateDelivered(qviews.QueryViewStatePreparing)
	sm.OnReady()
	require.Equal(t, qviews.QueryViewStateUp, sm.State())
	require.NotNil(t, sm.ConsumePersist())
	require.Equal(t, qviews.QueryViewStateUp, qviews.QueryViewState(sm.ConsumeReport().GetMeta().GetState()))
}

func TestReplicaDoesNotAdvertiseRetiredViewAfterDelayedPreparation(t *testing.T) {
	for _, target := range []qviews.QueryViewState{qviews.QueryViewStateDown, qviews.QueryViewStateDropped} {
		t.Run(target.String(), func(t *testing.T) {
			sm := newSNQueryViewStateMachine(buildTestMeta(), nil, nil)
			sm.OnCoordStateDelivered(qviews.QueryViewStateUp)
			sm.OnCoordStateDelivered(target)
			sm.OnReady()
			require.NotEqual(t, qviews.QueryViewStateUp, sm.State())
			require.Nil(t, sm.ConsumePersist())
		})
	}
}
