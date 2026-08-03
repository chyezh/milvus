package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestQueryViewPChannelMetadataDefaultsWALReplicaIDToZero(t *testing.T) {
	ctx := EncodeQueryViewPChannelToOutgoingContext(context.Background(), types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	})
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	incomingCtx := metadata.NewIncomingContext(context.Background(), md)

	replicaID, err := DecodeQueryViewWALReplicaIDFromIncomingContext(incomingCtx)

	require.NoError(t, err)
	require.Equal(t, int64(0), replicaID)
}

func TestQueryViewWALReplicaMetadataRoundTrip(t *testing.T) {
	ctx := EncodeQueryViewWALReplicaToOutgoingContext(context.Background(), types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}, 2)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	incomingCtx := metadata.NewIncomingContext(context.Background(), md)

	pchannel, err := DecodeQueryViewPChannelFromIncomingContext(incomingCtx)
	require.NoError(t, err)
	replicaID, err := DecodeQueryViewWALReplicaIDFromIncomingContext(incomingCtx)

	require.NoError(t, err)
	require.Equal(t, types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}, pchannel)
	require.Equal(t, int64(2), replicaID)
}

func TestQueryViewWALReplicaMetadataRejectsInvalidValue(t *testing.T) {
	ctx := EncodeQueryViewWALReplicaToOutgoingContext(context.Background(), types.PChannelInfo{
		Name:       "p0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}, 2)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	md.Set(queryViewWALReplicaIDMetadataKey, "-1")
	incomingCtx := metadata.NewIncomingContext(context.Background(), md)

	_, err := DecodeQueryViewWALReplicaIDFromIncomingContext(incomingCtx)

	require.Error(t, err)
}
