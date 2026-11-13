package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestQueryReplicaMetadataPinsAndReplacesTarget(t *testing.T) {
	target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
	ctx, err := EncodeQueryReplicaToOutgoingContext(t.Context(), target)
	require.NoError(t, err)
	got, err := DecodeQueryReplicaFromOutgoingContext(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(target, got))
	md, _ := metadata.FromOutgoingContext(ctx)
	incoming := metadata.NewIncomingContext(t.Context(), md)
	got, err = DecodeQueryReplicaFromIncomingContext(incoming)
	require.NoError(t, err)
	require.True(t, proto.Equal(target, got))
	channel, err := DecodeQueryViewPChannelFromIncomingContext(incoming)
	require.NoError(t, err)
	require.Equal(t, types.PChannelInfo{Name: "p", Term: 7, AccessMode: types.AccessModeRO}, channel)
	next := proto.CloneOf(target)
	next.Pchannel, next.RuntimeId, next.ReadOnly = "next", 6, false
	updated, err := EncodeQueryReplicaToOutgoingContext(ctx, next)
	require.NoError(t, err)
	md, _ = metadata.FromOutgoingContext(updated)
	channel, err = DecodeQueryViewPChannelFromIncomingContext(metadata.NewIncomingContext(t.Context(), md))
	require.NoError(t, err)
	require.Equal(t, types.PChannelInfo{Name: "next", Term: 7, AccessMode: types.AccessModeRW}, channel)
	got, err = DecodeQueryReplicaFromOutgoingContext(updated)
	require.NoError(t, err)
	require.True(t, proto.Equal(next, got))
	got, err = DecodeQueryReplicaFromOutgoingContext(ctx)
	require.NoError(t, err)
	require.True(t, proto.Equal(target, got), "the caller's metadata must remain immutable")
	got, err = DecodeQueryReplicaFromIncomingContext(t.Context())
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestQueryReplicaMetadataRejectsAmbiguousOrInvalidIdentity(t *testing.T) {
	for _, values := range [][]string{{"one", "two"}, {"\xff"}, {""}} {
		_, err := DecodeQueryReplicaFromIncomingContext(metadata.NewIncomingContext(t.Context(), metadata.MD{queryReplicaMetadataKey: values}))
		require.Error(t, err)
	}
	_, err := EncodeQueryReplicaToOutgoingContext(t.Context(), &viewpb.StreamingWorkNode{Pchannel: "\xff"})
	require.Error(t, err)
}
