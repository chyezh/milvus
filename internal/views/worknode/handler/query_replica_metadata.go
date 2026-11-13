package handler

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const queryReplicaMetadataKey = "milvus-query-view-runtime-bin"

func EncodeQueryReplicaToOutgoingContext(ctx context.Context, target *viewpb.StreamingWorkNode) (context.Context, error) {
	data, err := proto.Marshal(target)
	if err != nil {
		return nil, merr.Wrap(err, "encode query replica identity")
	}
	mode := types.AccessModeRW
	if target.ReadOnly {
		mode = types.AccessModeRO
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(queryViewPChannelNameMetadataKey, target.Pchannel)
	md.Set(queryViewPChannelTermMetadataKey, strconv.FormatInt(target.WalTerm, 10))
	md.Set(queryViewPChannelAccessModeMetadataKey, mode.String())
	md.Set(queryReplicaMetadataKey, string(data))
	return metadata.NewOutgoingContext(ctx, md), nil
}

func DecodeQueryReplicaFromIncomingContext(ctx context.Context) (*viewpb.StreamingWorkNode, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	return decodeQueryReplica(md)
}

func DecodeQueryReplicaFromOutgoingContext(ctx context.Context) (*viewpb.StreamingWorkNode, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	return decodeQueryReplica(md)
}

func decodeQueryReplica(md metadata.MD) (*viewpb.StreamingWorkNode, error) {
	values := md.Get(queryReplicaMetadataKey)
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("query replica identity must be unambiguous")
	}
	target := &viewpb.StreamingWorkNode{}
	if err := proto.Unmarshal([]byte(values[0]), target); err != nil {
		return nil, merr.Wrap(err, "decode query replica identity")
	}
	if target.NodeId <= 0 || target.RuntimeId == 0 || target.WalTerm <= 0 || target.Pchannel == "" {
		return nil, merr.WrapErrServiceInternalMsg("query replica identity is incomplete")
	}
	return target, nil
}
