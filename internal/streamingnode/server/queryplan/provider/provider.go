package provider

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type QueryPlanProvider interface {
	GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
	GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}

// QueryMVCCResolver resolves consistency against the authoritative writer.
// It does not require that writer's QueryView runtime to be ready.
type QueryMVCCResolver interface {
	ResolveQueryPlanMVCC(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlanMVCC, error)
}
