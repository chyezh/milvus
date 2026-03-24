package queryclient

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// ViewQueryClient executes queries using the two-phase query process.
// It orchestrates Phase 1 (query plan generation) and Phase 2 (query execution)
// across all shards of a collection, with streaming reduce and shard-level retry.
type ViewQueryClient interface {
	Search(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
	Query(ctx context.Context, req *milvuspb.QueryRequest) (*milvuspb.QueryResults, error)
}
