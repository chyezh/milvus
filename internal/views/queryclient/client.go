package queryclient

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// ViewQueryClient executes queries using the two-phase query process.
// It orchestrates Phase 1 (query plan generation) and Phase 2 (query execution)
// across all shards of a collection, with streaming reduce and shard-level retry.
type ViewQueryClient interface {
	Search(ctx context.Context, req *SearchRequest) (*SearchResult, error)
	Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
}

// SearchRequest wraps the internal search request with orchestration metadata.
type SearchRequest struct {
	CollectionID     int64
	PartitionIDs     []int64
	ConsistencyLevel commonpb.ConsistencyLevel
	Req              *internalpb.SearchRequest
}

// SearchResult contains the reduced search results across all shards.
type SearchResult struct {
	Results *internalpb.SearchResults
}

// QueryRequest wraps the internal retrieve request with orchestration metadata.
type QueryRequest struct {
	CollectionID     int64
	PartitionIDs     []int64
	ConsistencyLevel commonpb.ConsistencyLevel
	Req              *internalpb.RetrieveRequest
}

// QueryResult contains the reduced retrieve results across all shards.
type QueryResult struct {
	Results *internalpb.RetrieveResults
}
