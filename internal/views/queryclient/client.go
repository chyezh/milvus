package queryclient

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// ViewQueryClient executes queries using the two-phase query process.
//
// Execution stages: Plan → Search → [RerankQuery] → [Rerank] → [Requery]
//
// It provides separate entry points for Search and Query, both orchestrating
// Plan + Search across all shards with streaming reduce and shard-level retry.
// Reranking is handled internally when multiple sub-searches are present,
// using the reranker.Builder injected at construction time.
type ViewQueryClient interface {
	// Search executes one or more vector searches with optional reranking.
	// Single Search: len(SubSearches)=1, no reranking.
	// HybridSearch: len(SubSearches)>1, reranker built internally from request.
	Search(ctx context.Context, req *SearchRequest) (*SearchResult, error)

	// Query executes a single expression-based retrieve.
	Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
}

// SearchRequest contains one or more vector search sub-requests.
// When multiple sub-searches are present, the ViewQueryClient internally
// constructs a reranker via the reranker.Builder to merge results.
type SearchRequest struct {
	CollectionID     int64
	PartitionIDs     []int64
	ConsistencyLevel commonpb.ConsistencyLevel

	// One or more search sub-requests. HybridSearch has len > 1.
	SubSearches []*internalpb.SearchRequest
}

// SearchResult contains the final search result after reduce and optional reranking.
type SearchResult struct {
	Results *internalpb.SearchResults
}

// QueryRequest contains a single expression-based retrieve request.
type QueryRequest struct {
	CollectionID     int64
	PartitionIDs     []int64
	ConsistencyLevel commonpb.ConsistencyLevel
	Req              *internalpb.RetrieveRequest
}

// QueryResult contains the final retrieve result after reduce.
type QueryResult struct {
	Results *internalpb.RetrieveResults
}
