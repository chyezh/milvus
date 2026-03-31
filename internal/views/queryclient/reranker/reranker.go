package reranker

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// Reranker merges and reranks results from multiple sub-searches.
type Reranker interface {
	// RequiredFields returns the field names needed by this reranker.
	// These fields must be available before Rerank is called.
	// Used by FieldFetchPlanner to decide field fetch strategy.
	RequiredFields() []string

	// Rerank merges results from multiple sub-searches into a single result.
	Rerank(ctx context.Context, results []*internalpb.SearchResults) (*internalpb.SearchResults, error)
}

// Builder creates a Reranker from user request parameters.
// It encapsulates all dependencies needed for reranker construction
// (schema cache, model service clients, etc.) internally.
type Builder interface {
	// Build creates a Reranker for the given request.
	// Returns (nil, nil) if the request does not require reranking.
	Build(ctx context.Context, req *BuildRequest) (Reranker, error)
}

// BuildRequest contains the user-facing parameters needed to construct a reranker.
type BuildRequest struct {
	CollectionID int64
	// Number of sub-searches in the request. If 1, reranking is typically not needed.
	NumSubSearches int
	// Legacy rank params from HybridSearchRequest.RankParams.
	// Used when FunctionScore is not set.
	RankParams []*commonpb.KeyValuePair
	// FunctionScore from the search request schema.
	FunctionScore *schemapb.FunctionScore
}
