package queryclient

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// SearchResultReducer incrementally reduces search results as they arrive from work nodes.
// Thread-safe: Add may be called concurrently from multiple goroutines.
//
// Internally maintains a per-shard sub-reducer. ResetShard discards a single shard's
// accumulated results in O(1), enabling shard-level retry without losing other shards' data.
// Final cross-shard merge happens at Finish.
type SearchResultReducer interface {
	// Add feeds a single work node's search result into the reducer.
	Add(shardID qviews.ShardID, result *internalpb.SearchResults) error

	// ResetShard discards all accumulated results for the given shard.
	ResetShard(shardID qviews.ShardID)

	// Finish merges results across all shards and returns the final result.
	Finish() (*internalpb.SearchResults, error)
}

// RetrieveResultReducer incrementally reduces retrieve results as they arrive from work nodes.
// Thread-safe: Add may be called concurrently from multiple goroutines.
//
// Follows the same shard-aware pattern as SearchResultReducer.
type RetrieveResultReducer interface {
	// Add feeds a single work node's retrieve result into the reducer.
	Add(shardID qviews.ShardID, result *internalpb.RetrieveResults) error

	// ResetShard discards all accumulated results for the given shard.
	ResetShard(shardID qviews.ShardID)

	// Finish merges results across all shards and returns the final result.
	Finish() (*internalpb.RetrieveResults, error)
}
