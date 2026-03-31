package queryclient

import "context"

// Query execution stages:
//
//   Plan → Search → [RerankQuery] → [Rerank] → [Requery]
//
// - Plan:        Shard resolution + GetQueryPlan (Phase 1) + FieldFetchPlan generation.
// - Search:      Dispatch to work nodes + streaming reduce (Phase 2).
// - RerankQuery: Fetch reranker-required fields via RequeryOnView (optional).
// - Rerank:      Cross-sub-search reranking (optional, HybridSearch only).
// - Requery:     Fetch remaining output fields via RequeryOnView (optional).
//
// Fields can be fetched at three stages:
// - SearchFields:      returned from work nodes alongside PKs + scores during Search.
// - RerankQueryFields: fetched via RequeryOnView during RerankQuery (for reranker use).
// - RequeryFields:     fetched via RequeryOnView during Requery (for final output).

// FieldFetchPlanner decides which fields to fetch at each execution stage.
// The planner examines reranker requirements, user-requested output fields,
// and query characteristics to produce an optimal field fetch plan.
//
// Implementations range from static strategies (always defer, always inline)
// to cost-based planners that evaluate field sizes and candidate counts.
type FieldFetchPlanner interface {
	Plan(ctx context.Context, params *FieldFetchPlanParams) (*FieldFetchPlan, error)
}

// FieldFetchPlanParams provides the inputs for planning field fetch strategy.
type FieldFetchPlanParams struct {
	// Fields required by the reranker (from Reranker.RequiredFields()).
	RerankFields []string
	// Fields requested by the user as output.
	OutputFields []string
	// Number of sub-searches in the request.
	NumSubSearches int
	// Top-k per sub-search.
	TopK int64
	// Number of work nodes from the query plan.
	NumWorkNodes int
}

// FieldFetchPlan tells the executor what fields to fetch at each stage.
// The three field sets are disjoint; their union equals rerankFields ∪ outputFields.
type FieldFetchPlan struct {
	// SearchFields are returned from work nodes alongside PKs + scores
	// during the Search stage. Avoids requery but increases per-node response size.
	SearchFields []string

	// RerankQueryFields are fetched via RequeryOnView during the RerankQuery stage,
	// on the full candidate set. Only needed if the reranker requires fields
	// not already available from SearchFields.
	RerankQueryFields []string

	// RequeryFields are fetched via RequeryOnView during the Requery stage,
	// on the final top-k only. For output fields not yet available from
	// SearchFields or RerankQueryFields.
	RequeryFields []string
}

// CostEstimator provides cost signals for the cost-based field fetch planner.
type CostEstimator interface {
	// EstimateFieldSize estimates the average byte size of a field per row.
	EstimateFieldSize(ctx context.Context, collectionID int64, field string) (int64, error)
}

// NewDefaultFieldFetchPlanner returns a planner that carries rerank fields
// during Search and defers remaining output fields to Requery.
// When no reranker is present, all output fields are carried during Search.
func NewDefaultFieldFetchPlanner() FieldFetchPlanner {
	return &defaultFieldFetchPlanner{}
}

type defaultFieldFetchPlanner struct{}

func (p *defaultFieldFetchPlanner) Plan(_ context.Context, params *FieldFetchPlanParams) (*FieldFetchPlan, error) {
	if len(params.RerankFields) == 0 {
		// No reranker: carry all output fields during Search.
		return &FieldFetchPlan{
			SearchFields: params.OutputFields,
		}, nil
	}

	// Has reranker: carry rerank fields during Search, defer remaining to Requery.
	rerankSet := make(map[string]struct{}, len(params.RerankFields))
	for _, f := range params.RerankFields {
		rerankSet[f] = struct{}{}
	}

	var requeryFields []string
	for _, f := range params.OutputFields {
		if _, ok := rerankSet[f]; !ok {
			requeryFields = append(requeryFields, f)
		}
	}

	return &FieldFetchPlan{
		SearchFields:  params.RerankFields,
		RequeryFields: requeryFields,
	}, nil
}
