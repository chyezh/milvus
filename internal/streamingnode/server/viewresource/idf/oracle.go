package idf

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type bm25Stats map[int64]*storage.BM25Stats

func newBM25StatsFromSchema(schema *schemapb.CollectionSchema) bm25Stats {
	stats := make(bm25Stats)
	if schema == nil {
		return stats
	}
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		stats.getOrCreate(function.GetOutputFieldIds()[0])
	}
	return stats
}

func (s bm25Stats) getOrCreate(fieldID int64) *storage.BM25Stats {
	stats, ok := s[fieldID]
	if !ok {
		stats = storage.NewBM25Stats()
		s[fieldID] = stats
	}
	return stats
}

type oracle struct {
	mu             sync.RWMutex
	stats          bm25Stats
	schema         *schemapb.CollectionSchema
	activeSegments map[int64]struct{}
}

func newOracle(stats bm25Stats, schema *schemapb.CollectionSchema, activeSegmentIDs []int64) *oracle {
	activeSegments := make(map[int64]struct{}, len(activeSegmentIDs))
	for _, segmentID := range activeSegmentIDs {
		activeSegments[segmentID] = struct{}{}
	}
	return &oracle{
		stats:          stats,
		schema:         schema,
		activeSegments: activeSegments,
	}
}

func (o *oracle) BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	stats, ok := o.stats[fieldID]
	if !ok {
		return nil, 0, errors.Errorf("bm25 field %d not found in oracle", fieldID)
	}
	idfs := make([][]byte, 0, len(tfs.GetContents()))
	for _, tf := range tfs.GetContents() {
		idfs = append(idfs, stats.BuildIDF(tf))
	}
	return idfs, stats.GetAvgdl(), nil
}

func (o *oracle) ApplyLiveMessage(_ context.Context, msg message.ImmutableMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return walview.ForEachSegmentInsertMessage(msg, 0, func(insert walview.SegmentInsertMessage) error {
		segmentID := insert.Assignment.GetSegmentAssignment().GetSegmentId()
		if _, ok := o.activeSegments[segmentID]; !ok {
			return nil
		}
		return collectGrowingInsertStats(o.stats, o.schema, insert)
	})
}
