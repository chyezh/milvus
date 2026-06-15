package qnview

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"google.golang.org/protobuf/proto"
)

type DefaultSegmentLoadScheduler struct {
	meta      MetadataProvider
	planner   LoadPlanner
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator
}

func NewDefaultSegmentLoadScheduler(meta MetadataProvider, planner LoadPlanner, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *DefaultSegmentLoadScheduler {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &DefaultSegmentLoadScheduler{
		meta:      meta,
		planner:   planner,
		loader:    loader,
		estimator: estimator,
	}
}

func (s *DefaultSegmentLoadScheduler) Submit(task SegmentLoadTask) {
	go s.load(task)
}

func (s *DefaultSegmentLoadScheduler) Cancel(int64) {}

func (s *DefaultSegmentLoadScheduler) load(task SegmentLoadTask) {
	ctx := task.Context
	if ctx == nil {
		ctx = context.Background()
	}
	loaded, err := s.loadMissing(ctx, task)
	if err != nil {
		if task.OnUnrecoverable != nil {
			task.OnUnrecoverable(err)
		}
		return
	}
	for _, segment := range loaded.Segments {
		if task.OnLoaded != nil {
			task.OnLoaded(segment)
		}
	}
}

func (s *DefaultSegmentLoadScheduler) loadMissing(ctx context.Context, task SegmentLoadTask) (*LoadedSegments, error) {
	collection, err := s.meta.DescribeCollection(ctx, task.Meta.GetCollectionId())
	if err != nil {
		return nil, err
	}
	segments, err := s.meta.GetSegmentInfo(ctx, task.SegmentID)
	if err != nil {
		return nil, err
	}
	if len(segments) != 1 {
		return nil, fmt.Errorf("missing segment metadata: expected 1, got %d", len(segments))
	}
	indexes, err := s.meta.ListIndexes(ctx, task.Meta.GetCollectionId())
	if err != nil {
		return nil, err
	}
	segmentIndexes, err := s.meta.GetIndexInfo(ctx, task.Meta.GetCollectionId(), task.SegmentID)
	if err != nil {
		if !errors.Is(err, merr.ErrIndexNotFound) {
			return nil, err
		}
		segmentIndexes = nil
	}
	plan, err := s.planner.Build(ctx, BuildLoadPlanRequest{
		Meta: proto.Clone(task.Meta).(*viewpb.QueryViewMeta),
		View: &viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: task.PartitionID, SegmentIds: []int64{task.SegmentID}}},
		},
		Collection:     collection,
		Segments:       segments,
		Indexes:        indexes,
		SegmentIndexes: segmentIndexes,
	})
	if err != nil {
		return nil, err
	}
	if len(plan.Segments) != 1 {
		return nil, fmt.Errorf("segment load plan should contain exactly one segment, got %d", len(plan.Segments))
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, plan.IndexInfos); err != nil {
		return nil, err
	}
	reservation, err := s.reserve(ctx, plan.Segments[0], task.Collection)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	segment, err := s.loader.Load(ctx, plan.Segments[0], task.Collection)
	if err != nil {
		return nil, err
	}
	if task.TransformStartAfterTimeTick > 0 {
		segment = &transformStartSegment{
			TransformSegment: segment,
			startAfter:       task.TransformStartAfterTimeTick,
		}
	}
	return &LoadedSegments{
		Segments:         []TransformSegment{segment},
		ReadyByPartition: map[int64][]int64{segment.PartitionID(): {segment.ID()}},
	}, nil
}

func (s *DefaultSegmentLoadScheduler) reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error) {
	if s.estimator == nil {
		return nil, nil
	}
	return s.estimator.Reserve(ctx, info, collection)
}

func updateCollectionIndexMeta(ctx context.Context, collection CollectionRuntime, indexes []*indexpb.IndexInfo) error {
	updater, ok := collection.(CollectionIndexMetaUpdater)
	if !ok {
		return nil
	}
	return updater.UpdateIndexMeta(ctx, indexes)
}

type transformStartSegment struct {
	TransformSegment
	startAfter uint64
}

func (s *transformStartSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}
