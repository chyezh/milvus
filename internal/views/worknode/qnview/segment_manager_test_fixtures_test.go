//go:build test && dynamic

package qnview

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type fakeTransformSegment struct {
	id          int64
	vchannel    string
	partitionID int64
	startAfter  uint64
	applied     uint64
	released    bool
}

func (s *fakeTransformSegment) ID() int64 {
	return s.id
}

func (s *fakeTransformSegment) VChannel() string {
	if s.vchannel == "" {
		return testVChannel
	}
	return s.vchannel
}

func (s *fakeTransformSegment) PartitionID() int64 {
	return s.partitionID
}

func (s *fakeTransformSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *fakeTransformSegment) ApplyTransform(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (s *fakeTransformSegment) AppliedTransformTimeTick() uint64 {
	return s.applied
}

func (s *fakeTransformSegment) Release(context.Context) error {
	s.released = true
	return nil
}

type fakeTransformRegistration struct {
	waitCh       chan struct{}
	waitErr      error
	unregistered bool
}

func newFakeTransformRegistration() *fakeTransformRegistration {
	return &fakeTransformRegistration{waitCh: make(chan struct{})}
}

func (r *fakeTransformRegistration) WaitCatchup(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.waitCh:
		return r.waitErr
	}
}

func (r *fakeTransformRegistration) Unregister() {
	r.unregistered = true
}

type fakeTransformLogBuffer struct {
	mu               sync.Mutex
	acquireView      *qviews.QueryViewAtQueryNode
	acquireErr       error
	guard            *fakeTransformLogGuard
	registerSegments []int64
	registerErr      error
	regs             []*fakeTransformRegistration
}

func (b *fakeTransformLogBuffer) Acquire(_ context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.acquireView = view
	if b.acquireErr != nil {
		return nil, b.acquireErr
	}
	if b.guard == nil {
		b.guard = &fakeTransformLogGuard{}
	}
	return b.guard, nil
}

func (b *fakeTransformLogBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.registerErr != nil {
		return nil, b.registerErr
	}
	reg := newFakeTransformRegistration()
	b.registerSegments = append(b.registerSegments, segment.ID())
	b.regs = append(b.regs, reg)
	return reg, nil
}

type fakeTransformLogGuard struct {
	mu       sync.Mutex
	released bool
}

func (g *fakeTransformLogGuard) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
}

type fakeCollectionRuntimeManager struct {
	mu          sync.Mutex
	acquireView *qviews.QueryViewAtQueryNode
	acquireErr  error
	guard       *fakeCollectionRuntimeGuard
}

func (m *fakeCollectionRuntimeManager) Acquire(_ context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquireView = view
	if m.acquireErr != nil {
		return nil, m.acquireErr
	}
	if m.guard == nil {
		m.guard = &fakeCollectionRuntimeGuard{}
	}
	return m.guard, nil
}

type fakeCollectionRuntimeGuard struct {
	mu             sync.Mutex
	released       bool
	collectionID   int64
	databaseName   string
	schema         *schemapb.CollectionSchema
	schemaVersion  int64
	updatedIndexes []*indexpb.IndexInfo
	updateErr      error
}

func (g *fakeCollectionRuntimeGuard) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = true
}

func (g *fakeCollectionRuntimeGuard) CollectionID() int64 {
	return g.collectionID
}

func (g *fakeCollectionRuntimeGuard) DatabaseName() string {
	return g.databaseName
}

func (g *fakeCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema {
	return g.schema
}

func (g *fakeCollectionRuntimeGuard) SchemaVersion() int64 {
	return g.schemaVersion
}

func (g *fakeCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return nil
}

func (g *fakeCollectionRuntimeGuard) UpdateIndexMeta(_ context.Context, indexes []*indexpb.IndexInfo) error {
	g.updatedIndexes = cloneIndexInfos(indexes)
	return g.updateErr
}

type fakePhysicalSegmentManager struct {
	acquire func(AcquirePhysicalSegments)
	release func(ReleaseSegments)
}

func (m fakePhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	m.acquire(req)
}

func (m fakePhysicalSegmentManager) Release(req ReleaseSegments) {
	m.release(req)
}

type instantTransformRegistration struct{}

func (instantTransformRegistration) WaitCatchup(context.Context) error {
	return nil
}

func (instantTransformRegistration) Unregister() {}

type interleavingTransformLogBuffer struct {
	mu                sync.Mutex
	acquireCalls      int
	secondAcquire     chan struct{}
	registeredChannel map[int64]string
}

func newInterleavingTransformLogBuffer() *interleavingTransformLogBuffer {
	return &interleavingTransformLogBuffer{
		secondAcquire:     make(chan struct{}),
		registeredChannel: make(map[int64]string),
	}
}

func (b *interleavingTransformLogBuffer) Acquire(_ context.Context, _ *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	b.mu.Lock()
	b.acquireCalls++
	if b.acquireCalls == 2 {
		close(b.secondAcquire)
	}
	isFirst := b.acquireCalls == 1
	b.mu.Unlock()

	if isFirst {
		select {
		case <-b.secondAcquire:
		case <-time.After(50 * time.Millisecond):
		}
	}
	return instantTransformGuard{}, nil
}

func (b *interleavingTransformLogBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registeredChannel[segment.ID()] = segment.VChannel()
	return instantTransformRegistration{}, nil
}

type instantTransformGuard struct{}

func (instantTransformGuard) Release() {}

type fakeMetadataProvider struct {
	mu              sync.Mutex
	describeCalled  bool
	segmentsCalled  []int64
	indexesCalled   bool
	indexInfoCalled []int64
	loadInfoCalled  []int64
	collection      *milvuspb.DescribeCollectionResponse
	segments        []*datapb.SegmentInfo
	indexes         []*indexpb.IndexInfo
	segmentIndexes  map[int64][]*querypb.FieldIndexInfo
	loadInfos       []*querypb.SegmentLoadInfo
	loadIndexInfos  []*indexpb.IndexInfo
	err             error
	indexErr        error
}

func (p *fakeMetadataProvider) DescribeCollection(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
	p.describeCalled = true
	return p.collection, p.err
}

func (p *fakeMetadataProvider) GetSegmentInfo(_ context.Context, segmentIDs ...int64) ([]*datapb.SegmentInfo, error) {
	p.segmentsCalled = append([]int64(nil), segmentIDs...)
	if len(segmentIDs) > 0 {
		keep := make(map[int64]struct{}, len(segmentIDs))
		for _, segmentID := range segmentIDs {
			keep[segmentID] = struct{}{}
		}
		out := make([]*datapb.SegmentInfo, 0, len(segmentIDs))
		for _, segment := range p.segments {
			if _, ok := keep[segment.GetID()]; ok {
				out = append(out, segment)
			}
		}
		return out, p.err
	}
	return p.segments, p.err
}

func (p *fakeMetadataProvider) ListIndexes(context.Context, int64) ([]*indexpb.IndexInfo, error) {
	p.indexesCalled = true
	return p.indexes, p.err
}

func (p *fakeMetadataProvider) GetIndexInfo(_ context.Context, _ int64, segmentIDs ...int64) (map[int64][]*querypb.FieldIndexInfo, error) {
	p.indexInfoCalled = append([]int64(nil), segmentIDs...)
	if p.indexErr != nil {
		return nil, p.indexErr
	}
	return p.segmentIndexes, p.err
}

func (p *fakeMetadataProvider) GetQueryViewSegmentLoadInfo(_ context.Context, _ int64, segmentIDs ...int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	p.mu.Lock()
	p.loadInfoCalled = append(p.loadInfoCalled, segmentIDs...)
	p.mu.Unlock()
	if p.err != nil {
		return nil, nil, p.err
	}
	indexes := p.loadIndexInfos
	if indexes == nil {
		indexes = p.indexes
	}
	if p.loadInfos != nil {
		return p.loadInfos, indexes, nil
	}
	keep := make(map[int64]struct{}, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		keep[segmentID] = struct{}{}
	}
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, len(segmentIDs))
	for _, segment := range p.segments {
		if _, ok := keep[segment.GetID()]; !ok {
			continue
		}
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segment.GetID(),
			PartitionID:   segment.GetPartitionID(),
			CollectionID:  segment.GetCollectionID(),
			InsertChannel: segment.GetInsertChannel(),
			IndexInfos:    p.segmentIndexes[segment.GetID()],
		})
	}
	return loadInfos, indexes, nil
}

type fakeLoadPlanner struct {
	req   BuildLoadPlanRequest
	reqs  []BuildLoadPlanRequest
	plan  *LoadPlan
	plans []*LoadPlan
	err   error
}

func (p *fakeLoadPlanner) Build(_ context.Context, req BuildLoadPlanRequest) (*LoadPlan, error) {
	p.req = req
	p.reqs = append(p.reqs, req)
	if len(p.plans) > 0 {
		plan := p.plans[0]
		p.plans = p.plans[1:]
		return fillFakePlanSegments(plan, req), p.err
	}
	return fillFakePlanSegments(p.plan, req), p.err
}

func fillFakePlanSegments(plan *LoadPlan, req BuildLoadPlanRequest) *LoadPlan {
	if plan == nil || len(plan.Segments) > 0 {
		return plan
	}
	out := *plan
	for _, segment := range req.Segments {
		out.Segments = append(out.Segments, &querypb.SegmentLoadInfo{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
		})
	}
	return &out
}

type fakePhysicalLoader struct {
	loadInfos   []*querypb.SegmentLoadInfo
	collections []CollectionRuntime
	released    []int64
	loaded      TransformSegment
	loadErr     error
	releaseErr  error
	loadFn      func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
}

func (l *fakePhysicalLoader) Load(_ context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
	l.loadInfos = append(l.loadInfos, info)
	l.collections = append(l.collections, collection)
	if l.loadFn != nil {
		return l.loadFn(info, collection)
	}
	return l.loaded, l.loadErr
}

func (l *fakePhysicalLoader) Release(_ context.Context, segmentIDs []int64) error {
	l.released = append(l.released, segmentIDs...)
	return l.releaseErr
}

type fakeResourceReservation struct {
	released bool
}

func (r *fakeResourceReservation) Release() {
	r.released = true
}

type fakeSegmentResourceEstimator struct {
	infos        []*querypb.SegmentLoadInfo
	collections  []CollectionRuntime
	reservations []*fakeResourceReservation
	err          error
}

func (e *fakeSegmentResourceEstimator) Reserve(_ context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error) {
	e.infos = append(e.infos, info)
	e.collections = append(e.collections, collection)
	if e.err != nil {
		return nil, e.err
	}
	reservation := &fakeResourceReservation{}
	e.reservations = append(e.reservations, reservation)
	return reservation, nil
}

type fakeSegmentLoadScheduler struct {
	tasks    []SegmentLoadTask
	canceled []int64
}

func (s *fakeSegmentLoadScheduler) Submit(task SegmentLoadTask) {
	s.tasks = append(s.tasks, task)
}

func (s *fakeSegmentLoadScheduler) Cancel(segmentID int64) {
	s.canceled = append(s.canceled, segmentID)
}
