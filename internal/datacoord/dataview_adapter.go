// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	DataViewManager                  = dataview.Manager
	CreateCollectionDataViewEvent    = dataview.CreateCollectionDataViewEvent
	FlushDataViewEvent               = dataview.FlushDataViewEvent
	ImportDataViewEvent              = dataview.ImportDataViewEvent
	CopySegmentCompleteDataViewEvent = dataview.CopySegmentCompleteDataViewEvent
	CompactDataViewEvent             = dataview.CompactDataViewEvent
	L0CompactDataViewEvent           = dataview.L0CompactDataViewEvent
	ExternalRefreshDataViewEvent     = dataview.ExternalRefreshDataViewEvent
	DropPartitionDataViewEvent       = dataview.DropPartitionDataViewEvent
	TruncateDataViewEvent            = dataview.TruncateDataViewEvent
	SegmentMembership                = dataview.SegmentMembership
	SegmentTrimTarget                = dataview.SegmentTrimTarget
	SegmentTrimFinalize              = dataview.SegmentTrimFinalize
	PublishedMutation                = dataview.PublishedMutation
)

type dataViewSegmentStore struct {
	meta *meta
}

func newDataViewManager(catalog metastore.DataCoordCatalog, meta *meta) DataViewManager {
	return dataview.NewManager(catalog, &dataViewSegmentStore{meta: meta})
}

func (s *Server) CreateCollectionDataView(ctx context.Context, collectionID int64, vchannels []string) (*viewpb.DataVersion, error) {
	if s.dataViewManager == nil {
		return nil, nil
	}
	return s.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: collectionID,
		VChannels:    vchannels,
	})
}

func (s *Server) DropCollectionDataView(ctx context.Context, collectionID int64) error {
	if s.dataViewReferences == nil {
		return nil
	}
	return s.dataViewReferences.DropCollection(ctx, collectionID)
}

func (s *Server) FinalizeDropCollectionDataView(ctx context.Context, collectionID int64) error {
	if s.dataViewReferences == nil {
		return nil
	}
	return s.dataViewReferences.FinalizeDropCollection(ctx, collectionID)
}

func (s *Server) PinDataView(ctx context.Context, collectionID int64, version qviews.DataVersion) error {
	if s.dataViewReferences == nil {
		return merr.WrapErrServiceNotReadyMsg("data view reference manager is not initialized")
	}
	return s.dataViewReferences.PinDataView(ctx, collectionID, version)
}

func (s *Server) RecoverDataViewReference(ctx context.Context, collectionID int64, version qviews.DataVersion) (bool, error) {
	if s.dataViewReferences == nil {
		return false, merr.WrapErrServiceNotReadyMsg("data view reference manager is not initialized")
	}
	return s.dataViewReferences.RecoverDataViewReference(ctx, collectionID, version)
}

func (s *Server) UnpinDataView(collectionID int64, version qviews.DataVersion) {
	if s.dataViewReferences != nil {
		s.dataViewReferences.UnpinDataView(collectionID, version)
	}
}

func (s *Server) Snapshot(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error) {
	if s.dataViewManager == nil {
		return nil, nil
	}
	return s.dataViewManager.Snapshot(ctx, collectionIDs)
}

func (s *Server) DataViewProvider() balancer.DataViewProvider {
	return s.dataViewManager
}

func (s *dataViewSegmentStore) GetSegment(ctx context.Context, segmentID int64) *dataview.Segment {
	return newDataViewSegment(s.meta.GetSegment(ctx, segmentID))
}

func (s *dataViewSegmentStore) GetSegments(_ context.Context, segmentIDs []int64) []*dataview.Segment {
	segments := s.meta.GetSegmentInfos(segmentIDs)
	result := make([]*dataview.Segment, 0, len(segments))
	for _, segment := range segments {
		result = append(result, newDataViewSegment(segment))
	}
	return result
}

func (s *dataViewSegmentStore) SelectSegments(ctx context.Context, collectionID int64) []*dataview.Segment {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID))
	result := make([]*dataview.Segment, 0, len(segments))
	validPartitions := s.validPartitions(collectionID)
	for _, segment := range segments {
		if validPartitions != nil {
			if _, ok := validPartitions[segment.GetPartitionID()]; !ok {
				continue
			}
		}
		result = append(result, newDataViewSegment(segment))
	}
	return result
}

func (s *dataViewSegmentStore) ListAllSegmentsForVersionAllocation(
	ctx context.Context,
	collectionID int64,
) []*dataview.Segment {
	segments := s.meta.SelectSegments(ctx, WithCollection(collectionID))
	result := make([]*dataview.Segment, 0, len(segments))
	for _, segment := range segments {
		result = append(result, newDataViewSegment(segment))
	}
	return result
}

func (s *dataViewSegmentStore) SaveSealedAtDataVersion(
	ctx context.Context,
	segmentID int64,
	version *viewpb.DataVersion,
) error {
	return s.meta.SetSegmentSealedAtDataVersion(ctx, segmentID, version)
}

func (s *dataViewSegmentStore) validPartitions(collectionID int64) map[int64]struct{} {
	collection := s.meta.GetCollection(collectionID)
	if collection == nil || len(collection.Partitions) == 0 {
		return nil
	}
	partitions := make(map[int64]struct{}, len(collection.Partitions))
	for _, partitionID := range collection.Partitions {
		partitions[partitionID] = struct{}{}
	}
	return partitions
}

func newDataViewSegment(segment *SegmentInfo) *dataview.Segment {
	if segment == nil {
		return nil
	}
	var sealedAtDataVersion *viewpb.DataVersion
	if version := segment.GetSealedAtDataVersion(); version != nil {
		sealedAtDataVersion = proto.Clone(version).(*viewpb.DataVersion)
	}
	return &dataview.Segment{
		ID:                          segment.GetID(),
		CollectionID:                segment.GetCollectionID(),
		PartitionID:                 segment.GetPartitionID(),
		InsertChannel:               segment.GetInsertChannel(),
		NumOfRows:                   segment.GetNumOfRows(),
		MemSize:                     dataViewSegmentMemSize(segment),
		State:                       segment.GetState(),
		Level:                       segment.GetLevel(),
		IsImporting:                 segment.GetIsImporting(),
		IsInvisible:                 segment.GetIsInvisible(),
		StartPosition:               segment.GetStartPosition(),
		DmlPosition:                 segment.GetDmlPosition(),
		CommitTimestamp:             segment.GetCommitTimestamp(),
		TransformStartAfterTimetick: segment.GetDeleteApplyStartAfterTimetick(),
		CreatedByCompaction:         segment.GetCreatedByCompaction(),
		CompactionFrom:              append([]int64(nil), segment.GetCompactionFrom()...),
		SealedAtDataVersion:         sealedAtDataVersion,
	}
}

func publishedSegmentMembership(segment *SegmentInfo) dataview.SegmentMembership {
	return dataview.SegmentMembership{
		SegmentID:    segment.GetID(),
		CollectionID: segment.GetCollectionID(),
		PartitionID:  segment.GetPartitionID(),
		VChannel:     segment.GetInsertChannel(),
		State:        segment.GetState(),
		Level:        segment.GetLevel(),
		IsImporting:  segment.GetIsImporting(),
		IsInvisible:  segment.GetIsInvisible(),
	}
}

func clonePublishedDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
}

func (m *meta) loadablePublishedMemberships(segmentIDs []int64) ([]dataview.SegmentMembership, bool) {
	memberships := make([]dataview.SegmentMembership, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := m.GetSegment(m.ctx, segmentID)
		if segment == nil {
			return nil, false
		}
		if !isImmediatelyLoadableFlushSegment(segment) {
			return nil, false
		}
		memberships = append(memberships, publishedSegmentMembership(segment))
	}
	return memberships, true
}

func (m *meta) loadableCompactionMemberships(segmentIDs []int64) ([]dataview.SegmentMembership, bool) {
	memberships := make([]dataview.SegmentMembership, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := m.GetSegment(m.ctx, segmentID)
		if segment == nil {
			return nil, false
		}
		if segment.GetState() == commonpb.SegmentState_Dropped || segment.GetNumOfRows() == 0 {
			continue
		}
		if !isImmediatelyLoadableFlushSegment(segment) {
			return nil, false
		}
		memberships = append(memberships, publishedSegmentMembership(segment))
	}
	return memberships, true
}

func (m *meta) commitDataViewRewrite(
	ctx context.Context,
	collectionID int64,
	addSegmentIDs []int64,
	removeSegmentIDs []int64,
) (*viewpb.DataVersion, error) {
	if m.dataViewManager == nil {
		return nil, nil
	}
	memberships, ready := m.loadablePublishedMemberships(addSegmentIDs)
	if !ready {
		return nil, merr.WrapErrServiceUnavailableMsg(
			"published membership for collection %d is not loadable",
			collectionID,
		)
	}
	return m.dataViewManager.CommitRewrite(ctx, collectionID, PublishedMutation{
		Add:    memberships,
		Remove: append([]int64(nil), removeSegmentIDs...),
	})
}

func (m *meta) commitDataViewTrim(
	ctx context.Context,
	collectionID int64,
	segmentIDs []int64,
	finalize SegmentTrimFinalize,
) (*viewpb.DataVersion, error) {
	if m.dataViewManager == nil {
		return nil, nil
	}
	targets := make([]SegmentTrimTarget, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		targets = append(targets, SegmentTrimTarget{SegmentID: segmentID})
	}
	return m.dataViewManager.CommitSegmentTrim(ctx, collectionID, targets, finalize)
}

func (m *meta) finalizeDataViewTrim(ctx context.Context, collectionID int64, segmentIDs []int64) error {
	m.segMu.Lock()
	defer m.segMu.Unlock()

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	segmentsToDrop := make([]*SegmentInfo, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		// segMu is already held for the complete persistent finalization.
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return merr.WrapErrServiceUnavailableMsg(
				"segment %d disappeared before finalizing DataView trim of collection %d",
				segmentID,
				collectionID,
			)
		}
		if segment.GetCollectionID() != collectionID {
			return merr.WrapErrDataIntegrityMsg(
				"trimmed segment %d belongs to collection %d, requested collection %d",
				segmentID,
				segment.GetCollectionID(),
				collectionID,
			)
		}
		if segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		cloned := segment.Clone()
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
		segmentsToDrop = append(segmentsToDrop, cloned)
	}
	if len(segmentsToDrop) == 0 {
		return nil
	}
	segments := lo.Map(segmentsToDrop, func(segment *SegmentInfo, _ int) *datapb.SegmentInfo {
		return segment.SegmentInfo
	})
	if err := m.catalog.AlterSegments(ctx, segments); err != nil {
		return err
	}
	for _, segment := range segmentsToDrop {
		m.segments.SetSegment(segment.GetID(), segment)
	}
	metricMutation.commit()
	return nil
}

func (m *meta) commitDataViewStreaming(
	ctx context.Context,
	collectionID int64,
	addSegmentIDs []int64,
) (*viewpb.DataVersion, error) {
	if m.dataViewManager == nil {
		return nil, nil
	}
	memberships, ready := m.loadablePublishedMemberships(addSegmentIDs)
	if !ready {
		return nil, merr.WrapErrServiceUnavailableMsg(
			"published membership for collection %d is not loadable",
			collectionID,
		)
	}
	return m.dataViewManager.CommitStreamingView(ctx, collectionID, PublishedMutation{Add: memberships})
}

func (m *meta) segmentIDsForPartition(collectionID int64, partitionIDs map[int64]struct{}) []int64 {
	segments := m.SelectSegments(m.ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		_, ok := partitionIDs[segment.GetPartitionID()]
		return ok
	}))
	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
}

func (m *meta) segmentIDsForTruncate(collectionID int64, vchannel string, flushTs uint64) []int64 {
	segments := m.SelectSegments(m.ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		if segment.GetInsertChannel() != vchannel {
			return false
		}
		effectiveTs := segment.GetCommitTimestamp()
		if effectiveTs == 0 && segment.GetDmlPosition() != nil {
			effectiveTs = segment.GetDmlPosition().GetTimestamp()
		}
		return effectiveTs <= flushTs
	}))
	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
}

func dataViewSegmentMemSize(segment *SegmentInfo) int64 {
	if segment == nil {
		return 0
	}
	var total int64
	for _, fieldBinlog := range segment.GetBinlogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	for _, fieldBinlog := range segment.GetStatslogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	for _, fieldBinlog := range segment.GetBm25Statslogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	return total
}

func fieldBinlogMemSize(fieldBinlog *datapb.FieldBinlog) int64 {
	var total int64
	for _, binlog := range fieldBinlog.GetBinlogs() {
		memorySize := binlog.GetMemorySize()
		if memorySize == 0 {
			memorySize = binlog.GetLogSize()
		}
		total += memorySize
	}
	return total
}
