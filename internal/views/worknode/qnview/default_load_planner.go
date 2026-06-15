package qnview

import (
	"context"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"google.golang.org/protobuf/proto"
)

type DefaultLoadPlanner struct{}

func NewDefaultLoadPlanner() *DefaultLoadPlanner {
	return &DefaultLoadPlanner{}
}

func (p *DefaultLoadPlanner) Build(_ context.Context, req BuildLoadPlanRequest) (*LoadPlan, error) {
	if req.Meta == nil {
		return nil, fmt.Errorf("query view meta is nil")
	}
	if req.View == nil {
		return nil, fmt.Errorf("query node view is nil")
	}
	if req.Collection == nil || req.Collection.GetSchema() == nil {
		return nil, fmt.Errorf("collection metadata is incomplete")
	}

	segmentByID := make(map[int64]*datapb.SegmentInfo, len(req.Segments))
	for _, segment := range req.Segments {
		segmentByID[segment.GetID()] = segment
	}

	loadInfos := make([]*querypb.SegmentLoadInfo, 0, len(segmentPartitionMap(req.View)))
	for _, partition := range req.View.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			segment, ok := segmentByID[segmentID]
			if !ok {
				return nil, fmt.Errorf("segment %d metadata is missing", segmentID)
			}
			loadInfos = append(loadInfos, packQVSegmentLoadInfo(segment, req.SegmentIndexes[segmentID]))
		}
	}

	schema := applyQVCollectionSettings(req.Collection.GetSchema(), req.Collection.GetProperties())
	return &LoadPlan{
		CollectionID: req.Meta.GetCollectionId(),
		ReplicaID:    req.Meta.GetReplicaId(),
		VChannel:     req.Meta.GetVchannel(),
		Schema:       schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:      querypb.LoadType_LoadCollection,
			CollectionID:  req.Meta.GetCollectionId(),
			PartitionIDs:  viewPartitionIDs(req.View),
			DbName:        req.Collection.GetDbName(),
			LoadFields:    append([]int64(nil), req.Meta.GetSettings().GetRequiredFields()...),
			SchemaVersion: req.Collection.GetUpdateTimestamp(),
		},
		IndexInfos:       cloneIndexInfos(req.Indexes),
		Segments:         loadInfos,
		ReadyByPartition: readyByPartition(req.View),
	}, nil
}

func applyQVCollectionSettings(schema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair) *schemapb.CollectionSchema {
	schemaCloned := proto.Clone(schema).(*schemapb.CollectionSchema)
	schemaCloned.Properties = mergeQVCollectionProperties(schemaCloned.GetProperties(), collectionProperties)

	collectionMmapEnabled, exist := common.IsMmapDataEnabled(collectionProperties...)
	if !exist {
		return schemaCloned
	}
	for _, field := range schemaCloned.GetFields() {
		if common.FieldHasMmapKey(schemaCloned, field.GetFieldID()) {
			continue
		}
		field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
			Key:   common.MmapEnabledKey,
			Value: strconv.FormatBool(collectionMmapEnabled),
		})
	}
	for _, structField := range schemaCloned.GetStructArrayFields() {
		structTypeParams := structField.GetTypeParams()
		structMmapEnabled, structExist := common.IsMmapDataEnabled(structTypeParams...)
		if !structExist && !common.FieldHasMmapKey(schemaCloned, structField.GetFieldID()) {
			structField.TypeParams = append(structField.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MmapEnabledKey,
				Value: strconv.FormatBool(collectionMmapEnabled),
			})
			structMmapEnabled = collectionMmapEnabled
			structExist = true
		}
		for _, field := range structField.GetFields() {
			if common.FieldHasMmapKey(schemaCloned, field.GetFieldID()) {
				continue
			}
			mmapEnabled := collectionMmapEnabled
			if structExist {
				mmapEnabled = structMmapEnabled
			}
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MmapEnabledKey,
				Value: strconv.FormatBool(mmapEnabled),
			})
		}
	}
	return schemaCloned
}

func mergeQVCollectionProperties(schemaProperties []*commonpb.KeyValuePair, collectionProperties []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string, len(schemaProperties)+len(collectionProperties))
	for _, property := range collectionProperties {
		props[property.GetKey()] = property.GetValue()
	}
	for _, property := range schemaProperties {
		props[property.GetKey()] = property.GetValue()
	}
	merged := make([]*commonpb.KeyValuePair, 0, len(props))
	for key, value := range props {
		merged = append(merged, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	return merged
}

func packQVSegmentLoadInfo(segment *datapb.SegmentInfo, indexes []*querypb.FieldIndexInfo) *querypb.SegmentLoadInfo {
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:       segment.GetID(),
		PartitionID:     segment.GetPartitionID(),
		CollectionID:    segment.GetCollectionID(),
		BinlogPaths:     cloneFieldBinlogs(segment.GetBinlogs()),
		NumOfRows:       segment.GetNumOfRows(),
		Deltalogs:       cloneFieldBinlogs(segment.GetDeltalogs()),
		CompactionFrom:  append([]int64(nil), segment.GetCompactionFrom()...),
		IndexInfos:      cloneFieldIndexInfos(indexes),
		InsertChannel:   segment.GetInsertChannel(),
		StartPosition:   cloneMsgPosition(segment.GetStartPosition()),
		DeltaPosition:   cloneMsgPosition(segment.GetDmlPosition()),
		Level:           segment.GetLevel(),
		StorageVersion:  segment.GetStorageVersion(),
		IsSorted:        segment.GetIsSorted(),
		Priority:        commonpb.LoadPriority_HIGH,
		ManifestPath:    segment.GetManifestPath(),
		DataVersion:     segment.GetDataVersion(),
		CommitTimestamp: segment.GetCommitTimestamp(),
	}
	if segment.GetManifestPath() == "" {
		loadInfo.Statslogs = cloneFieldBinlogs(segment.GetStatslogs())
		loadInfo.TextStatsLogs = cloneTextStats(segment.GetTextStatsLogs())
		loadInfo.Bm25Logs = cloneFieldBinlogs(segment.GetBm25Statslogs())
		loadInfo.JsonKeyStatsLogs = cloneJSONKeyStats(segment.GetJsonKeyStats())
	}
	return loadInfo
}

func cloneMsgPosition(pos *msgpb.MsgPosition) *msgpb.MsgPosition {
	if pos == nil {
		return nil
	}
	return proto.Clone(pos).(*msgpb.MsgPosition)
}

func cloneFieldBinlogs(in []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(in) == 0 {
		return nil
	}
	out := make([]*datapb.FieldBinlog, 0, len(in))
	for _, field := range in {
		out = append(out, proto.Clone(field).(*datapb.FieldBinlog))
	}
	return out
}

func cloneFieldIndexInfos(in []*querypb.FieldIndexInfo) []*querypb.FieldIndexInfo {
	if len(in) == 0 {
		return nil
	}
	out := make([]*querypb.FieldIndexInfo, 0, len(in))
	for _, index := range in {
		cloned := proto.Clone(index).(*querypb.FieldIndexInfo)
		cloned.IndexParams = append(cloned.IndexParams, &commonpb.KeyValuePair{
			Key:   common.LoadPriorityKey,
			Value: commonpb.LoadPriority_HIGH.String(),
		})
		out = append(out, cloned)
	}
	return out
}

func cloneIndexInfos(in []*indexpb.IndexInfo) []*indexpb.IndexInfo {
	if len(in) == 0 {
		return nil
	}
	out := make([]*indexpb.IndexInfo, 0, len(in))
	for _, index := range in {
		out = append(out, proto.Clone(index).(*indexpb.IndexInfo))
	}
	return out
}

func cloneTextStats(in map[int64]*datapb.TextIndexStats) map[int64]*datapb.TextIndexStats {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int64]*datapb.TextIndexStats, len(in))
	for fieldID, stats := range in {
		out[fieldID] = proto.Clone(stats).(*datapb.TextIndexStats)
	}
	return out
}

func cloneJSONKeyStats(in map[int64]*datapb.JsonKeyStats) map[int64]*datapb.JsonKeyStats {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int64]*datapb.JsonKeyStats, len(in))
	for fieldID, stats := range in {
		out[fieldID] = proto.Clone(stats).(*datapb.JsonKeyStats)
	}
	return out
}

func viewPartitionIDs(view *viewpb.QueryViewOfQueryNode) []int64 {
	partitions := make([]int64, 0, len(view.GetPartitions()))
	for _, partition := range view.GetPartitions() {
		partitions = append(partitions, partition.GetPartitionId())
	}
	return partitions
}
