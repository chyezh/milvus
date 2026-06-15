//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDefaultLoadPlanner_BuildsBlockingLoadPlan(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.Settings = &viewpb.QueryViewSettings{RequiredFields: []int64{101, 102}}
	binlog := []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "binlog-1"}}}}
	stats := []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "stats-1"}}}}
	deltas := []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []*datapb.Binlog{{LogPath: "delta-1"}}}}
	segment := &datapb.SegmentInfo{
		ID:              1000,
		CollectionID:    testCollectionID,
		PartitionID:     10,
		InsertChannel:   testVChannel,
		NumOfRows:       100,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 10},
		DmlPosition:     &msgpb.MsgPosition{Timestamp: 20},
		Binlogs:         binlog,
		Statslogs:       stats,
		Deltalogs:       deltas,
		CompactionFrom:  []int64{1, 2},
		Level:           datapb.SegmentLevel_L1,
		StorageVersion:  2,
		IsSorted:        true,
		ManifestPath:    "manifest",
		DataVersion:     3,
		CommitTimestamp: 40,
	}
	indexes := map[int64][]*querypb.FieldIndexInfo{
		1000: {{FieldID: 101, IndexID: 300, IndexName: "vec_idx"}},
	}

	plan, err := NewDefaultLoadPlanner().Build(context.Background(), BuildLoadPlanRequest{
		Meta: meta,
		View: &viewpb.QueryViewOfQueryNode{
			NodeId:     1,
			Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
		},
		Collection: &milvuspb.DescribeCollectionResponse{
			CollectionID:    testCollectionID,
			DbName:          "db",
			UpdateTimestamp: 99,
			Properties:      []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
			Schema: &schemapb.CollectionSchema{
				Name: "coll",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		},
		Segments:       []*datapb.SegmentInfo{segment},
		Indexes:        []*indexpb.IndexInfo{{IndexID: 300, FieldID: 101, IndexName: "vec_idx"}},
		SegmentIndexes: indexes,
	})
	require.NoError(t, err)

	assert.Equal(t, testCollectionID, plan.CollectionID)
	assert.Equal(t, testReplicaID, plan.ReplicaID)
	assert.Equal(t, testVChannel, plan.VChannel)
	assert.Contains(t, plan.Schema.GetProperties(), &commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "true"})
	assert.Contains(t, plan.Schema.GetFields()[0].GetTypeParams(), &commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "true"})
	assert.Equal(t, []int64{101, 102}, plan.LoadMeta.LoadFields)
	assert.Equal(t, querypb.LoadType_LoadCollection, plan.LoadMeta.LoadType)
	assert.Equal(t, uint64(99), plan.LoadMeta.SchemaVersion)
	assert.Equal(t, map[int64][]int64{10: {1000}}, plan.ReadyByPartition)
	require.Len(t, plan.Segments, 1)
	info := plan.Segments[0]
	assert.Equal(t, int64(1000), info.SegmentID)
	require.Len(t, info.BinlogPaths, 1)
	assert.Equal(t, "binlog-1", info.BinlogPaths[0].GetBinlogs()[0].GetLogPath())
	assert.Empty(t, info.Statslogs)
	assert.Empty(t, info.Bm25Logs)
	require.Len(t, info.Deltalogs, 1)
	assert.Equal(t, "delta-1", info.Deltalogs[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, []int64{1, 2}, info.CompactionFrom)
	assert.Equal(t, "manifest", info.ManifestPath)
	assert.Equal(t, int32(3), info.DataVersion)
	assert.Equal(t, uint64(40), info.CommitTimestamp)
	require.Len(t, info.IndexInfos, 1)
	assert.Equal(t, int64(300), info.IndexInfos[0].GetIndexID())
	assert.Contains(t, info.IndexInfos[0].GetIndexParams(), &commonpb.KeyValuePair{Key: common.LoadPriorityKey, Value: commonpb.LoadPriority_HIGH.String()})
	require.Len(t, plan.IndexInfos, 1)
	assert.Equal(t, int64(300), plan.IndexInfos[0].GetIndexID())
	assert.Equal(t, int64(101), plan.IndexInfos[0].GetFieldID())
	assert.Equal(t, commonpb.LoadPriority_HIGH, info.Priority)
}
