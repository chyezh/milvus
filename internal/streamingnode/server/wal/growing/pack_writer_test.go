package growing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestGrowingBulkPackWriterFlushInsertBufferBuildsPersistedStorage(t *testing.T) {
	schema := newTestGrowingPackWriterSchema()
	writer := &growingBulkPackWriter{
		writeFn: func(_ context.Context, req *growingBulkWriteRequest) (*growingBulkWriteResult, error) {
			require.Equal(t, storage.StorageV2, req.storageVersion)
			require.Len(t, req.insertData, 1)
			assert.Equal(t, 2, req.insertData[0].GetRowNum())
			assert.NotNil(t, req.syncPack)
			require.NotEmpty(t, req.currentSplit)
			for _, columnGroup := range req.currentSplit {
				assert.NotEmpty(t, columnGroup.Fields)
				assert.NotEmpty(t, columnGroup.Columns)
			}
			return &growingBulkWriteResult{
				insertBinlogs: map[int64]*datapb.FieldBinlog{
					100: {FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "insert-100"}}},
				},
				statsBinlogs: map[int64]*datapb.FieldBinlog{
					100: {FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "stats-100"}}},
				},
				manifestPath: "manifest-2",
			}, nil
		},
	}
	pack := &FlushPack{
		Meta: &streamingpb.SegmentAssignmentMeta{
			CollectionId:     1,
			PartitionId:      2,
			SegmentId:        10,
			Vchannel:         "v1",
			StorageVersion:   storage.StorageV2,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: "manifest-1"},
		},
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    10,
		VChannel:     "v1",
		FromTimeTick: 5,
		ToTimeTick:   6,
		Schema:       schema,
		Rows:         2,
		Inserts: []InsertEntry{
			{
				timeTick:   6,
				assignment: &messagespb.PartitionSegmentAssignment{PartitionId: 2, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 10}},
				request:    newTestGrowingPackWriterInsertRequest(),
			},
		},
	}

	result, err := writer.FlushInsertBuffer(context.Background(), pack)
	require.NoError(t, err)
	require.NotNil(t, result.PersistedStorage)
	assert.Equal(t, "manifest-2", result.PersistedStorage.GetManifestPath())
	require.Len(t, result.PersistedStorage.GetBinlogs(), 1)
	binlogs := result.PersistedStorage.GetBinlogs()[0]
	assert.Equal(t, uint64(5), binlogs.GetFromTimeTick())
	assert.Equal(t, uint64(6), binlogs.GetToTimeTick())
	assert.Equal(t, "insert-100", binlogs.GetFieldBinlog()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, "stats-100", binlogs.GetStatsBinlog()[0].GetBinlogs()[0].GetLogPath())
}

func newTestGrowingPackWriterSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "2"},
				},
			},
		},
	}
}

func newTestGrowingPackWriterInsertRequest() *msgpb.InsertRequest {
	return &msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Insert,
		},
		ShardName:    "v1",
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    10,
		Version:      msgpb.InsertDataVersion_ColumnBased,
		FieldsData: []*schemapb.FieldData{
			{
				FieldId: 100,
				Type:    schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2}},
						},
					},
				},
			},
			{
				FieldId: 101,
				Type:    schemapb.DataType_FloatVector,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 2,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: []float32{0.1, 0.2, 0.3, 0.4}},
						},
					},
				},
			},
		},
		RowIDs:     []int64{11, 12},
		Timestamps: []uint64{5, 6},
		NumRows:    2,
	}
}
