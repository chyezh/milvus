package growing

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestBuildEnsureGrowingSegmentRequest(t *testing.T) {
	req := buildEnsureGrowingSegmentRequest(&streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		Vchannel:       "v1",
		StorageVersion: 2,
	})

	assert.Equal(t, int64(1), req.CollectionId)
	assert.Equal(t, int64(2), req.PartitionId)
	assert.Equal(t, int64(3), req.SegmentId)
	assert.Equal(t, "v1", req.Vchannel)
	assert.Equal(t, int64(2), req.StorageVersion)
	assert.True(t, req.IsCreatedByStreaming)
}

func TestBuildCommitL1SegmentRequest(t *testing.T) {
	fieldBinlog := &datapb.FieldBinlog{FieldID: 10, Binlogs: []*datapb.Binlog{{LogPath: "insert"}}}
	statsBinlog := &datapb.FieldBinlog{FieldID: 20, Binlogs: []*datapb.Binlog{{LogPath: "stats"}}}
	mergedStats := &datapb.FieldBinlog{FieldID: 21, Binlogs: []*datapb.Binlog{{LogPath: "merged-stats"}}}
	bm25Binlog := &datapb.FieldBinlog{FieldID: 30, Binlogs: []*datapb.Binlog{{LogPath: "bm25"}}}

	req := buildCommitL1SegmentRequest(1000, &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		Vchannel:       "v1",
		StorageVersion: 2,
		Stat: &streamingpb.SegmentAssignmentStat{
			ModifiedRows: 100,
			Level:        datapb.SegmentLevel_L1,
		},
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath:      "manifest",
			MergedStatsBinlog: mergedStats,
			Binlogs:           []*streamingpb.L1SegmentBinLogs{{FieldBinlog: []*datapb.FieldBinlog{fieldBinlog}, StatsBinlog: []*datapb.FieldBinlog{statsBinlog}, Bm25Binlog: []*datapb.FieldBinlog{bm25Binlog}}},
		},
	})

	assert.Equal(t, int64(1000), req.GetBase().GetSourceID())
	assert.Equal(t, int64(3), req.SegmentID)
	assert.Equal(t, "v1", req.Channel)
	assert.True(t, req.Flushed)
	assert.True(t, req.WithFullBinlogs)
	assert.Equal(t, "manifest", req.ManifestPath)
	assert.Equal(t, []*datapb.FieldBinlog{fieldBinlog}, req.Field2BinlogPaths)
	assert.Equal(t, []*datapb.FieldBinlog{statsBinlog, mergedStats}, req.Field2StatslogPaths)
	assert.Equal(t, []*datapb.FieldBinlog{bm25Binlog}, req.Field2Bm25LogPaths)
	assert.Equal(t, int64(100), req.CheckPoints[0].NumOfRows)
}

func TestBuildCommitL0SegmentRequest(t *testing.T) {
	start := &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 10}
	checkpoint := &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 20}
	deltalog := &datapb.FieldBinlog{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "delta"}}}

	req := buildCommitL0SegmentRequest(1000, &L0DeleteBatch{
		VChannel:      "v1",
		CollectionID:  1,
		PartitionID:   2,
		SegmentID:     3,
		Deltalogs:     []*datapb.FieldBinlog{deltalog},
		StartPosition: start,
		Checkpoint:    checkpoint,
	})

	assert.Equal(t, int64(1000), req.GetBase().GetSourceID())
	assert.Equal(t, int64(3), req.SegmentID)
	assert.Equal(t, datapb.SegmentLevel_L0, req.SegLevel)
	assert.True(t, req.Flushed)
	assert.Equal(t, []*datapb.FieldBinlog{deltalog}, req.Deltalogs)
	assert.Equal(t, start, req.StartPositions[0].StartPosition)
	assert.Equal(t, checkpoint, req.CheckPoints[0].Position)
}
