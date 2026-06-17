package idf

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	utilmock "github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestProviderSkipsWhenLoadedFieldsDoNotNeedBM25(t *testing.T) {
	provider := NewProvider(nil)

	runtime, err := provider.BuildInitial(context.Background(), newLoadResourceDescriptor(
		0,
		"",
		qviews.DataVersion{},
		&schemapb.CollectionSchema{
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{100}},
			},
		},
		&viewpb.QueryViewSettings{RequiredFields: []int64{200}},
	))

	require.NoError(t, err)
	require.Empty(t, runtime.Resources)
}

func TestProviderLoadsBM25Resources(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	bm25Binlog := &datapb.FieldBinlog{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{
			{LogPath: "bm25/log/1"},
		},
	}
	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
			Bm25Resources: []*querypb.StreamingNodeBM25Resource{
				{
					SegmentId:      100,
					PartitionId:    10,
					Bm25Binlogs:    []*datapb.FieldBinlog{bm25Binlog},
					StorageVersion: 3,
					ManifestPath:   "bm25/manifest",
				},
			},
		},
	})

	runtime, err := provider.BuildInitial(context.Background(), newLoadResourceDescriptor(
		1,
		"ch",
		version,
		&schemapb.CollectionSchema{
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{100}},
			},
		},
		&viewpb.QueryViewSettings{RequiredFields: []int64{100}},
		walview.VisibleSegment{SegmentID: 10},
		walview.VisibleSegment{SegmentID: 11},
	))

	require.NoError(t, err)
	require.Len(t, runtime.Resources, 1)
	require.Equal(t, int64(100), runtime.Resources[0].SegmentID)
	require.Equal(t, int64(10), runtime.Resources[0].PartitionID)
	require.Equal(t, int64(3), runtime.Resources[0].StorageVersion)
	require.Equal(t, "bm25/manifest", runtime.Resources[0].ManifestPath)
	require.Equal(t, []*datapb.FieldBinlog{bm25Binlog}, runtime.Resources[0].BM25Binlogs)
	require.Equal(t, []int64{10, 11}, runtime.GrowingSegmentIDs)
}

func TestProviderBuildsBM25OracleFromSealedAndGrowingStats(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	sealedStats := storage.NewBM25Stats()
	sealedStats.Append(
		map[uint32]float32{7: 1},
		map[uint32]float32{8: 1},
	)
	sealedBytes, err := sealedStats.Serialize()
	require.NoError(t, err)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	sealedPath := filepath.Join(cm.RootPath(), "bm25", "100", "0")
	require.NoError(t, cm.Write(context.Background(), sealedPath, sealedBytes))

	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
			Bm25Resources: []*querypb.StreamingNodeBM25Resource{
				{
					SegmentId:   100,
					PartitionId: 10,
					Bm25Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{LogPath: sealedPath},
							},
						},
					},
				},
			},
		},
	}, WithChunkManager(cm))
	schema := bm25TestSchema()
	growingRow := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})
	runtime, err := provider.BuildInitial(context.Background(), viewresource.LoadResourceDescriptor{
		WALView: walview.VChannelWALView{
			CollectionID: 1,
			VChannel:     "ch",
			Schema:       schema,
			LoadConfig: &streamingpb.VChannelLoadConfig{
				Header: &messagespb.AlterLoadConfigMessageHeader{
					LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
				},
			},
			SegmentSnapshot: walview.VisibleSegmentSnapshot{
				DataVersion: version,
				Segments: []walview.VisibleSegment{
					{
						SegmentID:   10,
						PartitionID: 10,
						Data: walview.SegmentSnapshotData{
							InsertMessages: []message.ImmutableMessage{
								newBM25InsertMessage(t, "ch", 10, 30, growingRow),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	expected := storage.NewBM25Stats()
	expected.Merge(sealedStats)
	expected.Append(map[uint32]float32{7: 1})
	tfs := &schemapb.SparseFloatArray{
		Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})},
		Dim:      9,
	}
	idfs, avgdl, err := runtime.Oracle.BuildIDF(100, tfs)
	require.NoError(t, err)
	require.Equal(t, [][]byte{expected.BuildIDF(tfs.GetContents()[0])}, idfs)
	require.Equal(t, expected.GetAvgdl(), avgdl)
}

func TestProviderReusesSealedBM25StatsWhileRuntimeRetained(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	sealedStats := storage.NewBM25Stats()
	sealedStats.Append(map[uint32]float32{7: 1})
	sealedBytes, err := sealedStats.Serialize()
	require.NoError(t, err)
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	sealedPath := filepath.Join(cm.RootPath(), "bm25", "100", "0")
	require.NoError(t, cm.Write(context.Background(), sealedPath, sealedBytes))
	countingCM := &countingChunkManager{ChunkManager: cm}

	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
			Bm25Resources: []*querypb.StreamingNodeBM25Resource{
				{
					SegmentId:   100,
					PartitionId: 10,
					Bm25Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{LogPath: sealedPath},
							},
						},
					},
				},
			},
		},
	}, WithChunkManager(countingCM))
	desc := newLoadResourceDescriptor(
		1,
		"ch",
		version,
		bm25TestSchema(),
		&viewpb.QueryViewSettings{RequiredFields: []int64{100}},
	)

	runtimeA, err := provider.BuildInitial(context.Background(), desc)
	require.NoError(t, err)
	require.Equal(t, int64(1), countingCM.readCount.Load())

	runtimeB, err := provider.BuildInitial(context.Background(), desc)
	require.NoError(t, err)
	require.Equal(t, int64(1), countingCM.readCount.Load())

	runtimeA.Close()
	runtimeB.Close()

	_, err = provider.BuildInitial(context.Background(), desc)
	require.NoError(t, err)
	require.Equal(t, int64(2), countingCM.readCount.Load())
}

func TestProviderRuntimeAppliesLiveGrowingBM25Stats(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
		},
	})
	manager := viewresource.NewManager(viewresource.SnapshotGrowingSegmentRuntimeBuilder{
		NewApplier: func(context.Context, viewresource.LoadResourceDescriptor) (viewresource.GrowingRuntimeApplier, error) {
			return noopGrowingApplier{}, nil
		},
	}, provider)
	schema := bm25TestSchema()
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		Schema:       schema,
		LoadConfig: &streamingpb.VChannelLoadConfig{
			Header: &messagespb.AlterLoadConfigMessageHeader{
				LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
			},
		},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
			Segments: []walview.VisibleSegment{
				{
					SegmentID: 10,
					Data: walview.SegmentSnapshotData{
						InsertMessages: []message.ImmutableMessage{
							newBM25InsertMessage(t, "ch", 10, 30, typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})),
						},
					},
				},
			},
		},
	})
	require.NotNil(t, observer)
	defer manager.Close()
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(viewresource.ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	tf := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{8: 1})
	before, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
	require.NoError(t, err)
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newBM25InsertMessage(t, "ch", 99, 39, tf)}))
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 39
	}, time.Second, 10*time.Millisecond)
	afterInactiveSegment, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
	require.NoError(t, err)
	require.Equal(t, before, afterInactiveSegment)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newCreateSegmentMessage(t, "ch", 99, 40)}))
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newBM25InsertMessage(t, "ch", 99, 41, tf)}))
	require.Eventually(t, func() bool {
		after, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
		return err == nil && string(after[0]) != string(before[0])
	}, time.Second, 10*time.Millisecond)
}

func TestProviderRuntimeRejectsLiveGrowingInsertAfterFlush(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
		},
	})
	schema := bm25TestSchema()
	runtime, err := provider.BuildInitial(context.Background(), viewresource.LoadResourceDescriptor{
		WALView: walview.VChannelWALView{
			CollectionID: 1,
			VChannel:     "ch",
			Schema:       schema,
			LoadConfig: &streamingpb.VChannelLoadConfig{
				Header: &messagespb.AlterLoadConfigMessageHeader{
					LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
				},
			},
			SegmentSnapshot: walview.VisibleSegmentSnapshot{
				DataVersion: version,
			},
		},
	})
	require.NoError(t, err)

	segmentID := int64(99)
	require.NoError(t, runtime.ApplyLiveMessage(context.Background(), newCreateSegmentMessage(t, "ch", segmentID, 40)))
	require.NoError(t, runtime.ApplyLiveMessage(context.Background(), newBM25FlushMessage(t, "ch", segmentID, 41)))
	err = runtime.ApplyLiveMessage(context.Background(), newBM25InsertMessage(
		t,
		"ch",
		segmentID,
		42,
		typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{8: 1}),
	))
	require.ErrorContains(t, err, "already flushed")
}

func TestProviderRuntimeCatchupWaitsForManagerHandoff(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 1,
			Vchannel:     "ch",
			DataVersion:  version.IntoProto(),
		},
	})
	schema := bm25TestSchema()
	runtime, err := provider.BuildInitial(context.Background(), viewresource.LoadResourceDescriptor{
		WALView: walview.VChannelWALView{
			CollectionID: 1,
			VChannel:     "ch",
			Schema:       schema,
			LoadConfig: &streamingpb.VChannelLoadConfig{
				Header: &messagespb.AlterLoadConfigMessageHeader{
					LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
				},
			},
			SegmentSnapshot: walview.VisibleSegmentSnapshot{
				DataVersion: version,
			},
		},
	})
	require.NoError(t, err)
	defer runtime.Close()
	select {
	case <-runtime.CatchupDone():
		t.Fatal("provider closed catchup before resource manager live handoff")
	default:
	}

	manager := viewresource.NewManager(viewresource.SnapshotGrowingSegmentRuntimeBuilder{
		NewApplier: func(context.Context, viewresource.LoadResourceDescriptor) (viewresource.GrowingRuntimeApplier, error) {
			return noopGrowingApplier{}, nil
		},
	}, provider)
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		Schema:       schema,
		LoadConfig: &streamingpb.VChannelLoadConfig{
			Header: &messagespb.AlterLoadConfigMessageHeader{
				LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
			},
		},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	viewRuntime, ready, err := manager.GetViewRuntime(viewresource.ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	select {
	case <-viewRuntime.BM25.CatchupDone():
	default:
		t.Fatal("resource manager did not close catchup after live handoff")
	}
}

func TestProviderRuntimeAdvancementRemovesLiveGrowingStats(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	provider := NewProvider(&versionedBM25QueryCoordClient{})
	manager := viewresource.NewManager(viewresource.SnapshotGrowingSegmentRuntimeBuilder{
		NewApplier: func(context.Context, viewresource.LoadResourceDescriptor) (viewresource.GrowingRuntimeApplier, error) {
			return noopGrowingApplier{}, nil
		},
	}, provider)
	schema := bm25TestSchema()
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		Schema:       schema,
		LoadConfig: &streamingpb.VChannelLoadConfig{
			Header: &messagespb.AlterLoadConfigMessageHeader{
				LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}},
			},
		},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(viewresource.ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	tf := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{8: 1})
	before, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
	require.NoError(t, err)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newCreateSegmentMessage(t, "ch", 20, 40)}))
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newBM25InsertMessage(t, "ch", 20, 41, tf)}))
	require.Eventually(t, func() bool {
		after, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
		return err == nil && string(after[0]) != string(before[0])
	}, time.Second, 10*time.Millisecond)

	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	runtime.BM25.ApplySegmentSealed(20, target)
	runtime.BM25.MaybeAdvance(target)
	require.Eventually(t, func() bool {
		after, _, err := runtime.BM25.Oracle.BuildIDF(100, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: 9})
		return err == nil && string(after[0]) == string(before[0])
	}, time.Second, 10*time.Millisecond)
}

func TestProviderRejectsMismatchedBM25ResourceResponse(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	provider := NewProvider(&bm25QueryCoordClient{
		resp: &querypb.GetStreamingNodeQueryViewResourcesResponse{
			CollectionId: 2,
			Vchannel:     "other",
			DataVersion:  qviews.DataVersion{StreamingVersion: 11}.IntoProto(),
		},
	})

	runtime, err := provider.BuildInitial(context.Background(), newLoadResourceDescriptor(
		1,
		"ch",
		version,
		&schemapb.CollectionSchema{
			Functions: []*schemapb.FunctionSchema{
				{Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{100}},
			},
		},
		&viewpb.QueryViewSettings{RequiredFields: []int64{100}},
	))

	require.ErrorContains(t, err, "bm25 resource response mismatch")
	require.Nil(t, runtime)
}

func newLoadResourceDescriptor(
	collectionID int64,
	vchannel string,
	version qviews.DataVersion,
	schema *schemapb.CollectionSchema,
	settings *viewpb.QueryViewSettings,
	segments ...walview.VisibleSegment,
) viewresource.LoadResourceDescriptor {
	header := &messagespb.AlterLoadConfigMessageHeader{
		CollectionId: collectionID,
		PartitionIds: append([]int64{}, settings.GetRequiredPartitions()...),
	}
	for _, fieldID := range settings.GetRequiredFields() {
		header.LoadFields = append(header.LoadFields, &messagespb.LoadFieldConfig{FieldId: fieldID})
	}
	return viewresource.LoadResourceDescriptor{
		WALView: walview.VChannelWALView{
			CollectionID: collectionID,
			VChannel:     vchannel,
			Schema:       schema,
			LoadConfig:   &streamingpb.VChannelLoadConfig{Header: header},
			SegmentSnapshot: walview.VisibleSegmentSnapshot{
				CollectionID: collectionID,
				VChannel:     vchannel,
				DataVersion:  version,
				Segments:     segments,
			},
		},
	}
}

func bm25TestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "bm25_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 100, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{2},
				OutputFieldIds: []int64{100},
			},
		},
	}
}

func newBM25InsertMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64, rows ...[]byte) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        uint64(len(rows)),
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: 1,
			PartitionID:  10,
			SegmentID:    segmentID,
			NumRows:      uint64(len(rows)),
			Version:      msgpb.InsertDataVersion_ColumnBased,
			RowIDs:       []int64{1},
			Timestamps:   []uint64{timetick},
			FieldsData: []*schemapb.FieldData{
				{
					Type:    schemapb.DataType_Int64,
					FieldId: 1,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
					}},
				},
				{
					Type:    schemapb.DataType_VarChar,
					FieldId: 2,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"text"}}},
					}},
				},
				{
					Type:    schemapb.DataType_SparseFloatVector,
					FieldId: 100,
					Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
						Data: &schemapb.VectorField_SparseFloatVector{
							SparseFloatVector: &schemapb.SparseFloatArray{Contents: rows, Dim: 9},
						},
					}},
				},
			},
		}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newCreateSegmentMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId: 1,
			PartitionId:  10,
			SegmentId:    segmentID,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newBM25FlushMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 1,
			PartitionId:  10,
			SegmentId:    segmentID,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

type noopGrowingApplier struct{}

func (noopGrowingApplier) LoadPersistedSegment(context.Context, walview.VisibleSegment) error {
	return nil
}

func (noopGrowingApplier) ApplySnapshotInsert(context.Context, walview.VisibleSegment, message.ImmutableMessage) error {
	return nil
}

func (noopGrowingApplier) ApplyDeleteReplay(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (noopGrowingApplier) ApplyLiveMessage(context.Context, message.ImmutableMessage) error {
	return nil
}

func (noopGrowingApplier) Close() {}

type countingChunkManager struct {
	storage.ChunkManager
	readCount atomic.Int64
}

func (m *countingChunkManager) Read(ctx context.Context, path string) ([]byte, error) {
	m.readCount.Add(1)
	return m.ChunkManager.Read(ctx, path)
}

type bm25QueryCoordClient struct {
	utilmock.GrpcQueryCoordClient
	resp *querypb.GetStreamingNodeQueryViewResourcesResponse
}

func (c *bm25QueryCoordClient) GetStreamingNodeQueryViewResources(
	context.Context,
	*querypb.GetStreamingNodeQueryViewResourcesRequest,
	...grpc.CallOption,
) (*querypb.GetStreamingNodeQueryViewResourcesResponse, error) {
	return c.resp, nil
}

type versionedBM25QueryCoordClient struct {
	utilmock.GrpcQueryCoordClient
}

func (versionedBM25QueryCoordClient) GetStreamingNodeQueryViewResources(
	_ context.Context,
	req *querypb.GetStreamingNodeQueryViewResourcesRequest,
	_ ...grpc.CallOption,
) (*querypb.GetStreamingNodeQueryViewResourcesResponse, error) {
	return &querypb.GetStreamingNodeQueryViewResourcesResponse{
		CollectionId: req.GetCollectionId(),
		Vchannel:     req.GetVchannel(),
		DataVersion:  req.GetDataVersion(),
	}, nil
}
