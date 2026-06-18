package viewresource

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ walview.LoadConfigListener = (*DefaultManager)(nil)

func TestManagerInterfaceUsesWALViewBoundary(t *testing.T) {
	_, ok := reflect.TypeOf((*Manager)(nil)).Elem().MethodByName("BuildInitial")
	require.False(t, ok)
}

func TestLoadResourceDescriptorDoesNotDuplicateWALViewFields(t *testing.T) {
	descType := reflect.TypeOf(LoadResourceDescriptor{})
	for _, field := range []string{"CollectionID", "VChannel", "DataVersion", "Settings", "Schema"} {
		_, ok := descType.FieldByName(field)
		require.False(t, ok, "LoadResourceDescriptor should derive %s from WALView", field)
	}
}

func testAlterLoadConfigView(collectionID int64, vchannel string, version qviews.DataVersion, settings *viewpb.QueryViewSettings) walview.VChannelWALView {
	header := &messagespb.AlterLoadConfigMessageHeader{
		CollectionId: collectionID,
	}
	if settings != nil {
		header.PartitionIds = append([]int64{}, settings.GetRequiredPartitions()...)
		for _, fieldID := range settings.GetRequiredFields() {
			header.LoadFields = append(header.LoadFields, &messagespb.LoadFieldConfig{FieldId: fieldID})
		}
	}
	return walview.VChannelWALView{
		CollectionID: collectionID,
		VChannel:     vchannel,
		LoadConfig:   &streamingpb.VChannelLoadConfig{Header: header},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: collectionID,
			VChannel:     vchannel,
			DataVersion:  version,
		},
	}
}

type fakeGrowingSegmentRuntimeBuilder struct {
	mu    sync.Mutex
	calls []LoadResourceDescriptor
	block chan struct{}
	seen  chan struct{}
}

func (p *fakeGrowingSegmentRuntimeBuilder) Build(_ context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, desc)
	return &GrowingRuntime{}, nil
}

type cancelAwareGrowingSegmentRuntimeBuilder struct {
	started  chan struct{}
	canceled chan struct{}
}

func (p *cancelAwareGrowingSegmentRuntimeBuilder) Build(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
	close(p.started)
	<-ctx.Done()
	close(p.canceled)
	return nil, ctx.Err()
}

type fakeIDFOracleRuntimeBuilder struct {
	mu    sync.Mutex
	calls []LoadResourceDescriptor
}

func (p *fakeIDFOracleRuntimeBuilder) BuildInitial(_ context.Context, desc LoadResourceDescriptor) (*BM25Runtime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, desc)
	runtime := &BM25Runtime{Resources: []*BM25SegmentResource{{SegmentID: 20}}}
	runtime.MarkCatchupDone()
	return runtime, nil
}

type errorIDFOracleRuntimeBuilder struct {
	err error
}

func (p errorIDFOracleRuntimeBuilder) BuildInitial(context.Context, LoadResourceDescriptor) (*BM25Runtime, error) {
	return nil, p.err
}

func initSegcoreForViewResourceTest(t *testing.T) {
	t.Helper()
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
}

func newTestInsertMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newTestSegmentInsertMessage(t *testing.T, vchannel string, segmentID int64, rowCount int, timetick uint64, schema *schemapb.CollectionSchema) message.ImmutableMessage {
	t.Helper()
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID:  1,
		Schema:        schema,
		LoadFieldList: nil,
	})
	require.NoError(t, err)
	defer collection.Release()

	insertMsg, err := mock_segcore.GenInsertMsg(collection, 10, segmentID, rowCount)
	require.NoError(t, err)
	insertMsg.ShardName = vchannel
	insertMsg.CollectionID = 1
	insertMsg.PartitionID = 10
	insertMsg.SegmentID = segmentID

	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        uint64(rowCount),
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(insertMsg.InsertRequest).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestFlushMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
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
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestCreateSegmentMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
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
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestInsertTxnMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	insert := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
	return newTestTxnMessage(t, vchannel, timetick, insert.
		WithTimeTick(timetick-1).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick-1))))
}

func newTestDeleteTxnMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	deleted := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return newTestTxnMessage(t, vchannel, timetick, deleted.
		WithTimeTick(timetick-1).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick-1))))
}

func newTestTxnMessage(t *testing.T, vchannel string, timetick uint64, innerMessages ...message.MutableMessage) message.ImmutableMessage {
	t.Helper()
	txnCtx := message.TxnContext{
		TxnID:     message.TxnID(timetick),
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	beginMsg := message.MustAsImmutableBeginTxnMessageV2(begin.
		WithTxnContext(txnCtx).
		WithTimeTick(timetick - 2).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick - 2))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick - 2))))

	builder := message.NewImmutableTxnMessageBuilder(beginMsg)
	for i, inner := range innerMessages {
		builder.Add(inner.
			WithTxnContext(txnCtx).
			IntoImmutableMessage(rmq.NewRmqID(int64(timetick - 1 + uint64(i)))))
	}
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()
	commitMsg := message.MustAsImmutableCommitTxnMessageV2(commit.
		WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1))))

	txn, err := builder.Build(commitMsg)
	require.NoError(t, err)
	return txn
}

type fakeTransformLogScanner struct {
	ch     chan wal.TransformLogEvent
	done   chan struct{}
	err    error
	closed bool
}

func newFakeTransformLogScanner() *fakeTransformLogScanner {
	return &fakeTransformLogScanner{
		ch:   make(chan wal.TransformLogEvent, 16),
		done: make(chan struct{}),
	}
}

func (s *fakeTransformLogScanner) Name() string {
	return "fake"
}

func (s *fakeTransformLogScanner) Chan() <-chan wal.TransformLogEvent {
	return s.ch
}

func (s *fakeTransformLogScanner) Error() error {
	return s.err
}

func (s *fakeTransformLogScanner) Done() <-chan struct{} {
	return s.done
}

func (s *fakeTransformLogScanner) Close() error {
	s.closed = true
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return s.err
}

func TestManagerOnAlterLoadConfigBuildsWALViewDataVersion(t *testing.T) {
	growing := &fakeGrowingSegmentRuntimeBuilder{}
	bm25 := &fakeIDFOracleRuntimeBuilder{}
	manager := NewManager(growing, bm25)

	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "by-dev-rootcoord-dml_0_1v0",
		LoadConfig: &streamingpb.VChannelLoadConfig{
			Header: &messagespb.AlterLoadConfigMessageHeader{
				PartitionIds: []int64{10},
				LoadFields: []*messagespb.LoadFieldConfig{
					{FieldId: 100},
				},
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

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "by-dev-rootcoord-dml_0_1v0",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.NotNil(t, runtime)

	growing.mu.Lock()
	require.Len(t, growing.calls, 1)
	require.Equal(t, version, growing.calls[0].DataVersion())
	require.Equal(t, []int64{10}, growing.calls[0].Settings().GetRequiredPartitions())
	require.Equal(t, []int64{100}, growing.calls[0].Settings().GetRequiredFields())
	growing.mu.Unlock()

	bm25.mu.Lock()
	require.Len(t, bm25.calls, 1)
	require.Equal(t, version, bm25.calls[0].DataVersion())
	bm25.mu.Unlock()
}

func TestManagerDefaultGrowingRuntimeBuilderUsesWALViewSegmentSnapshot(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
			Segments: []walview.VisibleSegment{
				{SegmentID: 10},
				{SegmentID: 11},
			},
		},
	})
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, []int64{10, 11}, runtime.Growing.SegmentIDs())
}

func TestManagerDefaultGrowingRuntimeBuilderBuildsSegcoreGrowingSegmentFromSnapshotInsert(t *testing.T) {
	initSegcoreForViewResourceTest(t)

	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	schema := mock_segcore.GenTestCollectionSchema("snview-resource", schemapb.DataType_Int64, false)
	segmentID := int64(10)
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:        1,
		VChannel:            "ch",
		BaseGrowingTimeTick: 30,
		Schema:              schema,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 1,
			VChannel:     "ch",
			DataVersion:  version,
			Segments: []walview.VisibleSegment{
				{
					SegmentID:   segmentID,
					PartitionID: 10,
					Schema:      schema,
					Data: walview.SegmentSnapshotData{
						InsertMessages: []message.ImmutableMessage{
							newTestSegmentInsertMessage(t, "ch", segmentID, 3, 30, schema),
						},
					},
				},
			},
		},
	})
	require.NotNil(t, observer)

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	segment, ok := runtime.Growing.Segment(segmentID)
	require.True(t, ok)
	require.Equal(t, int64(3), segment.RowNum())

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{CollectionID: 1, VChannel: "ch"})
}

func TestManagerDefaultGrowingRuntimeBuilderAppliesLiveInsertToNewSegcoreSegment(t *testing.T) {
	initSegcoreForViewResourceTest(t)

	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	schema := mock_segcore.GenTestCollectionSchema("snview-resource-live", schemapb.DataType_Int64, false)
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:        1,
		VChannel:            "ch",
		BaseGrowingTimeTick: 30,
		Schema:              schema,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 1,
			VChannel:     "ch",
			DataVersion:  version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	segmentID := int64(20)
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestSegmentInsertMessage(t, "ch", segmentID, 2, 40, schema)}))
	require.Eventually(t, func() bool {
		segment, ok := runtime.Growing.Segment(segmentID)
		return ok && segment.RowNum() == 2
	}, time.Second, 10*time.Millisecond)

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{CollectionID: 1, VChannel: "ch"})
}

func TestManagerDefaultGrowingRuntimeBuilderAppliesLiveCreateSegment(t *testing.T) {
	initSegcoreForViewResourceTest(t)

	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	schema := mock_segcore.GenTestCollectionSchema("snview-resource-create", schemapb.DataType_Int64, false)
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:        1,
		VChannel:            "ch",
		BaseGrowingTimeTick: 30,
		Schema:              schema,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 1,
			VChannel:     "ch",
			DataVersion:  version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	segmentID := int64(30)
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestCreateSegmentMessage(t, "ch", segmentID, 40)}))
	require.Eventually(t, func() bool {
		_, ok := runtime.Growing.Segment(segmentID)
		return ok
	}, time.Second, 10*time.Millisecond)
	require.Contains(t, runtime.Growing.SegmentIDs(), segmentID)

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{CollectionID: 1, VChannel: "ch"})
}

func TestManagerGrowingRuntimeRecordsLiveFlush(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
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
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestFlushMessage(t, "ch", 10, 40)}))
	require.Eventually(t, func() bool {
		return runtime.Growing.SegmentFlushed(10)
	}, time.Second, 10*time.Millisecond)
}

func TestManagerDefaultGrowingRuntimeBuilderDrainsDeleteReplayBeforeReady(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	scanner := newFakeTransformLogScanner()
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
		DeleteReplay: scanner,
	})

	select {
	case <-manager.NotifyReady():
		t.Fatal("manager reported ready before delete replay caught up")
	case <-time.After(50 * time.Millisecond):
	}

	entry := &streamingpb.TransformLogEntry{TimeTick: 10}
	scanner.ch <- wal.TransformLogEvent{Entry: entry}
	scanner.ch <- wal.TransformLogEvent{CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: 1}}
	close(scanner.done)

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	deleteReplayEntries := runtime.Growing.DeleteReplayEntries()
	require.Len(t, deleteReplayEntries, 1)
	require.Equal(t, uint64(10), deleteReplayEntries[0].GetTimeTick())
	require.True(t, scanner.closed)
}

func TestManagerDefaultGrowingRuntimeBuilderInitializesAppliedFrontiers(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, uint64(20), runtime.Growing.AppliedGrowingTimeTick())
	require.Equal(t, uint64(10), runtime.Growing.AppliedTransformTimeTick())
}

func TestManagerGrowingRuntimeAdvancesLiveAppliedFrontier(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestInsertMessage(t, "ch", 30)}))
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 30
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(10), runtime.Growing.AppliedTransformTimeTick())
}

func TestManagerGrowingRuntimeDoesNotAdvanceTransformFrontierForInsertOnlyTxn(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestInsertTxnMessage(t, "ch", 30)}))
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 30
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(10), runtime.Growing.AppliedTransformTimeTick())
}

func TestManagerGrowingRuntimeAdvancesTransformFrontierForDeleteTxn(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestDeleteTxnMessage(t, "ch", 30)}))
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 30 && runtime.Growing.AppliedTransformTimeTick() == 30
	}, time.Second, 10*time.Millisecond)
}

func TestManagerGetViewRuntimeWaitsForDeleteApplyFrontier(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	desc := ViewResourceDescriptor{
		CollectionID:                  1,
		VChannel:                      "ch",
		Version:                       qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		DeleteApplyStartAfterTimeTick: 20,
	}
	runtime, ready, err := manager.GetViewRuntime(desc)
	require.NoError(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)

	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestDeleteMessage(t, "ch", 20)}))
	require.Eventually(t, func() bool {
		runtime, ready, err = manager.GetViewRuntime(desc)
		return err == nil && ready && runtime != nil && runtime.Growing.AppliedTransformTimeTick() == 20
	}, time.Second, 10*time.Millisecond)
}

func TestManagerGetViewRuntimeErrorsWhenRequestedVersionIsBehindForwardLoad(t *testing.T) {
	manager := NewManager(nil, nil)
	loadedVersion := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	oldVersion := qviews.DataVersion{StreamingVersion: 99, CompactVersion: 2}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: loadedVersion,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: oldVersion, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "is behind loaded data version")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestManagerDefaultGrowingRuntimeBuilderRejectsMismatchedWALViewSnapshot(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 2,
			VChannel:     "other",
			DataVersion:  version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "wal view snapshot mismatch")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestManagerDefaultGrowingRuntimeBuilderFailsOnDeleteReplayError(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	scanner := newFakeTransformLogScanner()
	scanner.err = errors.New("delete replay failed")
	close(scanner.done)

	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
		DeleteReplay: scanner,
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "delete replay failed")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestManagerDoesNotBuildGrowingRuntimeWhenBM25PreparationFails(t *testing.T) {
	growing := &fakeGrowingSegmentRuntimeBuilder{}
	manager := NewManager(growing, errorIDFOracleRuntimeBuilder{err: errors.New("bm25 failed")})
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}

	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "bm25 failed")
	require.False(t, ready)
	require.Nil(t, runtime)
	growing.mu.Lock()
	require.Empty(t, growing.calls)
	growing.mu.Unlock()
}

func TestManagerFinishBuildClosesRuntimeReturnedWithError(t *testing.T) {
	manager := NewManager(nil, nil)
	closed := make(chan struct{})
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	key := runtimeKey{vchannel: "ch", version: version}
	task := newResourceBuildTask(context.Background(), BuildKey{
		CollectionID: 1,
		VChannel:     "ch",
		DataVersion:  version,
	}, func(context.Context) (*ViewRuntime, error) {
		return nil, nil
	})
	manager.runtimes[key] = &runtimeState{
		collectionID: 1,
		loading:      true,
		task:         task,
	}

	task.finish(&ViewRuntime{
		CollectionID: 1,
		VChannel:     "ch",
		DataVersion:  version,
		BM25: &BM25Runtime{OnClose: func() {
			close(closed)
		}},
	}, errors.New("build canceled"))
	manager.finishBuild(key, task)

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for errored runtime close")
	}
}

func TestManagerLiveObserverFeedsPreparedGrowingRuntime(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestInsertMessage(t, "ch", 30)}))
	require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestInsertMessage(t, "ch", 31)}))

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	var runtime *ViewRuntime
	require.Eventually(t, func() bool {
		var ready bool
		var err error
		runtime, ready, err = manager.GetViewRuntime(ViewResourceDescriptor{
			CollectionID: 1,
			VChannel:     "ch",
			Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		})
		return err == nil && ready && runtime != nil
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 31
	}, time.Second, 10*time.Millisecond)

	observer.Close()
	require.False(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: nil}))
}

func TestManagerOnAlterLoadConfigReturnsNilObserverForDuplicateVersion(t *testing.T) {
	blocking := &cancelAwareGrowingSegmentRuntimeBuilder{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	manager := NewManager(blocking, NoopIDFOracleRuntimeBuilder{})
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	view := walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	}

	observer := manager.OnAlterLoadConfig(view)
	require.NotNil(t, observer)
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load")
	}
	require.Nil(t, manager.OnAlterLoadConfig(view))
	newerView := view
	newerView.SegmentSnapshot.DataVersion = qviews.DataVersion{StreamingVersion: 101, CompactVersion: 2}
	require.Nil(t, manager.OnAlterLoadConfig(newerView))

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})
	select {
	case <-blocking.canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load cancellation")
	}

	readyManager := NewManager(nil, nil)
	observer = readyManager.OnAlterLoadConfig(view)
	require.NotNil(t, observer)
	select {
	case <-readyManager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}
	require.Nil(t, readyManager.OnAlterLoadConfig(view))
	observer.Close()
}

func TestManagerDropLoadConfigClosesPreparedLiveObserver(t *testing.T) {
	manager := NewManager(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
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

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})

	require.False(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: newTestInsertMessage(t, "ch", 30)}))
}

func TestLiveObserverCloseUnblocksFullBuffer(t *testing.T) {
	observer := newLiveObserver()
	for i := 0; i < defaultLiveObserverBufferSize; i++ {
		require.True(t, observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: nil}))
	}

	done := make(chan bool, 1)
	go func() {
		done <- observer.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: nil})
	}()
	select {
	case result := <-done:
		t.Fatalf("ObserveEvent returned before close: %v", result)
	case <-time.After(50 * time.Millisecond):
	}

	observer.Close()
	select {
	case result := <-done:
		require.False(t, result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ObserveEvent to unblock")
	}
}

func TestManagerDropLoadConfigCancelsInFlightAndMakesPreparedRuntimeUnavailable(t *testing.T) {
	manager := NewManager(NoopGrowingSegmentRuntimeBuilder{}, NoopIDFOracleRuntimeBuilder{})
	readyVersion := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: readyVersion,
		},
	})
	<-manager.NotifyReady()

	blocking := &cancelAwareGrowingSegmentRuntimeBuilder{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	manager.growing = blocking
	loadingVersion := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 0}
	manager.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: loadingVersion,
		},
	})
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load")
	}

	manager.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})
	select {
	case <-blocking.canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load cancellation")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: readyVersion, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)

	runtime, ready, err = manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: loadingVersion, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestManagerOnAlterLoadConfigUsesWALViewBoundary(t *testing.T) {
	growing := &fakeGrowingSegmentRuntimeBuilder{}
	bm25 := &fakeIDFOracleRuntimeBuilder{}
	manager := NewManager(growing, bm25)

	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := manager.OnAlterLoadConfig(testAlterLoadConfigView(
		1,
		"by-dev-rootcoord-dml_0_1v0",
		version,
		&viewpb.QueryViewSettings{
			RequiredPartitions: []int64{10},
			RequiredFields:     []int64{100},
		},
	))
	require.NotNil(t, observer)
	t.Cleanup(observer.Close)

	select {
	case <-manager.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for manager readiness")
	}

	runtime, ready, err := manager.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "by-dev-rootcoord-dml_0_1v0",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.NotNil(t, runtime)
	require.Len(t, runtime.BM25.Resources, 1)

	growing.mu.Lock()
	require.Len(t, growing.calls, 1)
	growing.mu.Unlock()

	bm25.mu.Lock()
	require.Len(t, bm25.calls, 1)
	bm25.mu.Unlock()
}
