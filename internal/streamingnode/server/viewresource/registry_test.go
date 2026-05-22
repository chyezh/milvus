package viewresource

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
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

var _ walview.LoadConfigListener = (*DefaultRegistry)(nil)

func TestRegistryInterfaceUsesWALViewBoundary(t *testing.T) {
	_, ok := reflect.TypeOf((*Registry)(nil)).Elem().MethodByName("PrepareLatestFromAlterLoadConfig")
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

type fakeGrowingPreparer struct {
	mu    sync.Mutex
	calls []LoadResourceDescriptor
	block chan struct{}
	seen  chan struct{}
}

func (p *fakeGrowingPreparer) PrepareLatest(_ context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, desc)
	return &GrowingRuntime{SegmentIDs: []int64{10, 11}}, nil
}

type cancelAwareGrowingPreparer struct {
	started  chan struct{}
	canceled chan struct{}
}

func (p *cancelAwareGrowingPreparer) PrepareLatest(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
	close(p.started)
	<-ctx.Done()
	close(p.canceled)
	return nil, ctx.Err()
}

type fakeBM25Provider struct {
	mu    sync.Mutex
	calls []LoadResourceDescriptor
}

func (p *fakeBM25Provider) PrepareLatestFromAlterLoadConfig(_ context.Context, desc LoadResourceDescriptor) (*BM25Runtime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, desc)
	return &BM25Runtime{Resources: []*BM25SegmentResource{{SegmentID: 20}}}, nil
}

type errorBM25Provider struct {
	err error
}

func (p errorBM25Provider) PrepareLatestFromAlterLoadConfig(context.Context, LoadResourceDescriptor) (*BM25Runtime, error) {
	return nil, p.err
}

type recordingGrowingApplier struct {
	mu      sync.Mutex
	events  []string
	closed  bool
	closeCh chan struct{}
}

func newRecordingGrowingApplier() *recordingGrowingApplier {
	return &recordingGrowingApplier{
		closeCh: make(chan struct{}),
	}
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

func (a *recordingGrowingApplier) LoadPersistedSegment(_ context.Context, segment walview.VisibleSegment) error {
	a.record("persisted:%d", segment.SegmentID)
	return nil
}

func (a *recordingGrowingApplier) ApplySnapshotInsert(_ context.Context, segment walview.VisibleSegment, msg message.ImmutableMessage) error {
	a.record("snapshot:%d:%d", segment.SegmentID, msg.TimeTick())
	return nil
}

func (a *recordingGrowingApplier) ApplyDeleteReplay(_ context.Context, entry *streamingpb.TransformLogEntry) error {
	a.record("delete:%d", entry.GetTimeTick())
	return nil
}

func (a *recordingGrowingApplier) ApplyLiveMessage(_ context.Context, msg message.ImmutableMessage) error {
	a.record("live:%s:%d", msg.MessageType().String(), msg.TimeTick())
	return nil
}

func (a *recordingGrowingApplier) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	close(a.closeCh)
}

func (a *recordingGrowingApplier) record(format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, fmt.Sprintf(format, args...))
}

func (a *recordingGrowingApplier) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.events...)
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

type fakeTransformLogScanner struct {
	ch     chan transformlogapi.Event
	done   chan struct{}
	err    error
	closed bool
}

func newFakeTransformLogScanner() *fakeTransformLogScanner {
	return &fakeTransformLogScanner{
		ch:   make(chan transformlogapi.Event, 16),
		done: make(chan struct{}),
	}
}

func (s *fakeTransformLogScanner) Name() string {
	return "fake"
}

func (s *fakeTransformLogScanner) Chan() <-chan transformlogapi.Event {
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

func TestRegistryOnAlterLoadConfigPreparesWALViewDataVersion(t *testing.T) {
	growing := &fakeGrowingPreparer{}
	bm25 := &fakeBM25Provider{}
	registry := NewRegistry(growing, bm25)

	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
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
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
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

func TestRegistryDefaultGrowingPreparerUsesWALViewSegmentSnapshot(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
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
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, []int64{10, 11}, runtime.Growing.SegmentIDs)
}

func TestRegistryDefaultGrowingPreparerAppliesSnapshotDeleteAndLiveInOrder(t *testing.T) {
	applier := newRecordingGrowingApplier()
	registry := NewRegistry(SnapshotGrowingSegmentPreparer{
		NewApplier: func(context.Context, LoadResourceDescriptor) (GrowingRuntimeApplier, error) {
			return applier, nil
		},
	}, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	scanner := newFakeTransformLogScanner()
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   40,
		BaseTransformTimeTick: 35,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
			Segments: []walview.VisibleSegment{
				{
					SegmentID: 10,
					Data: walview.SegmentSnapshotData{
						PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
						InsertMessages: []message.ImmutableMessage{
							newTestInsertMessage(t, "ch", 30),
						},
					},
				},
			},
		},
		DeleteReplay: scanner,
	})
	require.NotNil(t, observer)

	scanner.ch <- transformlogapi.Event{Entry: &streamingpb.TransformLogEntry{TimeTick: 35}}
	scanner.ch <- transformlogapi.Event{CaughtUp: &transformlogapi.CaughtUp{StartAfterTimeTick: 1}}

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	require.True(t, observer.ObserveMessage(context.Background(), newTestDeleteMessage(t, "ch", 45)))
	require.Eventuallyf(t, func() bool {
		return len(applier.snapshot()) == 4
	}, time.Second, 10*time.Millisecond, "events: %v", applier.snapshot())
	require.Equal(t, []string{
		"persisted:10",
		"snapshot:10:30",
		"delete:35",
		"live:Delete:45",
	}, applier.snapshot())
}

func TestRegistryDefaultGrowingPreparerBuildsSegcoreGrowingSegmentFromSnapshotInsert(t *testing.T) {
	initSegcoreForViewResourceTest(t)

	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	schema := mock_segcore.GenTestCollectionSchema("snview-resource", schemapb.DataType_Int64, false)
	segmentID := int64(10)
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
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
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Contains(t, runtime.Growing.Segments, segmentID)
	require.Equal(t, int64(3), runtime.Growing.Segments[segmentID].RowNum())

	registry.ReleaseLoad(1, "ch")
}

func TestRegistryDefaultGrowingPreparerAppliesLiveInsertToNewSegcoreSegment(t *testing.T) {
	initSegcoreForViewResourceTest(t)

	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	schema := mock_segcore.GenTestCollectionSchema("snview-resource-live", schemapb.DataType_Int64, false)
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
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
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}
	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	segmentID := int64(20)
	require.True(t, observer.ObserveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", segmentID, 2, 40, schema)))
	require.Eventually(t, func() bool {
		segment, ok := runtime.Growing.Segment(segmentID)
		return ok && segment.RowNum() == 2
	}, time.Second, 10*time.Millisecond)

	registry.ReleaseLoad(1, "ch")
}

func TestRegistryGrowingRuntimeRecordsLiveFlush(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}
	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveMessage(context.Background(), newTestFlushMessage(t, "ch", 10, 40)))
	require.Eventually(t, func() bool {
		return runtime.Growing.SegmentFlushed(10)
	}, time.Second, 10*time.Millisecond)
}

func TestDeleteTimestampsFromRequestUsesPerRowTimestamps(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		Timestamps: []uint64{11, 12, 13},
	}

	require.Equal(t, []uint64{11, 12, 13}, deleteTimestampsFromRequest(100, request))
}

func TestDeleteTimestampsFromRequestFallsBackToMessageTimeTick(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
	}

	require.Equal(t, []uint64{100, 100}, deleteTimestampsFromRequest(100, request))
}

func TestDeleteTimestampsFromTransformLogBlockUsesEntryTimeTick(t *testing.T) {
	block := &streamingpb.TransformDeleteBlock{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
	}

	require.Equal(t, []uint64{200, 200, 200}, deleteTimestampsFromTransformLogBlock(200, block))
}

func TestRegistryDefaultGrowingPreparerDrainsDeleteReplayBeforeReady(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	scanner := newFakeTransformLogScanner()
	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
		DeleteReplay: scanner,
	})

	select {
	case <-registry.NotifyReady():
		t.Fatal("registry reported ready before delete replay caught up")
	case <-time.After(50 * time.Millisecond):
	}

	entry := &streamingpb.TransformLogEntry{TimeTick: 10}
	scanner.ch <- transformlogapi.Event{Entry: entry}
	scanner.ch <- transformlogapi.Event{CaughtUp: &transformlogapi.CaughtUp{StartAfterTimeTick: 1}}
	close(scanner.done)

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, runtime.Growing.DeleteReplayEntries, 1)
	require.Equal(t, uint64(10), runtime.Growing.DeleteReplayEntries[0].GetTimeTick())
	require.True(t, scanner.closed)
}

func TestRegistryDefaultGrowingPreparerInitializesAppliedFrontiers(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, uint64(20), runtime.Growing.AppliedGrowingTimeTick())
	require.Equal(t, uint64(10), runtime.Growing.AppliedTransformTimeTick())
}

func TestRegistryGrowingRuntimeAdvancesLiveAppliedFrontier(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}
	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)

	require.True(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 30)))
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 30
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(10), runtime.Growing.AppliedTransformTimeTick())
}

func TestRegistryGetViewRuntimeWaitsForDeleteApplyFrontier(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID:          1,
		VChannel:              "ch",
		BaseGrowingTimeTick:   20,
		BaseTransformTimeTick: 10,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	desc := ViewResourceDescriptor{
		CollectionID:                  1,
		VChannel:                      "ch",
		Version:                       qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
		DeleteApplyStartAfterTimeTick: 20,
	}
	runtime, ready, err := registry.GetViewRuntime(desc)
	require.NoError(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)

	require.True(t, observer.ObserveMessage(context.Background(), newTestDeleteMessage(t, "ch", 20)))
	require.Eventually(t, func() bool {
		runtime, ready, err = registry.GetViewRuntime(desc)
		return err == nil && ready && runtime != nil && runtime.Growing.AppliedTransformTimeTick() == 20
	}, time.Second, 10*time.Millisecond)
}

func TestRegistryGetViewRuntimeErrorsWhenRequestedVersionIsBehindForwardLoad(t *testing.T) {
	registry := NewRegistry(nil, nil)
	loadedVersion := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	oldVersion := qviews.DataVersion{StreamingVersion: 99, CompactVersion: 2}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: loadedVersion,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: oldVersion, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "is behind loaded data version")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestRegistryDefaultGrowingPreparerRejectsMismatchedWALViewSnapshot(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 2,
			VChannel:     "other",
			DataVersion:  version,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "wal view snapshot mismatch")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestRegistryDefaultGrowingPreparerFailsOnDeleteReplayError(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	scanner := newFakeTransformLogScanner()
	scanner.err = errors.New("delete replay failed")
	close(scanner.done)

	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
		DeleteReplay: scanner,
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "delete replay failed")
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestRegistryClosesPreparedGrowingRuntimeWhenBM25PreparationFails(t *testing.T) {
	applier := newRecordingGrowingApplier()
	registry := NewRegistry(SnapshotGrowingSegmentPreparer{
		NewApplier: func(context.Context, LoadResourceDescriptor) (GrowingRuntimeApplier, error) {
			return applier, nil
		},
	}, errorBM25Provider{err: errors.New("bm25 failed")})
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}

	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "bm25 failed")
	require.False(t, ready)
	require.Nil(t, runtime)
	select {
	case <-applier.closeCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for growing runtime close")
	}
}

func TestRegistryLiveObserverFeedsPreparedGrowingRuntime(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.True(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 30)))
	require.True(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 31)))

	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.Eventually(t, func() bool {
		return runtime.Growing.AppliedGrowingTimeTick() == 31
	}, time.Second, 10*time.Millisecond)

	observer.Close()
	require.False(t, observer.ObserveMessage(context.Background(), nil))
}

func TestRegistryOnAlterLoadConfigReturnsNilObserverForDuplicateVersion(t *testing.T) {
	blocking := &cancelAwareGrowingPreparer{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	registry := NewRegistry(blocking, NoopBM25Provider{})
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	view := walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	}

	observer := registry.OnAlterLoadConfig(view)
	require.NotNil(t, observer)
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load")
	}
	require.Nil(t, registry.OnAlterLoadConfig(view))

	registry.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})
	select {
	case <-blocking.canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load cancellation")
	}

	readyRegistry := NewRegistry(nil, nil)
	observer = readyRegistry.OnAlterLoadConfig(view)
	require.NotNil(t, observer)
	select {
	case <-readyRegistry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}
	require.Nil(t, readyRegistry.OnAlterLoadConfig(view))
	observer.Close()
}

func TestRegistryEvictBeforeClosesPreparedLiveObserver(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	registry.EvictBefore(1, "ch", qviews.DataVersion{StreamingVersion: 101})

	require.False(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 30)))
}

func TestRegistryReleaseLoadClosesPreparedLiveObserver(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	registry.ReleaseLoad(1, "ch")

	require.False(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 30)))
}

func TestRegistryDropLoadConfigClosesPreparedLiveObserver(t *testing.T) {
	registry := NewRegistry(nil, nil)
	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: version,
		},
	})
	require.NotNil(t, observer)
	select {
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	registry.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})

	require.False(t, observer.ObserveMessage(context.Background(), newTestInsertMessage(t, "ch", 30)))
}

func TestLiveObserverCloseUnblocksFullBuffer(t *testing.T) {
	observer := newLiveObserver()
	for i := 0; i < defaultLiveObserverBufferSize; i++ {
		require.True(t, observer.ObserveMessage(context.Background(), nil))
	}

	done := make(chan bool, 1)
	go func() {
		done <- observer.ObserveMessage(context.Background(), nil)
	}()
	select {
	case result := <-done:
		t.Fatalf("ObserveMessage returned before close: %v", result)
	case <-time.After(50 * time.Millisecond):
	}

	observer.Close()
	select {
	case result := <-done:
		require.False(t, result)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ObserveMessage to unblock")
	}
}

func TestRegistryDropLoadConfigCancelsInFlightAndMakesPreparedRuntimeUnavailable(t *testing.T) {
	registry := NewRegistry(NoopGrowingSegmentPreparer{}, NoopBM25Provider{})
	readyVersion := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			DataVersion: readyVersion,
		},
	})
	<-registry.NotifyReady()

	blocking := &cancelAwareGrowingPreparer{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	registry.growing = blocking
	loadingVersion := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 0}
	registry.OnAlterLoadConfig(walview.VChannelWALView{
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

	registry.OnDropLoadConfig(walview.DropLoadConfigEvent{
		CollectionID: 1,
		VChannel:     "ch",
	})
	select {
	case <-blocking.canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight load cancellation")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: readyVersion, QueryVersion: 1},
	})
	require.ErrorContains(t, err, "load config was dropped")
	require.False(t, ready)
	require.Nil(t, runtime)

	runtime, ready, err = registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: loadingVersion, QueryVersion: 1},
	})
	require.Error(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestRegistryOnAlterLoadConfigUsesWALViewBoundary(t *testing.T) {
	growing := &fakeGrowingPreparer{}
	bm25 := &fakeBM25Provider{}
	registry := NewRegistry(growing, bm25)

	version := qviews.DataVersion{StreamingVersion: 100, CompactVersion: 2}
	observer := registry.OnAlterLoadConfig(testAlterLoadConfigView(
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
	case <-registry.NotifyReady():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for registry readiness")
	}

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "by-dev-rootcoord-dml_0_1v0",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.NotNil(t, runtime)
	require.Equal(t, []int64{10, 11}, runtime.Growing.SegmentIDs)
	require.Len(t, runtime.BM25.Resources, 1)

	growing.mu.Lock()
	require.Len(t, growing.calls, 1)
	growing.mu.Unlock()

	bm25.mu.Lock()
	require.Len(t, bm25.calls, 1)
	bm25.mu.Unlock()

	registry.EvictBefore(1, "by-dev-rootcoord-dml_0_1v0", qviews.DataVersion{StreamingVersion: 101, CompactVersion: 0})
	runtime, ready, err = registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "by-dev-rootcoord-dml_0_1v0",
		Version:      qviews.QueryViewVersion{DataVersion: version, QueryVersion: 1},
	})
	require.Error(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)
}

func TestRegistryEvictBeforeKeepsMinAndNewer(t *testing.T) {
	registry := NewRegistry(NoopGrowingSegmentPreparer{}, NoopBM25Provider{})
	v1 := qviews.DataVersion{StreamingVersion: 1, CompactVersion: 0}
	v2 := qviews.DataVersion{StreamingVersion: 2, CompactVersion: 0}

	observer := registry.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", v1, nil))
	require.NotNil(t, observer)
	t.Cleanup(observer.Close)
	<-registry.NotifyReady()
	observer = registry.OnAlterLoadConfig(testAlterLoadConfigView(1, "ch", v2, nil))
	require.NotNil(t, observer)
	t.Cleanup(observer.Close)
	<-registry.NotifyReady()

	registry.EvictBefore(1, "ch", v2)

	runtime, ready, err := registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: v1, QueryVersion: 1},
	})
	require.Error(t, err)
	require.False(t, ready)
	require.Nil(t, runtime)

	runtime, ready, err = registry.GetViewRuntime(ViewResourceDescriptor{
		CollectionID: 1,
		VChannel:     "ch",
		Version:      qviews.QueryViewVersion{DataVersion: v2, QueryVersion: 1},
	})
	require.NoError(t, err)
	require.True(t, ready)
	require.NotNil(t, runtime)
}
