package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestModuleDirtySnapshotAdvancesDataBarrierAfterMarkPersisted(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	msg := newModuleTestDeleteMessage(t, 10)
	result := module.ObserveMessage(ctx, msg)
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	log := module.getLog("v1")
	require.NotNil(t, log)
	flushResult, err := log.log.Flush(ctx, FlushOption{TargetTimeTick: 10})
	require.NoError(t, err)
	assert.True(t, flushResult.Started)

	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	assert.Equal(t, moduleapi.ModuleNameTransformLog, snapshots[0].ModuleName())
	assert.Equal(t, moduleapi.SnapshotOpUpsert, snapshots[0].Op())
	assert.Equal(t, uint64(10), snapshots[0].DataTimeTick())
	assert.Equal(t, uint64(0), result.Data.TimeTick())

	snapshots[0].MarkPersisted()
	assert.Equal(t, uint64(10), result.Data.TimeTick())
}

func TestLatestTransformTimeTickIncludesUnflushedBuffer(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	assert.Equal(t, uint64(0), module.LatestTransformTimeTick("v1"))

	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 10))

	assert.Equal(t, uint64(10), module.LatestTransformTimeTick("v1"))
}

func TestStreamNotifyStateTracksChangedVChannel(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	_, seq, changed := module.streamNotifyStateSince(0)
	require.Zero(t, seq)
	require.Empty(t, changed)

	result := module.ObserveMessage(ctx, newModuleTestDeleteMessageWithVChannel(t, 10, "v1"))
	require.NotNil(t, result.Data)
	_, seq, changed = module.streamNotifyStateSince(seq)
	require.NotZero(t, seq)
	require.Equal(t, []string{"v1"}, changed)

	result = module.ObserveMessage(ctx, newModuleTestDeleteMessageWithVChannel(t, 20, "v2"))
	require.NotNil(t, result.Data)
	_, seq, changed = module.streamNotifyStateSince(seq)
	require.NotZero(t, seq)
	require.Equal(t, []string{"v2"}, changed)
}

func TestDeletePublishesToLiveSubscriberBeforeFlush(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", map[string]*streamingpb.VChannelTransformLogMeta{
		"v1": {},
	}, newMemoryStore())
	module.SwitchIntoMetaAndData()

	stream, err := module.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	caughtUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, caughtUp.CaughtUp)

	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 10))

	select {
	case event := <-handler.events:
		require.NotNil(t, event.Entry)
		assert.Equal(t, uint64(10), event.Entry.GetTimeTick())
		require.IsType(t, &streamingpb.TransformLogEntry_Delete{}, event.Entry.GetEntry())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live transform log delete entry")
	}
}

func TestReadReplaysUnflushedBuffer(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", map[string]*streamingpb.VChannelTransformLogMeta{
		"v1": {},
	}, newMemoryStore())
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 10))

	stream, err := module.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	select {
	case event := <-handler.events:
		require.NotNil(t, event.Entry)
		assert.Equal(t, uint64(10), event.Entry.GetTimeTick())
		require.IsType(t, &streamingpb.TransformLogEntry_Delete{}, event.Entry.GetEntry())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unflushed transform log delete entry")
	}
	caughtUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, caughtUp.CaughtUp)
}

func TestCreateCollectionCreatesTransformLogForVChannelWithoutDeleteHistory(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	stream, err := module.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            newRecordingStreamHandler(),
	})
	assert.True(t, errors.Is(err, wal.ErrTransformLogVChannelUnavailable))

	module.ObserveMessage(ctx, newModuleTestCreateCollectionMessage(t, 5))

	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	select {
	case event := <-handler.events:
		require.NotNil(t, event.Entry)
		assert.Equal(t, uint64(5), event.Entry.GetTimeTick())
		require.IsType(t, &streamingpb.TransformLogEntry_Barrier{}, event.Entry.GetEntry())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live transform log barrier entry")
	}
	caughtUp := recvStreamEvent(t, handler.events)
	require.NotNil(t, caughtUp.CaughtUp)

	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 10))

	select {
	case event := <-handler.events:
		require.NotNil(t, event.Entry)
		assert.Equal(t, uint64(10), event.Entry.GetTimeTick())
		require.IsType(t, &streamingpb.TransformLogEntry_Delete{}, event.Entry.GetEntry())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live transform log delete entry")
	}
}

func TestCreateCollectionProducesTransformBarrierEntry(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newModuleTestCreateCollectionMessage(t, 5))
	require.NotNil(t, result.Data)

	log := module.getLog("v1")
	require.NotNil(t, log)
	flushResult, err := log.log.Flush(ctx, FlushOption{TargetTimeTick: 5})
	require.NoError(t, err)
	assert.True(t, flushResult.Started)
	assert.Equal(t, uint64(5), flushResult.DurableTimeTick)

	transformLog := log.log.(*transformLog)
	require.Len(t, transformLog.chunks, 1)
	require.Len(t, transformLog.chunks[0].entries, 1)
	entry := transformLog.chunks[0].entries[0]
	assert.Equal(t, uint64(5), entry.GetTimeTick())
	require.IsType(t, &streamingpb.TransformLogEntry_Barrier{}, entry.GetEntry())
}

func TestFlushStyleMessageProducesTransformBarrierEntry(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", nil, newMemoryStore())
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newModuleTestCreateCollectionMessage(t, 5))

	result := module.ObserveMessage(ctx, newModuleTestManualFlushMessage(t, 20))
	require.NotNil(t, result.Data)

	log := module.getLog("v1")
	require.NotNil(t, log)
	flushResult, err := log.log.Flush(ctx, FlushOption{TargetTimeTick: 20})
	require.NoError(t, err)
	assert.True(t, flushResult.Started)
	assert.Equal(t, uint64(20), flushResult.DurableTimeTick)

	transformLog := log.log.(*transformLog)
	require.Len(t, transformLog.chunks, 1)
	entries := transformLog.chunks[0].entries
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(5), entries[0].GetTimeTick())
	require.IsType(t, &streamingpb.TransformLogEntry_Barrier{}, entries[0].GetEntry())
	assert.Equal(t, uint64(20), entries[1].GetTimeTick())
	require.IsType(t, &streamingpb.TransformLogEntry_Barrier{}, entries[1].GetEntry())
}

func TestRecoveryBarrierInMetaOnlyModeDoesNotHideRecoveredDelete(t *testing.T) {
	ctx := context.Background()
	module := NewModule("p1", map[string]*streamingpb.VChannelTransformLogMeta{
		"v1": {},
	}, newMemoryStore())

	module.ObserveMessage(ctx, newModuleTestRecoveryBarrierMessage(t, 30))
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newModuleTestDeleteMessage(t, 20))
	module.ObserveMessage(ctx, newModuleTestRecoveryBarrierMessage(t, 30))

	stream, err := module.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           "v1",
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)

	deleteEvent := recvStreamEvent(t, handler.events)
	require.NotNil(t, deleteEvent.Entry)
	assert.Equal(t, uint64(20), deleteEvent.Entry.GetTimeTick())
	require.IsType(t, &streamingpb.TransformLogEntry_Delete{}, deleteEvent.Entry.GetEntry())

	barrierEvent := recvStreamEvent(t, handler.events)
	require.NotNil(t, barrierEvent.Entry)
	assert.Equal(t, uint64(30), barrierEvent.Entry.GetTimeTick())
	require.IsType(t, &streamingpb.TransformLogEntry_Barrier{}, barrierEvent.Entry.GetEntry())
}

type memoryStore struct {
	chunks map[string]map[uint64]*streamingpb.TransformLogChunk
	reads  map[string]map[uint64]int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		chunks: make(map[string]map[uint64]*streamingpb.TransformLogChunk),
		reads:  make(map[string]map[uint64]int),
	}
}

func (s *memoryStore) WriteTransformLogChunk(_ context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	if s.chunks[vchannel] == nil {
		s.chunks[vchannel] = make(map[uint64]*streamingpb.TransformLogChunk)
	}
	s.chunks[vchannel][chunk.GetChunkId()] = proto.Clone(chunk).(*streamingpb.TransformLogChunk)
	return nil
}

func (s *memoryStore) ReadTransformLogChunk(_ context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	if s.reads[vchannel] == nil {
		s.reads[vchannel] = make(map[uint64]int)
	}
	s.reads[vchannel][chunkID]++
	return proto.Clone(s.chunks[vchannel][chunkID]).(*streamingpb.TransformLogChunk), nil
}

func (s *memoryStore) resetReadCount() {
	s.reads = make(map[string]map[uint64]int)
}

func (s *memoryStore) readCount(vchannel string, chunkID uint64) int {
	return s.reads[vchannel][chunkID]
}

func newModuleTestCreateCollectionMessage(t *testing.T, timetick uint64) message.ImmutableCreateCollectionMessageV1 {
	t.Helper()
	mutableMsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{10},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			Base:             &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionSchema: &schemapb.CollectionSchema{Name: "test_collection"},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableCreateCollectionMessageV1(msg)
}

func newModuleTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableDeleteMessageV1 {
	return newModuleTestDeleteMessageWithVChannel(t, timetick, "v1")
}

func newModuleTestDeleteMessageWithVChannel(t *testing.T, timetick uint64, vchannel string) message.ImmutableDeleteMessageV1 {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
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
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func newModuleTestManualFlushMessage(t *testing.T, timetick uint64) message.ImmutableManualFlushMessageV2 {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableManualFlushMessageV2(msg)
}

func newModuleTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableRecoveryBarrierMessageV2 {
	t.Helper()
	mutableMsg := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableRecoveryBarrierMessageV2(msg)
}
