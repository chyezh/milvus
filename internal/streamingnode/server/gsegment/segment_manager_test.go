package gsegment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestNewSegmentManager(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	assert.NotNil(t, sm)
	assert.NotNil(t, sm.l0Segments)
	assert.NotNil(t, sm.l1Segments)
	assert.NotNil(t, sm.scheduler)
	assert.Equal(t, mockCM, sm.chunkManager)
	sm.Close()
}

func TestSegmentManager_CreateL1Segment(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}

	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId:    1001,
		CollectionId: 1,
		PartitionId:  100,
		Vchannel:     "test-channel-v0",
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat: &streamingpb.SegmentAssignmentStat{
			BeginTimeTick: 1000,
			EndTimeTick:   1000,
		},
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
			L1: &streamingpb.L1SegmentPersistedStorage{},
		},
	}

	// Test creating L1 segment
	sm.CreateL1Segment(meta, schema)

	// Verify segment was created
	sm.mu.RLock()
	segment, exists := sm.l1Segments[1001]
	sm.mu.RUnlock()

	assert.True(t, exists)
	assert.NotNil(t, segment)
	assert.Equal(t, int64(1001), segment.meta.SegmentId)
	assert.Equal(t, schema, segment.schema)

	// Test creating duplicate segment (should log warning but not panic)
	sm.CreateL1Segment(meta, schema)
}

func TestSegmentManager_CreateL0Segment(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId:    2001,
		CollectionId: 1,
		PartitionId:  100,
		Vchannel:     "test-channel-v0",
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat: &streamingpb.SegmentAssignmentStat{
			BeginTimeTick: 1000,
			EndTimeTick:   1000,
		},
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		},
	}

	// Test creating L0 segment
	sm.CreateL0Segment(meta)

	// Verify segment was created
	sm.mu.RLock()
	segment, exists := sm.l0Segments[2001]
	sm.mu.RUnlock()

	assert.True(t, exists)
	assert.NotNil(t, segment)
	assert.Equal(t, int64(2001), segment.meta.SegmentId)

	// Test creating duplicate segment (should log warning but not panic)
	sm.CreateL0Segment(meta)
}

func TestSegmentManager_ObserveInsert(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create L1 segment first
	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	sm.CreateL1Segment(meta, schema)

	// Create mock insert message
	mockMsg := createMockInsertMessage(1001, 100, 10)

	// Observe insert
	sm.ObserveInsert(mockMsg)

	// Verify segment received the insert
	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
}

func TestSegmentManager_ObserveDelete(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create mock delete message
	mockMsg := createMockDeleteMessage(2001, 100, 5)

	// Observe delete
	sm.ObserveDelete(mockMsg, createTestSchema())

	// Verify segment received the delete
	sm.mu.RLock()
	var segment *L0Segment
	for _, l0 := range sm.l0Segments {
		segment = l0
	}
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
	assert.Equal(t, int64(1), segment.GetMeta().GetCollectionId())
	assert.Equal(t, int64(100), segment.GetMeta().GetPartitionId())
	assert.Equal(t, "test-channel-v0", segment.GetMeta().GetVchannel())
	assert.Equal(t, datapb.SegmentLevel_L0, segment.GetMeta().GetStat().GetLevel())
	assert.Equal(t, uint64(1499), segment.GetMeta().GetStat().GetCreateSegmentTimeTick())
	assert.Equal(t, uint64(1499), segment.SyncSafeTimeTick())
	assert.Len(t, segment.GetSealedChunks(), 0)
}

func TestDeleteChunkTask_FieldIDMatchesPrimaryKey(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockCM.EXPECT().Write(mock.Anything, "files/delta_log/1/100/2001/10", mock.Anything).Return(nil).Once()

	chunk := &DeleteChunk{}
	chunk.Push(createMockDeleteMessage(2001, 100, 5))
	task := NewDeleteChunkTask(
		chunk,
		1,
		100,
		2001,
		createTestSchema(),
		mockCM,
		allocator.NewLocalAllocator(10, 11),
		&indexpb.StorageConfig{RootPath: "files"},
		nil,
	)

	assert.Equal(t, ErrContinue, task.Poll(context.Background()))
	assert.Equal(t, ErrContinue, task.Poll(context.Background()))
	assert.NoError(t, task.Poll(context.Background()))
	assert.Equal(t, int64(100), task.binlog.GetFieldID())
	assert.Len(t, task.binlog.GetBinlogs(), 1)
}

func TestSegmentManager_FlushL1Segment(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create and flush L1 segment
	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	sm.CreateL1Segment(meta, schema)

	// Flush segment
	sm.FlushL1Segment(1001, 2000)

	// Verify segment was flushed
	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
	assert.True(t, segment.IsSealed())

	// Test flushing non-existent segment (should log warning)
	sm.FlushL1Segment(9999, 2000)
}

func TestSegmentManager_FlushL0Segment(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create and flush L0 segment
	meta := createL0SegmentMeta(2001, 1, 100)
	sm.CreateL0Segment(meta)

	// Flush segment
	sm.FlushL0Segment(2001, 2000)

	// Verify segment was flushed
	sm.mu.RLock()
	segment := sm.l0Segments[2001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
	assert.True(t, segment.IsSealed())

	// Test flushing non-existent segment (should log warning)
	sm.FlushL0Segment(9999, 2000)
}

func TestSegmentManager_GetDirtySnapshots(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create segments
	schema := createTestSchema()
	l1Meta := createL1SegmentMeta(1001, 1, 100)
	l0Meta := createL0SegmentMeta(2001, 1, 100)

	sm.CreateL1Segment(l1Meta, schema)
	sm.CreateL0Segment(l0Meta)

	// Mark segments as dirty by flushing
	sm.FlushL1Segment(1001, 2000)
	sm.FlushL0Segment(2001, 2000)

	// Get dirty snapshots
	snapshots := sm.GetDirtySnapshots()

	assert.Len(t, snapshots, 2)
	assert.NotNil(t, snapshots[1001])
	assert.NotNil(t, snapshots[2001])

	// Get snapshots again (should be empty as dirty flag is consumed)
	snapshots2 := sm.GetDirtySnapshots()
	assert.Len(t, snapshots2, 0)
}

func TestSegmentManager_RecoverFromSnapshot(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create snapshot data
	segmentAssignments := map[int64]*streamingpb.SegmentAssignmentMeta{
		1001: createL1SegmentMeta(1001, 1, 100),
		2001: createL0SegmentMeta(2001, 1, 100),
	}

	// Recover from snapshot
	sm.RecoverFromSnapshot(segmentAssignments)

	// Verify segments were recovered
	sm.mu.RLock()
	l1Segment, l1Exists := sm.l1Segments[1001]
	l0Segment, l0Exists := sm.l0Segments[2001]
	sm.mu.RUnlock()

	assert.True(t, l1Exists)
	assert.True(t, l0Exists)
	assert.NotNil(t, l1Segment)
	assert.NotNil(t, l0Segment)
	assert.False(t, l1Segment.dirty) // Should not be dirty after recovery
	assert.False(t, l0Segment.dirty)

	// Test recovery with nil Stat
	segmentWithNilStat := &streamingpb.SegmentAssignmentMeta{
		SegmentId: 3001,
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		},
		Stat: nil,
	}

	sm.RecoverFromSnapshot(map[int64]*streamingpb.SegmentAssignmentMeta{
		3001: segmentWithNilStat,
	})

	sm.mu.RLock()
	segment3001 := sm.l0Segments[3001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment3001)
	assert.NotNil(t, segment3001.meta.Stat) // Should be initialized
}

type testSchemaProvider struct {
	schema   *schemapb.CollectionSchema
	vchannel string
	timetick uint64
}

func (p *testSchemaProvider) GetSchema(_ context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error) {
	p.vchannel = vchannel
	p.timetick = timetick
	return p.schema, nil
}

func TestSegmentManager_RecoverFromSnapshotWithSchema(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	schema := createTestSchema()
	provider := &testSchemaProvider{schema: schema}
	meta := createL1SegmentMeta(1001, 1, 100)
	meta.CheckpointTimeTick = 1500

	sm.RecoverFromSnapshotWithSchema(context.Background(), map[int64]*streamingpb.SegmentAssignmentMeta{
		1001: meta,
	}, provider)

	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
	assert.Equal(t, schema, segment.GetSchema())
	assert.Equal(t, "test-channel-v0", provider.vchannel)
	assert.Equal(t, uint64(1500), provider.timetick)
}

func TestSegmentManager_RemoveSegment(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create segments
	schema := createTestSchema()
	l1Meta := createL1SegmentMeta(1001, 1, 100)
	l0Meta := createL0SegmentMeta(2001, 1, 100)

	sm.CreateL1Segment(l1Meta, schema)
	sm.CreateL0Segment(l0Meta)

	// Remove L1 segment
	sm.RemoveSegment(1001)

	sm.mu.RLock()
	_, l1Exists := sm.l1Segments[1001]
	sm.mu.RUnlock()

	assert.False(t, l1Exists)

	// Remove L0 segment
	sm.RemoveSegment(2001)

	sm.mu.RLock()
	_, l0Exists := sm.l0Segments[2001]
	sm.mu.RUnlock()

	assert.False(t, l0Exists)
}

func TestSegmentManager_TryScheduleL1_NoOpWhenNoChunks(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	sm.CreateL1Segment(meta, schema)

	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()

	// No sealed chunks yet — BeginSaveChunk should refuse and tryScheduleL1
	// must not submit anything to the scheduler. The scheduler has no
	// expectations set on mockCM, so any accidental submission would fail
	// via mockCM's Write being called unexpectedly.
	sm.tryScheduleL1(segment)
}

func TestSegmentManager_SyncSafeTimeTick(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	meta.CheckpointTimeTick = 1000
	meta.Stat.CreateSegmentTimeTick = 1000
	sm.CreateL1Segment(meta, schema)

	assert.Equal(t, uint64(1000), sm.SyncSafeTimeTick(1500))

	sm.ObserveInsert(createMockInsertMessage(1001, 100, 10))
	assert.Equal(t, uint64(1000), sm.SyncSafeTimeTick(1500))

	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()
	segment.sealGrowingChunk()
	chunk, ok := segment.BeginSaveChunk()
	assert.True(t, ok)
	segment.SaveChunkDone(&SaveChunkDoneRequest{
		Binlog: &streamingpb.L1SegmentBinLogs{
			FromTimeTick: chunk.startFromTimeTick,
			ToTimeTick:   chunk.endToTimeTick,
		},
	})
	assert.Equal(t, uint64(1500), sm.SyncSafeTimeTick(2000))
}

func TestL1Segment_ChunkSyncContextReusesColumnGroups(t *testing.T) {
	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	segment := newL1Segment(meta, schema)
	segment.SaveChunkDone(&SaveChunkDoneRequest{
		Binlog: &streamingpb.L1SegmentBinLogs{
			FieldBinlog: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101},
			}},
		},
	})

	ctx := segment.ChunkSyncContext(newInsertChunk(1024))

	assert.False(t, ctx.Flush)
	assert.Len(t, ctx.ColumnGroups, 1)
	assert.Equal(t, int64(0), ctx.ColumnGroups[0].GroupID)
	assert.Equal(t, []int64{100, 101}, ctx.ColumnGroups[0].Fields)
	assert.Equal(t, []int{0, 1}, ctx.ColumnGroups[0].Columns)
	assert.Len(t, ctx.PreviousBinlog, 1)
}

func TestSegmentManager_SealStaleChunks(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	meta := createL1SegmentMeta(1001, 1, 100)
	meta.CheckpointTimeTick = 1000
	meta.Stat.CreateSegmentTimeTick = 1000
	sm.CreateL1Segment(meta, nil)
	sm.ObserveInsert(createMockInsertMessage(1001, 100, 10))

	sm.SealStaleChunks(1500, 0)

	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()
	assert.Len(t, segment.GetSealedChunks(), 1)
}

// Helper functions

func createTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}
}

func createL1SegmentMeta(segmentID, collectionID, partitionID int64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		SegmentId:    segmentID,
		CollectionId: collectionID,
		PartitionId:  partitionID,
		Vchannel:     "test-channel-v0",
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat: &streamingpb.SegmentAssignmentStat{
			BeginTimeTick: 1000,
			EndTimeTick:   1000,
		},
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
			L1: &streamingpb.L1SegmentPersistedStorage{},
		},
	}
}

func createL0SegmentMeta(segmentID, collectionID, partitionID int64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		SegmentId:    segmentID,
		CollectionId: collectionID,
		PartitionId:  partitionID,
		Vchannel:     "test-channel-v0",
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat: &streamingpb.SegmentAssignmentStat{
			BeginTimeTick: 1000,
			EndTimeTick:   1000,
		},
		PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
			L0: &streamingpb.L0SegmentPersistedStorage{},
		},
	}
}

// Mock message implementations
type mockInsertMessage struct {
	mock.Mock
	header   *messagespb.InsertMessageHeader
	vchannel string
	timetick uint64
}

func createMockInsertMessage(segmentID, partitionID int64, rows uint64) message.ImmutableInsertMessageV1 {
	return &mockInsertMessage{
		header: &messagespb.InsertMessageHeader{
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
					Rows:       rows,
					BinarySize: rows * 100,
				},
			},
		},
		vchannel: "test-channel-v0",
		timetick: 1500,
	}
}

func (m *mockInsertMessage) Header() *messagespb.InsertMessageHeader {
	return m.header
}

func (m *mockInsertMessage) VChannel() string {
	return m.vchannel
}

func (m *mockInsertMessage) PChannel() string {
	return m.vchannel
}

func (m *mockInsertMessage) TimeTick() uint64 {
	return m.timetick
}

func (m *mockInsertMessage) MessageType() message.MessageType {
	return message.MessageTypeInsert
}

func (m *mockInsertMessage) MessageID() message.MessageID {
	return nil
}

func (m *mockInsertMessage) EstimateSize() int {
	return 1000
}

func (m *mockInsertMessage) Properties() message.RProperties {
	return nil
}

func (m *mockInsertMessage) Version() message.Version {
	return message.VersionV1
}

func (m *mockInsertMessage) WALName() message.WALName {
	return message.WALName(0)
}

func (m *mockInsertMessage) RawHeader() any {
	return m.header
}

func (m *mockInsertMessage) RawBody() any {
	return nil
}

func (m *mockInsertMessage) Unmarshal(v any) error {
	return nil
}

func (m *mockInsertMessage) BarrierTimeTick() uint64 {
	return m.timetick
}

func (m *mockInsertMessage) Body() (*message.InsertRequest, error) {
	return nil, nil
}

func (m *mockInsertMessage) MustBody() *message.InsertRequest {
	return nil
}

func (m *mockInsertMessage) LastConfirmedMessageID() message.MessageID {
	return nil
}

func (m *mockInsertMessage) IntoImmutableMessageProto() *commonpb.ImmutableMessage {
	return nil
}

func (m *mockInsertMessage) IntoBroadcastMutableMessage() message.BroadcastMutableMessage {
	return nil
}

func (m *mockInsertMessage) BroadcastHeader() *message.BroadcastHeader {
	return nil
}

func (m *mockInsertMessage) IntoMessageProto() *messagespb.Message {
	return nil
}

func (m *mockInsertMessage) IsPersisted() bool {
	return false
}

func (m *mockInsertMessage) IsPChannelLevel() bool {
	return false
}

func (m *mockInsertMessage) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
}

func (m *mockInsertMessage) MessageTypeWithVersion() message.MessageTypeWithVersion {
	return message.MessageTypeWithVersion{}
}

func (m *mockInsertMessage) Payload() []byte {
	return nil
}

func (m *mockInsertMessage) TxnContext() *message.TxnContext {
	return nil
}

func (m *mockInsertMessage) ReplicateHeader() *message.ReplicateHeader {
	return nil
}

// Mock delete message implementation
type mockDeleteMessage struct {
	mock.Mock
	header   *messagespb.DeleteMessageHeader
	body     *message.DeleteRequest
	vchannel string
	timetick uint64
}

func createMockDeleteMessage(segmentID, partitionID int64, rows uint64) message.ImmutableDeleteMessageV1 {
	_ = segmentID
	return &mockDeleteMessage{
		header: &messagespb.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         rows,
		},
		body: &message.DeleteRequest{
			PartitionID: partitionID,
			PrimaryKeys: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
				},
			},
		},
		vchannel: "test-channel-v0",
		timetick: 1500,
	}
}

func (m *mockDeleteMessage) Header() *messagespb.DeleteMessageHeader {
	return m.header
}

func (m *mockDeleteMessage) VChannel() string {
	return m.vchannel
}

func (m *mockDeleteMessage) PChannel() string {
	return m.vchannel
}

func (m *mockDeleteMessage) TimeTick() uint64 {
	return m.timetick
}

func (m *mockDeleteMessage) MessageType() message.MessageType {
	return message.MessageTypeDelete
}

func (m *mockDeleteMessage) MessageID() message.MessageID {
	return nil
}

func (m *mockDeleteMessage) EstimateSize() int {
	return 500
}

func (m *mockDeleteMessage) Properties() message.RProperties {
	return nil
}

func (m *mockDeleteMessage) Version() message.Version {
	return message.VersionV1
}

func (m *mockDeleteMessage) WALName() message.WALName {
	return message.WALName(0)
}

func (m *mockDeleteMessage) RawHeader() any {
	return m.header
}

func (m *mockDeleteMessage) RawBody() any {
	return m.body
}

func (m *mockDeleteMessage) Unmarshal(v any) error {
	return nil
}

func (m *mockDeleteMessage) BarrierTimeTick() uint64 {
	return m.timetick
}

func (m *mockDeleteMessage) Body() (*message.DeleteRequest, error) {
	return m.body, nil
}

func (m *mockDeleteMessage) MustBody() *message.DeleteRequest {
	return m.body
}

func (m *mockDeleteMessage) LastConfirmedMessageID() message.MessageID {
	return nil
}

func (m *mockDeleteMessage) IntoImmutableMessageProto() *commonpb.ImmutableMessage {
	return nil
}

func (m *mockDeleteMessage) IntoBroadcastMutableMessage() message.BroadcastMutableMessage {
	return nil
}

func (m *mockDeleteMessage) BroadcastHeader() *message.BroadcastHeader {
	return nil
}

func (m *mockDeleteMessage) IntoMessageProto() *messagespb.Message {
	return nil
}

func (m *mockDeleteMessage) IsPersisted() bool {
	return false
}

func (m *mockDeleteMessage) IsPChannelLevel() bool {
	return false
}

func (m *mockDeleteMessage) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
}

func (m *mockDeleteMessage) MessageTypeWithVersion() message.MessageTypeWithVersion {
	return message.MessageTypeWithVersion{}
}

func (m *mockDeleteMessage) Payload() []byte {
	return nil
}

func (m *mockDeleteMessage) TxnContext() *message.TxnContext {
	return nil
}

func (m *mockDeleteMessage) ReplicateHeader() *message.ReplicateHeader {
	return nil
}
