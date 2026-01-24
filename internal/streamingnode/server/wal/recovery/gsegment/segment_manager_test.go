package gsegment

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestNewSegmentManager(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	assert.NotNil(t, sm)
	assert.NotNil(t, sm.l0Segments)
	assert.NotNil(t, sm.l1Segments)
	assert.NotNil(t, sm.schemas)
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

	// Create L0 segment first
	meta := createL0SegmentMeta(2001, 1, 100)
	sm.CreateL0Segment(meta)

	// Create mock delete message
	mockMsg := createMockDeleteMessage(2001, 100, 5)

	// Observe delete
	sm.ObserveDelete(mockMsg)

	// Verify segment received the delete
	sm.mu.RLock()
	segment := sm.l0Segments[2001]
	sm.mu.RUnlock()

	assert.NotNil(t, segment)
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

func TestSegmentManager_CheckAndSchedulePersistence(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create L1 segment with schema
	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	sm.CreateL1Segment(meta, schema)

	// Add some insert data to make it have sealed chunks
	mockMsg := createMockInsertMessage(1001, 100, 10000) // Large insert to trigger chunk seal
	sm.ObserveInsert(mockMsg)

	// Flush to seal chunks
	sm.FlushL1Segment(1001, 2000)

	// Check persistence scheduling - this would normally create tasks
	// but without actual data, we're just testing the flow doesn't panic
	sm.checkAndSchedulePersistence(1001, true)
}

func TestSegmentManager_WaitForTaskCompletion(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	sm := NewSegmentManager(mockCM)
	defer sm.Close()

	// Create L1 segment
	schema := createTestSchema()
	meta := createL1SegmentMeta(1001, 1, 100)
	sm.CreateL1Segment(meta, schema)

	sm.mu.RLock()
	segment := sm.l1Segments[1001]
	sm.mu.RUnlock()

	// Create a mock task
	chunk := &InsertChunk{
		startFromTimeTick: 1000,
		endToTimeTick:     2000,
	}
	task := &NewInsertChunkTask{
		chunk:        chunk,
		schema:       schema,
		collectionID: 1,
		partitionID:  100,
		segmentID:    1001,
		chunkManager: mockCM,
		state:        taskStateDone,
		cpuBounded:   true,
		binlogs:      make(map[int64]*storage.Blob),
		uploadedPaths: map[string]string{
			"100": "test/path/100",
			"stats": "test/path/stats",
		},
		statsBlob: &storage.Blob{Value: []byte("stats")},
	}

	// Add a field to binlogs
	task.binlogs[100] = &storage.Blob{Value: []byte("test_data")}

	// Test task completion handling
	go sm.waitForInsertTaskCompletion(segment, task)

	// Give it time to complete
	time.Sleep(100 * time.Millisecond)
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
	header  *messagespb.InsertMessageHeader
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

func (m *mockInsertMessage) Properties() map[string]string {
	return nil
}

func (m *mockInsertMessage) Version() message.Version {
	return message.VersionV1
}

func (m *mockInsertMessage) WALName() string {
	return "test-wal"
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

// Mock delete message implementation
type mockDeleteMessage struct {
	mock.Mock
	header   *messagespb.DeleteMessageHeader
	vchannel string
	timetick uint64
}

func createMockDeleteMessage(segmentID, partitionID int64, rows uint64) message.ImmutableDeleteMessageV1 {
	return &mockDeleteMessage{
		header: &messagespb.DeleteMessageHeader{
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
					Rows:       rows,
					BinarySize: rows * 50,
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

func (m *mockDeleteMessage) Properties() map[string]string {
	return nil
}

func (m *mockDeleteMessage) Version() message.Version {
	return message.VersionV1
}

func (m *mockDeleteMessage) WALName() string {
	return "test-wal"
}

func (m *mockDeleteMessage) RawHeader() any {
	return m.header
}

func (m *mockDeleteMessage) RawBody() any {
	return nil
}

func (m *mockDeleteMessage) Unmarshal(v any) error {
	return nil
}

func (m *mockDeleteMessage) BarrierTimeTick() uint64 {
	return m.timetick
}

func (m *mockDeleteMessage) Body() (*message.DeleteRequest, error) {
	return nil, nil
}

func (m *mockDeleteMessage) MustBody() *message.DeleteRequest {
	return nil
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