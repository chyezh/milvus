// Package recovery_integration tests the integrated recovery module functionality
package recovery_integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery/gsegment"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// TestSegmentManager tests the SegmentManager functionality
func TestSegmentManager(t *testing.T) {
	// Create a mock chunk manager for testing
	// For unit tests, we'll use a simpler approach
	cm := createMockChunkManager()

	// Test SegmentManager creation
	t.Run("CreateSegmentManager", func(t *testing.T) {
		sm := gsegment.NewSegmentManager(cm)
		assert.NotNil(t, sm, "SegmentManager should be created successfully")
	})

	// Test L1 segment operations
	t.Run("L1SegmentOperations", func(t *testing.T) {
		sm := gsegment.NewSegmentManager(cm)

		// Create test schema
		schema := createTestSchema()

		// Create L1 segment metadata
		l1Meta := &streamingpb.SegmentAssignmentMeta{
			SegmentId:          1001,
			CollectionId:       1,
			PartitionId:        100,
			Vchannel:           "test-channel_v0",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1000,
			Stat: &streamingpb.SegmentAssignmentStat{
				BeginTimeTick: 1000,
				EndTimeTick:   1000,
			},
			PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
				L1: &streamingpb.L1SegmentPersistedStorage{
					ManifestPath: "",
					Binlogs:      nil,
				},
			},
		}

		// Create L1 segment
		sm.CreateL1Segment(l1Meta, schema)

		// Flush L1 segment
		sm.FlushL1Segment(1001, 2000)

		// Check dirty snapshots
		snapshots := sm.GetDirtySnapshots()
		assert.NotNil(t, snapshots[1001], "L1 segment should have dirty snapshot")
	})

	// Test L0 segment operations
	t.Run("L0SegmentOperations", func(t *testing.T) {
		sm := gsegment.NewSegmentManager(cm)

		// Create L0 segment metadata
		l0Meta := &streamingpb.SegmentAssignmentMeta{
			SegmentId:          2001,
			CollectionId:       1,
			PartitionId:        100,
			Vchannel:           "test-channel_v0",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 1000,
			Stat: &streamingpb.SegmentAssignmentStat{
				BeginTimeTick: 1000,
				EndTimeTick:   1000,
			},
			PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
				L0: &streamingpb.L0SegmentPersistedStorage{
					DeltaBinlog: nil,
				},
			},
		}

		// Create L0 segment
		sm.CreateL0Segment(l0Meta)

		// Flush L0 segment
		sm.FlushL0Segment(2001, 2000)

		// Check dirty snapshots
		snapshots := sm.GetDirtySnapshots()
		assert.NotNil(t, snapshots[2001], "L0 segment should have dirty snapshot")
	})

	// Test recovery from snapshots
	t.Run("RecoveryFromSnapshots", func(t *testing.T) {
		sm := gsegment.NewSegmentManager(cm)

		// Create segments with metadata
		segments := map[int64]*streamingpb.SegmentAssignmentMeta{
			3001: {
				SegmentId:          3001,
				CollectionId:       1,
				PartitionId:        100,
				CheckpointTimeTick: 3000,
				PersistedStorage: &streamingpb.SegmentAssignmentMeta_L1{
					L1: &streamingpb.L1SegmentPersistedStorage{},
				},
			},
			4001: {
				SegmentId:          4001,
				CollectionId:       1,
				PartitionId:        100,
				CheckpointTimeTick: 3000,
				PersistedStorage: &streamingpb.SegmentAssignmentMeta_L0{
					L0: &streamingpb.L0SegmentPersistedStorage{},
				},
			},
		}

		// Recover from snapshots
		sm.RecoverFromSnapshot(segments)

		// For L0 segments, we can flush without panics
		require.NotPanics(t, func() {
			sm.FlushL0Segment(4001, 4000)
		})

		// Note: L1 segments need schema to be properly recovered
		// In production, schema is recovered from vchannel metadata
	})
}

// createMockChunkManager creates a mock chunk manager for testing
func createMockChunkManager() storage.ChunkManager {
	// In a real test, this would be properly mocked
	// For now, we return nil as we're not actually using it in the test
	return nil
}

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