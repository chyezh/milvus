package messageack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestTrackerAdvancesOnlyContinuousCompletedPrefix(t *testing.T) {
	initial := utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
	}
	advanced := make([]utility.WALConsumeCheckpoint, 0, 2)
	tracker := NewTracker(initial, func(point utility.WALConsumeCheckpoint) {
		advanced = append(advanced, point)
	})

	first := tracker.Track(testPoint(2, 20))
	second := tracker.Track(testPoint(3, 30))
	firstRef := first.Retain()
	secondRef := second.Retain()
	first.Seal()
	second.Seal()

	secondRef.Done()
	point := tracker.CompletedPoint()
	require.True(t, initial.MessageID.EQ(point.MessageID))
	assert.Equal(t, initial.TimeTick, point.TimeTick)
	assert.Empty(t, advanced)
	assert.Equal(t, 2, tracker.Pending())

	firstRef.Done()
	point = tracker.CompletedPoint()
	require.True(t, walimplstest.NewTestMessageID(3).EQ(point.MessageID))
	assert.Equal(t, uint64(30), point.TimeTick)
	require.Len(t, advanced, 1)
	require.True(t, walimplstest.NewTestMessageID(3).EQ(advanced[0].MessageID))
	assert.Equal(t, uint64(30), advanced[0].TimeTick)
	assert.Zero(t, tracker.Pending())
}

func TestTrackerPreservesLastConfirmedMessageID(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(100)
	tracker := NewTracker(utility.WALConsumeCheckpoint{}, nil)
	record := tracker.Track(utility.WALConsumeCheckpoint{
		MessageID: lastConfirmed,
		TimeTick:  200,
	})

	record.Seal()

	point := tracker.CompletedPoint()
	require.NotNil(t, point.MessageID)
	assert.True(t, lastConfirmed.EQ(point.MessageID))
	assert.Equal(t, uint64(200), point.TimeTick)
}

func TestTrackerCompletedPointReturnsCopy(t *testing.T) {
	tracker := NewTracker(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)

	point := tracker.CompletedPoint()
	point.TimeTick = 100

	assert.Equal(t, uint64(10), tracker.CompletedPoint().TimeTick)
}

func testPoint(messageID int64, timetick uint64) utility.WALConsumeCheckpoint {
	return utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(messageID),
		TimeTick:  timetick,
	}
}
