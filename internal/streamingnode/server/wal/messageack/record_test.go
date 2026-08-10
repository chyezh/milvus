package messageack

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRecordCompletesAfterSealAndAllRefsDone(t *testing.T) {
	point := utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  20,
	}
	var completed atomic.Int32
	record := NewRecord(point, func() {
		completed.Add(1)
	})

	require.True(t, point.MessageID.EQ(record.Point().MessageID))
	assert.Equal(t, point.TimeTick, record.Point().TimeTick)
	assert.False(t, record.Sealed())
	assert.Equal(t, int64(1), record.RefCount())
	assert.False(t, record.Completed())

	first := record.Retain()
	second := record.Retain()
	assert.Equal(t, int64(3), record.RefCount())

	first.Done()
	first.Done()
	assert.Equal(t, int64(2), record.RefCount())
	assert.Zero(t, completed.Load())

	record.Seal()
	assert.True(t, record.Sealed())
	assert.Equal(t, int64(1), record.RefCount())
	assert.False(t, record.Completed())
	assert.Zero(t, completed.Load())

	second.Done()
	second.Done()
	assert.Zero(t, record.RefCount())
	assert.True(t, record.Completed())
	assert.Equal(t, int32(1), completed.Load())

	record.Seal()
	assert.Equal(t, int32(1), completed.Load())
}

func TestRecordCompletesAtSealWithoutConsumers(t *testing.T) {
	var completed atomic.Int32
	record := NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, func() {
		completed.Add(1)
	})

	record.Seal()

	assert.True(t, record.Completed())
	assert.Zero(t, record.RefCount())
	assert.Equal(t, int32(1), completed.Load())
}

func TestRecordRetainAfterSealPanics(t *testing.T) {
	record := NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)
	record.Seal()

	assert.Panics(t, func() {
		record.Retain()
	})
}

func TestRecordPointReturnsCopy(t *testing.T) {
	record := NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, nil)

	point := record.Point()
	point.TimeTick = 100

	assert.Equal(t, uint64(10), record.Point().TimeTick)
}

func TestRecordRetainAndSealAreSerialized(t *testing.T) {
	for range 100 {
		var completed atomic.Int32
		record := NewRecord(utility.WALConsumeCheckpoint{TimeTick: 10}, func() {
			completed.Add(1)
		})
		start := make(chan struct{})
		retained := make(chan Ref, 1)
		var workers sync.WaitGroup
		workers.Add(2)
		go func() {
			defer workers.Done()
			defer func() {
				if recover() != nil {
					retained <- nil
				}
			}()
			<-start
			retained <- record.Retain()
		}()
		go func() {
			defer workers.Done()
			<-start
			record.Seal()
		}()
		close(start)
		workers.Wait()
		if ref := <-retained; ref != nil {
			ref.Done()
		}

		assert.True(t, record.Completed())
		assert.Equal(t, int32(1), completed.Load())
	}
}
