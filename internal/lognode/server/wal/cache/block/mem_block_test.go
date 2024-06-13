package block

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimplstest"
	"github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/mock_message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/stretchr/testify/assert"
)

func TestMemBlockImpl(t *testing.T) {
	// Test mutable block concurrent read write.
	mutableB := NewMutableBlock()
	var immutableB ImmutableBlock
	msgCount := 100
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < msgCount; i++ {
			time.Sleep(1 * time.Millisecond)
			msgID := walimplstest.NewTestMessageID(int64(i))
			msg := mock_message.NewMockImmutableMessage(t)
			msg.EXPECT().MessageID().Return(msgID).Maybe()
			msg.EXPECT().EstimateSize().Return(2)
			mutableB.Append([]message.ImmutableMessage{msg})
			assert.Equal(t, (i+1)*2, mutableB.EstimateSize())
		}
		immutableB = mutableB.Seal()
	}()
	go func() {
		defer wg.Done()
		testScan(t, mutableB, walimplstest.NewTestMessageID(0), msgCount)
	}()
	go func() {
		defer wg.Done()
		testScan(t, mutableB, walimplstest.NewTestMessageID(50), msgCount-50)
	}()
	wg.Wait()
	assert.Equal(t, 2*msgCount, immutableB.EstimateSize())

	// Test immutable and mutable block concurrent read.
	wg.Add(4)
	go func() {
		defer wg.Done()
		testScan(t, mutableB, walimplstest.NewTestMessageID(0), msgCount)
	}()
	go func() {
		defer wg.Done()
		testScan(t, mutableB, walimplstest.NewTestMessageID(20), msgCount-20)
	}()
	go func() {
		defer wg.Done()
		testScan(t, immutableB, walimplstest.NewTestMessageID(50), msgCount-50)
	}()
	go func() {
		defer wg.Done()
		testScan(t, immutableB, walimplstest.NewTestMessageID(80), msgCount-80)
	}()
	wg.Wait()
}

func TestMemBlockTimeoutImpl(t *testing.T) {
	mutableB := NewMutableBlock()
	scanner := mutableB.ReadFrom(walimplstest.NewTestMessageID(100))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := scanner.Scan(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	msgID := walimplstest.NewTestMessageID(0)
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(msgID).Maybe()
	msg.EXPECT().EstimateSize().Return(2)
	mutableB.Append([]message.ImmutableMessage{msg})

	scanner = mutableB.ReadFrom(walimplstest.NewTestMessageID(0))
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = scanner.Scan(ctx)
	assert.NoError(t, err)
	err = scanner.Scan(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func testScan(t *testing.T, b Block, msgID message.MessageID, expectCnt int) {
	s := b.ReadFrom(msgID)
	cnt := 0
	for s.Scan(context.TODO()) == nil {
		assert.NotNil(t, s.Message())
		cnt++
	}
	assert.Equal(t, cnt, expectCnt)
}
