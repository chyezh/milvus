package message

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestRefCountedImmutableMessageDelegatesAndCompletes(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var completed atomic.Int32
	controller := NewRefCountedImmutableMessage(raw, func() {
		completed.Add(1)
	})

	assert.Equal(t, raw.MessageType(), controller.MessageType())
	assert.Equal(t, raw.TimeTick(), controller.TimeTick())
	assert.True(t, raw.MessageID().EQ(controller.MessageID()))
	assert.True(t, raw.LastConfirmedMessageID().EQ(controller.LastConfirmedMessageID()))
	assert.Equal(t, raw.Payload(), controller.Payload())
	assert.Equal(t, raw.Properties().ToRawMap(), controller.Properties().ToRawMap())
	assert.False(t, controller.Completed())

	first := controller.Retain()
	second := controller.Retain()
	require.NotSame(t, first, second)
	assert.False(t, first.Sealed())
	assert.False(t, first.IsExclusive())

	controller.Seal()
	assert.True(t, first.Sealed())
	assert.False(t, first.IsExclusive())
	assert.False(t, controller.Completed())
	assert.Zero(t, completed.Load())

	first.Release()
	first.Release()
	assert.True(t, second.IsExclusive())
	assert.False(t, controller.Completed())

	second.Release()
	second.Release()
	assert.True(t, controller.Completed())
	assert.Equal(t, int32(1), completed.Load())
	assert.Panics(t, func() { _ = controller.TimeTick() })
	assert.Panics(t, func() { _ = first.TimeTick() })
	assert.Panics(t, func() { _ = second.Sealed() })
}

func TestRefCountedImmutableMessageCompletesAtSealWithoutConsumers(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var completed atomic.Int32
	controller := NewRefCountedImmutableMessage(raw, func() {
		completed.Add(1)
	})

	controller.Seal()
	controller.Seal()

	assert.True(t, controller.Completed())
	assert.Equal(t, int32(1), completed.Load())
	assert.Panics(t, func() { controller.Retain() })
}

func TestRefCountedImmutableMessageClearsPayloadAfterCompletionCallback(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var controller RefCountedImmutableMessageController
	controller = NewRefCountedImmutableMessage(raw, func() {
		core := controller.(*refCountedImmutableMessage).core
		core.finishCompletion(nil, false)
		core.mu.Lock()
		assert.NotNil(t, core.message)
		core.mu.Unlock()
	})

	controller.Seal()

	assert.True(t, controller.Completed())
	core := controller.(*refCountedImmutableMessage).core
	core.mu.Lock()
	assert.Nil(t, core.message)
	core.mu.Unlock()
	assert.Panics(t, func() { _ = controller.TimeTick() })
}

func TestRefCountedImmutableMessageRetainAndSealAreSerialized(t *testing.T) {
	for range 100 {
		raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
			IntoImmutableMessage(testMessageID("11"))
		var completed atomic.Int32
		controller := NewRefCountedImmutableMessage(raw, func() {
			completed.Add(1)
		})
		start := make(chan struct{})
		retained := make(chan RetainedImmutableMessage, 1)
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
			retained <- controller.Retain()
		}()
		go func() {
			defer workers.Done()
			<-start
			controller.Seal()
		}()
		close(start)
		workers.Wait()
		if handle := <-retained; handle != nil {
			handle.Release()
		}

		assert.True(t, controller.Completed())
		assert.Equal(t, int32(1), completed.Load())
	}
}

func TestRefCountedSpecializedImmutableMessagePreservesLifecycle(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	controller := NewRefCountedImmutableMessage(raw, nil)

	specialized := MustAsImmutableInsertMessageV1(controller)
	refCounted, ok := specialized.(RefCountedSpecializedImmutableMessage[*InsertMessageHeader, *msgpb.InsertRequest])
	require.True(t, ok)
	assert.Equal(t, int64(100), refCounted.Header().Partitions[0].SegmentAssignment.SegmentId)
	assert.Equal(t, int64(1), refCounted.MustBody().CollectionID)

	retained := refCounted.Retain()
	retainedInsert := MustAsRetainedImmutableInsertMessageV1(retained)
	assert.Equal(t, int64(100), retainedInsert.Header().Partitions[0].SegmentAssignment.SegmentId)

	controller.Seal()
	assert.True(t, retainedInsert.IsExclusive())
	retainedInsert.Release()
	assert.True(t, controller.Completed())
	assert.Panics(t, func() { retainedInsert.MustBody() })
}

func TestRetainedImmutableTxnMessageOwnsBorrowedChildren(t *testing.T) {
	txn := buildRefCountedTestTxn(t)
	controller := NewRefCountedImmutableMessage(txn, nil)
	retained := controller.Retain()
	retainedTxn := AsImmutableTxnMessage(retained)
	require.NotNil(t, retainedTxn)
	require.Equal(t, 1, retainedTxn.Size())

	var child ImmutableMessage
	require.NoError(t, retainedTxn.RangeOver(func(msg ImmutableMessage) error {
		child = msg
		assert.Equal(t, txn.TimeTick(), msg.TimeTick())
		return nil
	}))
	assert.NotNil(t, retainedTxn.Begin())
	assert.NotNil(t, retainedTxn.Commit())

	controller.Seal()
	retained.Release()
	assert.True(t, controller.Completed())
	assert.Panics(t, func() { _ = retainedTxn.Size() })
	assert.Panics(t, func() { _ = child.TimeTick() })
}

func TestCloneImmutableMessageOutlivesRefCountedSource(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	sourcePayload := raw.Payload()
	controller := NewRefCountedImmutableMessage(raw, nil)

	cloned := CloneImmutableMessage(controller)
	controller.Seal()

	assert.True(t, controller.Completed())
	assert.Equal(t, uint64(20), cloned.TimeTick())
	assert.Equal(t, int64(100), MustAsImmutableInsertMessageV1(cloned).Header().Partitions[0].SegmentAssignment.SegmentId)
	require.NotEmpty(t, sourcePayload)
	require.NotEmpty(t, cloned.Payload())
	assert.NotSame(t, &sourcePayload[0], &cloned.Payload()[0])
}

func TestCloneImmutableTxnMessageCopiesBorrowedChildren(t *testing.T) {
	controller := NewRefCountedImmutableMessage(buildRefCountedTestTxn(t), nil)
	cloned := CloneImmutableMessage(controller)
	clonedTxn := AsImmutableTxnMessage(cloned)
	require.NotNil(t, clonedTxn)

	controller.Seal()

	require.Equal(t, 1, clonedTxn.Size())
	require.NoError(t, clonedTxn.RangeOver(func(inner ImmutableMessage) error {
		assert.Equal(t, MessageTypeInsert, inner.MessageType())
		return nil
	}))
}

func buildRefCountedTestTxn(t *testing.T) ImmutableTxnMessage {
	t.Helper()
	txnCtx := TxnContext{TxnID: 1, Keepalive: time.Second}
	begin, err := NewBeginTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&BeginTxnMessageHeader{}).
		WithBody(&BeginTxnMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	immutableBegin := begin.WithTxnContext(txnCtx).
		WithTimeTick(1).
		WithLastConfirmed(testMessageID("1")).
		IntoImmutableMessage(testMessageID("1"))
	beginMessage := MustAsImmutableBeginTxnMessageV2(immutableBegin)

	insert, err := NewInsertMessageBuilderV1().
		WithVChannel("vchan").
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)

	commit, err := NewCommitTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&CommitTxnMessageHeader{}).
		WithBody(&CommitTxnMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	immutableCommit := commit.WithTxnContext(txnCtx).
		WithTimeTick(3).
		WithLastConfirmed(testMessageID("3")).
		IntoImmutableMessage(testMessageID("4"))
	commitMessage := MustAsImmutableCommitTxnMessageV2(immutableCommit)

	txn, err := NewImmutableTxnMessageBuilder(beginMessage).
		Add(insert.WithTimeTick(2).WithTxnContext(txnCtx).IntoImmutableMessage(testMessageID("2"))).
		Build(commitMessage)
	require.NoError(t, err)
	return txn
}
