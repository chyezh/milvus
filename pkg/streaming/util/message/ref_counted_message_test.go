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

func TestRefCountedImmutableMessageOwnerCloneAndFinalize(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalizerCalls atomic.Int32
	owner := NewRefCountedImmutableMessageOwner(raw, func() {
		finalizerCalls.Add(1)
	})

	assert.Same(t, raw, owner.Message())
	first := owner.Clone()
	second := first.Clone()
	require.NotSame(t, first, second)
	assert.Same(t, raw, first.Message())
	assert.Same(t, raw, second.Message())

	owner.Release()
	assert.Zero(t, finalizerCalls.Load())
	first.Release()
	assert.Zero(t, finalizerCalls.Load())
	second.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load())

	assert.Panics(t, func() { _ = owner.Message() })
	assert.Panics(t, func() { _ = first.Message() })
	assert.Panics(t, func() { _ = second.Message() })
	first.Release()
}

func TestRefCountedImmutableMessageOwnerReleaseDoesNotInvalidateClones(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewRefCountedImmutableMessageOwner(raw, nil)
	clone := owner.Clone()

	owner.Release()
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, uint64(20), clone.Message().TimeTick())
	clone.Release()
}

func TestRefCountedImmutableMessageOwnerWithoutConsumers(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalized atomic.Bool
	owner := NewRefCountedImmutableMessageOwner(raw, func() {
		finalized.Store(true)
	})

	owner.Release()
	assert.True(t, finalized.Load())
}

func TestRetainedImmutableMessageConcurrentReleaseFinalizesOnce(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalizerCalls atomic.Int32
	owner := NewRefCountedImmutableMessageOwner(raw, func() {
		finalizerCalls.Add(1)
	})
	handles := make([]RetainedImmutableMessage, 64)
	for i := range handles {
		handles[i] = owner.Clone()
	}
	owner.Release()

	var wg sync.WaitGroup
	for _, handle := range handles {
		wg.Go(handle.Release)
	}
	wg.Wait()

	assert.Equal(t, int32(1), finalizerCalls.Load())
}

func TestRetainedImmutableMessageCloneIsIndependent(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewRefCountedImmutableMessageOwner(raw, nil)
	first := owner.Clone()
	second := first.Clone()

	first.Release()
	assert.Equal(t, uint64(20), second.Message().TimeTick())
	second.Release()
	owner.Release()
}

func TestRetainedMessageDoesNotExposeMessageAfterRelease(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewRefCountedImmutableMessageOwner(raw, nil)
	retained := owner.Clone()
	typed := RetainedMessage[ImmutableMessage]{message: raw, retained: retained}

	typed.Release()
	assert.Panics(t, func() { _ = typed.Message() })
	owner.Release()
}

func TestRetainedTxnKeepsWholeTransactionAlive(t *testing.T) {
	txn := buildRefCountedTestTxn(t)
	owner := NewRefCountedImmutableMessageOwner(txn, nil)
	retained := owner.Clone()

	retainedTxn := AsImmutableTxnMessage(retained.Message())
	require.NotNil(t, retainedTxn)
	require.Equal(t, 1, retainedTxn.Size())
	require.NoError(t, retainedTxn.RangeOver(func(inner ImmutableMessage) error {
		assert.Equal(t, MessageTypeInsert, inner.MessageType())
		return nil
	}))

	retained.Release()
	owner.Release()
}

func TestCloneImmutableMessageCanOutliveOwner(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewRefCountedImmutableMessageOwner(raw, nil)
	cloned := CloneImmutableMessage(owner.Message())
	owner.Release()

	assert.Equal(t, uint64(20), cloned.TimeTick())
	assert.NotSame(t, &raw.Payload()[0], &cloned.Payload()[0])
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
