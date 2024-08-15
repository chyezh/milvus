package utility

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

func TestTxnBuffer(t *testing.T) {
	b := NewTxnBuffer(log.With())

	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, nil, 1),
		newInsertMessage(t, nil, 2),
		newInsertMessage(t, nil, 3),
	}, 3)
	assert.Len(t, msgs, 3)

	msgs = b.HandleImmutableMessages([]message.ImmutableMessage{
		newInsertMessage(t, nil, 1),
		newInsertMessage(t, &message.TxnContext{
			TxnID:    1,
			BeginTSO: 1,
			TTL:      time.Second,
		}, 2),
		newInsertMessage(t, nil, 3),
	}, 3)
	assert.Len(t, msgs, 2)

	msgs := b.HandleImmutableMessages([]message.ImmutableMessage{})
}

func newInsertMessage(t *testing.T, txnCtx *message.TxnContext, idx int64) message.ImmutableMessage {
	msg, err := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	if txnCtx != nil {
		msg = msg.WithTxnContext(*txnCtx)
	}
	return msg.WithTimeTick(tsoutil.GetCurrentTime()).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(walimplstest.NewTestMessageID(idx))
}

func newBeginMessage(t *testing.T, txnCtx *message.TxnContext, idx int64) message.ImmutableMessage {
	msg, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	return msg.WithTimeTick(tsoutil.GetCurrentTime()).
		WithLastConfirmedUseMessageID().
		WithTxnContext(*txnCtx).
		IntoImmutableMessage(walimplstest.NewTestMessageID(idx))
}
