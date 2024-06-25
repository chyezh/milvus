package options

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/mock_message"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

func TestDeliver(t *testing.T) {
	policy := DeliverPolicyAll()
	_ = policy.Policy.(*streamingpb.DeliverPolicy_All)

	policy = DeliverPolicyLatest()
	_ = policy.Policy.(*streamingpb.DeliverPolicy_Latest)

	// TODO: No assert should be in NewPBMessageIDFromMessageID.
	// Remove the panic testing.
	assert.Panics(t, func() {
		messageID := mock_message.NewMockMessageID(t)
		policy = DeliverPolicyStartFrom(messageID)
		from := policy.Policy.(*streamingpb.DeliverPolicy_StartFrom)
		assert.Equal(t, messageID, from.StartFrom)
	})

	assert.Panics(t, func() {
		messageID := mock_message.NewMockMessageID(t)
		policy = DeliverPolicyStartAfter(messageID)
		after := policy.Policy.(*streamingpb.DeliverPolicy_StartAfter)
		assert.Equal(t, messageID, after.StartAfter)
	})
}
