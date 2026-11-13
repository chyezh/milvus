//go:build test

package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// TestFenceConsumeCheckpointStampsOwnTerm proves the takeover claim writes the
// checkpoint back carrying this term and nothing else: the position must be
// preserved, because the claim is a fence, not an advancement.
func TestFenceConsumeCheckpointStampsOwnTerm(t *testing.T) {
	rs := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1)})
	defer rs.closeRecoveryResources()
	rs.checkpoint.TimeTick = 42

	var received *streamingpb.WALCheckpoint
	catalog := &mock_metastore.MockStreamingNodeCataLog{}
	patch := mockey.Mock((*mock_metastore.MockStreamingNodeCataLog).SaveRecoverySnapshot).To(func(_ *mock_metastore.MockStreamingNodeCataLog, _ context.Context, name string, snapshot *metastore.WALRecoverySnapshot) error {
		require.Equal(t, "test-pchannel", name)
		received = proto.CloneOf(snapshot.ConsumeCheckpoint)
		return nil
	}).Build()
	defer patch.UnPatch()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 3))
	require.NotNil(t, received)
	assert.Equal(t, int64(3), received.GetTerm(), "the claim stamps this term")
	assert.Equal(t, uint64(42), received.GetTimeTick(), "the claim leaves the position alone")
	// Later advancements carry the term, so the compare-and-swap keeps
	// accepting this publisher.
	assert.Equal(t, int64(3), rs.checkpoint.Term)
	require.EqualValues(t, 1, rs.checkpoint.LogGeneration)
	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 4))
	require.EqualValues(t, 1, received.LogGeneration, "writer takeover keeps physical log identity")
}

// TestFenceConsumeCheckpointSkipsWhenAlreadyClaimed proves a reopen that does
// not change ownership writes nothing: the recorded term already matches.
func TestFenceConsumeCheckpointSkipsWhenAlreadyClaimed(t *testing.T) {
	rs := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1)})
	defer rs.closeRecoveryResources()
	rs.checkpoint.Term = 7
	rs.checkpoint.LogGeneration = 1
	// No catalog expectation: any catalog call fails the test.
	catalog := &mock_metastore.MockStreamingNodeCataLog{}
	patch := mockey.Mock((*mock_metastore.MockStreamingNodeCataLog).SaveRecoverySnapshot).To(func(_ *mock_metastore.MockStreamingNodeCataLog, _ context.Context, _ string, _ *metastore.WALRecoverySnapshot) error {
		t.Error("already fenced checkpoint must not be persisted again")
		return nil
	}).Build()
	defer patch.UnPatch()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 7))
}
