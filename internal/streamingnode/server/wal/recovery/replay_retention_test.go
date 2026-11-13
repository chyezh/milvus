package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type retentionTruncatorStub struct{ walimpls.WALImpls }

func (*retentionTruncatorStub) Truncate(context.Context, message.MessageID) error {
	panic("patch with mockey")
}

func TestTruncateHonorsReplayRetention(t *testing.T) {
	for _, outcome := range []string{"protected", "unavailable", "unsupported", "backend-error"} {
		t.Run(outcome, func(t *testing.T) {
			mockey.PatchConvey("publication does not authorize unprotected physical deletion", t, func() {
				resource.InitForTest(t)
				rs := &recoveryStorageImpl{channel: types.PChannelInfo{Name: "p", Term: 8}}
				rs.SetLogger(resource.Resource().Logger())
				rs.simpleTruncateCheckpoint(t.Context(), nil)
				rs.truncator = &retentionTruncatorStub{}
				rs.simpleTruncateCheckpoint(t.Context(), nil)
				rs.simpleTruncateCheckpoint(t.Context(), &WALCheckpoint{})
				cp := &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(100), TimeTick: 1000, LogGeneration: 2}
				mockey.Mock(utility.AuthorizeWALTruncation).To(func(_ context.Context, _ metastore.StreamingNodeCataLog, channel string, term int64, requested *streamingpb.WALCheckpoint) (message.MessageID, error) {
					require.Equal(t, "p", channel)
					require.EqualValues(t, 8, term)
					require.EqualValues(t, 2, requested.LogGeneration)
					require.EqualValues(t, 1000, requested.TimeTick)
					if outcome == "unavailable" {
						return nil, merr.ErrServiceUnavailable
					}
					if outcome == "unsupported" {
						return nil, nil
					}
					return walimplstest.NewTestMessageID(10), nil
				}).Build()
				truncate := mockey.Mock((*retentionTruncatorStub).Truncate).To(func(_ *retentionTruncatorStub, _ context.Context, bound message.MessageID) error {
					require.True(t, bound.EQ(walimplstest.NewTestMessageID(10)))
					if outcome == "backend-error" {
						return merr.ErrServiceUnavailable
					}
					return nil
				}).Build()
				rs.simpleTruncateCheckpoint(t.Context(), cp)
				if outcome == "unavailable" || outcome == "unsupported" {
					require.Zero(t, truncate.Times())
				} else {
					require.Equal(t, 1, truncate.Times())
				}
			})
		})
	}
}
