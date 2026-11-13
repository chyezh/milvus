package utility

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type replayRetentionCatalogStub struct {
	metastore.StreamingNodeCataLog
	metastore.WALReplayRetentionCatalog
}

func (*replayRetentionCatalogStub) AuthorizeWALTruncation(context.Context, string, int64, *streamingpb.WALCheckpoint) (*streamingpb.WALCheckpoint, error) {
	panic("patch with mockey")
}

func TestAuthorizeWALTruncation(t *testing.T) {
	bound, err := AuthorizeWALTruncation(t.Context(), nil, "p", 7, nil)
	require.NoError(t, err)
	require.Nil(t, bound, "no retention capability must preserve all backlog")
	for _, failure := range []string{"none", "catalog", "invalid-position"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("only the catalog-authorized bound can reach the backend", t, func() {
				cp := &streamingpb.WALCheckpoint{MessageId: walimplstest.NewTestMessageID(100).IntoProto()}
				mockey.Mock((*replayRetentionCatalogStub).AuthorizeWALTruncation).To(func(_ *replayRetentionCatalogStub, _ context.Context, channel string, term int64, requested *streamingpb.WALCheckpoint) (*streamingpb.WALCheckpoint, error) {
					require.Equal(t, "p", channel)
					require.EqualValues(t, 7, term)
					require.Same(t, cp, requested)
					if failure == "catalog" {
						return nil, merr.ErrServiceUnavailable
					}
					if failure == "invalid-position" {
						return &streamingpb.WALCheckpoint{}, nil
					}
					return &streamingpb.WALCheckpoint{MessageId: walimplstest.NewTestMessageID(10).IntoProto()}, nil
				}).Build()
				bound, err := AuthorizeWALTruncation(t.Context(), &replayRetentionCatalogStub{}, "p", 7, cp)
				if failure == "none" {
					require.NoError(t, err)
					require.True(t, bound.EQ(walimplstest.NewTestMessageID(10)))
				} else {
					require.Error(t, err)
					require.Nil(t, bound)
				}
			})
		})
	}
}
