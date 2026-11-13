package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// WALQueryViewRetentionCatalog extends the coordinator's shared View ownership
// through every local WAL runtime. Retain precedes local preparation; Release
// follows local drain. Disconnect, source replacement and node absence are not
// drain proofs. A retired view never admits another holder.
type WALQueryViewRetentionCatalog interface {
	RetainWALQueryView(context.Context, *viewpb.StreamingWorkNode, *viewpb.QueryViewMeta) error
	ReleaseWALQueryView(context.Context, *viewpb.StreamingWorkNode, *viewpb.QueryViewMeta) error
	// RetireWALQueryView closes admission durably and returns whether all holders
	// have released this View or their entire drained runtime. False is retriable.
	RetireWALQueryView(context.Context, *viewpb.QueryViewMeta) (bool, error)
}
