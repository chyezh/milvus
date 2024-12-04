package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// RecoveryStorage is the interface to manage the recovery info for query views.
type RecoveryStorage interface {
	// SwapPreparing swaps the old preparing view with new preparing view.
	SwapPreparing(ctx context.Context, shardID qviews.ShardID, old *viewpb.QueryViewOfShard, new *viewpb.QueryViewOfShard)

	// UpNewPerparingView remove the preparing view and make it as a up view.
	UpNewPreparingView(ctx context.Context, newUp *viewpb.QueryViewOfShard)

	// Save saves the recovery infos into underlying persist storage.
	// The operation should be executed atomically.
	Save(ctx context.Context, saved *viewpb.QueryViewOfShard)

	// Delete removes the view of the shard.
	Delete(ctx context.Context, deleted *viewpb.QueryViewOfShard)

	// All The write opeartion on recovery will generate a event after the event is done.
	Event() <-chan events.RecoveryEvent

	// Close closes the recovery.
	Close()
}
