package qviews

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/viewpb"
)

// RecoveryStorage is the interface to manage the recovery info for query views.
type RecoveryStorage interface {
	// SwapPreparing swaps the old preparing view with new preparing view.
	SwapPreparing(ctx context.Context, old *viewpb.QueryViewOfShard, new *viewpb.QueryViewOfShard)

	// UpNewPerparingView remove the preparing view and make it as a up view.
	UpNewPreparingView(ctx context.Context, newUp *viewpb.QueryViewOfShard)

	// Save saves the recovery infos into underlying persist storage.
	// The operation should be executed atomically.
	Save(ctx context.Context, saved *viewpb.QueryViewOfShard)

	// Delete removes the view of the shard.
	Delete(ctx context.Context, deleted *viewpb.QueryViewOfShard)

	// All The write opeartion on recovery will generate a event after the event is done.
	Event() <-chan RecoveryEvent

	// Close closes the recovery.
	Close()
}

// RecoveryEvent is the interface for recovery events.
type RecoveryEvent interface {
	GetShardID() ShardID

	isRecoveryEvent()
}

// RecoveryEventSwapPreparing is the event for swapping the preparing view.
type RecoveryEventSwapPreparing struct {
	ShardID
	OldVersion QueryViewVersion
	NewVersion QueryViewVersion
}

func (RecoveryEventSwapPreparing) isRecoveryEvent() {
}

// RecoveryEventUpNewPreparingView is the event for updating the new preparing view.
type RecoveryEventUpNewPreparingView struct {
	ShardID
	Version QueryViewVersion
}

func (RecoveryEventUpNewPreparingView) isRecoveryEvent() {
}

// RecoveryEventSave is the event for saving the view.
type RecoveryEventSave struct {
	ShardID
	Version QueryViewVersion
}

func (RecoveryEventSave) isRecoveryEvent() {
}

// RecoveryEventDelete is the event for deleted the view.
type RecoveryEventDelete struct {
	ShardID
	Version QueryViewVersion
}

func (RecoveryEventDelete) isRecoveryEvent() {
}
