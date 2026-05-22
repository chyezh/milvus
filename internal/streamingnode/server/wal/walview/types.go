package walview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// LoadConfigListener receives WAL-captured load-config events from RecoveryStorage.
type LoadConfigListener interface {
	OnAlterLoadConfig(VChannelWALView) VChannelLiveObserver
	OnDropLoadConfig(DropLoadConfigEvent)
}

// DropLoadConfigEvent identifies a dropped load-config anchor.
type DropLoadConfigEvent struct {
	PChannel     string
	VChannel     string
	CollectionID int64
}

// VChannelWALView is the RecoveryStorage-owned WAL input view for one loaded vchannel.
type VChannelWALView struct {
	PChannel     string
	VChannel     string
	CollectionID int64

	BaseGrowingTimeTick   uint64
	BaseTransformTimeTick uint64

	LoadConfig *streamingpb.VChannelLoadConfig
	Schema     *schemapb.CollectionSchema

	SegmentSnapshot VisibleSegmentSnapshot
	DeleteReplay    transformlog.Scanner
}

// VisibleSegmentSnapshot is the historical growing-side insert state captured at a WAL observe point.
type VisibleSegmentSnapshot struct {
	CollectionID        int64
	VChannel            string
	DataVersion         qviews.DataVersion
	BaseGrowingTimeTick uint64
	Segments            []VisibleSegment
}

// VisibleSegment is a query-visible segment and its shallow-copied data handles.
type VisibleSegment struct {
	SegmentID   int64
	PartitionID int64

	Schema *schemapb.CollectionSchema

	Assignment          *streamingpb.SegmentAssignmentMeta
	SealedAtDataVersion *viewpb.DataVersion

	Data SegmentSnapshotData
}

// SegmentSnapshotData contains persisted and in-memory insert data for a visible segment.
type SegmentSnapshotData struct {
	PersistedStorage *streamingpb.L1SegmentPersistedStorage
	InsertMessages   []message.ImmutableMessage
}

// VChannelLiveObserver observes raw WAL messages after a VChannelWALView capture.
type VChannelLiveObserver interface {
	ObserveMessage(ctx context.Context, msg message.ImmutableMessage) bool
	Close()
}
