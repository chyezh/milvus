package qnview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// AcquireSegments describes the segment requirements of a query view.
type AcquireSegments struct {
	// Key identifies the query view that holds the segment references.
	Key qviews.QueryViewKey

	// SegmentIDs lists all segments this view requires.
	SegmentIDs []int64

	// Settings contains the load configuration (fields, partitions, etc.).
	Settings *viewpb.QueryViewSettings

	// OnReady is called when segments become available.
	// readySegments maps partitionID → newly loaded segment IDs.
	// May be called multiple times for incremental progress.
	// Must NOT be called synchronously during Acquire.
	OnReady func(readySegments map[int64][]int64)

	// OnUnrecoverable is called when a fatal error prevents segment loading.
	// Must NOT be called synchronously during Acquire.
	OnUnrecoverable func()
}

// ReleaseSegments describes a segment release request for a query view.
type ReleaseSegments struct {
	// Key identifies the query view whose segment references are being released.
	Key qviews.QueryViewKey

	// OnDropped is called when the release operation completes (segments actually
	// unloaded or ref counts decremented).
	// Must NOT be called synchronously during Release.
	OnDropped func()
}

// SegmentManager manages sealed segment lifecycle on a QueryNode.
// It handles loading, reference counting, and unloading of segments
// shared across multiple query views.
type SegmentManager interface {
	// Acquire creates or updates a segment reference.
	// First call for a key: increments ref counts, starts loading with given settings.
	// Subsequent calls with same key: diffs settings and reconfigures if changed.
	Acquire(req AcquireSegments)

	// Release decrements reference counts for all segments held by this view.
	// Segments whose count reaches zero will be unloaded.
	Release(req ReleaseSegments)
}
