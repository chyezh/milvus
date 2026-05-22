package snview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// AcquireResource describes a resource acquisition request for a new Preparing view.
type AcquireResource struct {
	// Key identifies the query view.
	Key qviews.QueryViewKey

	// Meta carries the complete view metadata required by resource preparation.
	Meta *viewpb.QueryViewMeta

	// OnReady is called when resource preparation completes successfully.
	// Must NOT be called synchronously during Acquire.
	OnReady func()

	// OnUnrecoverable is called when a fatal error prevents resource setup.
	// Must NOT be called synchronously during Acquire.
	OnUnrecoverable func()
}

// RecoverResource describes a resource recovery request for a persisted view
// after SN crash recovery.
type RecoverResource struct {
	// Key identifies the query view.
	Key qviews.QueryViewKey

	// Meta carries the complete view metadata required by resource recovery.
	Meta *viewpb.QueryViewMeta

	// OnRecoveringDone is called when WAL catch-up completes successfully.
	// Must NOT be called synchronously during Recover.
	OnRecoveringDone func()

	// OnUnrecoverable is called when a fatal error prevents recovery.
	// Must NOT be called synchronously during Recover.
	OnUnrecoverable func()
}

// ReleaseResource describes a resource release request when a query view
// is being dropped.
type ReleaseResource struct {
	// Key identifies the query view whose resources are being released.
	Key qviews.QueryViewKey

	// OnDropped is called when the view-level release operation completes.
	// Physical SN resources are reclaimed by DataVersion watermark updates,
	// not by per-view reference counting.
	// Must NOT be called synchronously during Release.
	OnDropped func()
}

// UpdateMinDataVersionResource publishes the minimum DataVersion still required
// by a vchannel's live SN query views.
type UpdateMinDataVersionResource struct {
	CollectionID   int64
	VChannel       string
	MinDataVersion qviews.DataVersion
}

// ReleaseLoadResource releases all locally prepared resources for a vchannel
// when no local Up or recovering query view still retains a DataVersion.
type ReleaseLoadResource struct {
	CollectionID int64
	VChannel     string
}

// StreamingNodeResourceManager manages streaming resources on a StreamingNode.
// Resources include growing segments, BM25 IDF statistics, and other
// shard-level query state required to serve a query view.
//
// StreamingNode uses DataVersion watermark-based reclamation instead of
// QueryView reference counting. This is SN-specific: AlterLoadConfig prepares
// the latest DataVersion before QueryView exists, and later QueryView sync gives
// the SN enough local state to publish the minimum DataVersion that must remain
// in memory for each vchannel.
//
// # Liveness Contracts
//
// Implementations MUST guarantee the following callback obligations.
// Violating these contracts causes the corresponding query views to
// stall without ever producing a response to the Coordinator.
//
//   - Acquire: for every Acquire call, the implementation MUST eventually
//     invoke exactly one of OnReady or OnUnrecoverable.
//     Failure to do so leaves the view stuck in Preparing with no report.
//
//   - Recover: for every Recover call, the implementation MUST eventually
//     invoke exactly one of OnRecoveringDone or OnUnrecoverable.
//     Failure to do so leaves the view stuck in UpRecovering with no report.
//
//   - Release: for every Release call, the implementation MUST eventually
//     invoke OnDropped exactly once.
//     Failure to do so leaves the view stuck in Dropping with no report.
//
// All callbacks MUST be invoked asynchronously (not during the Acquire /
// Recover / Release call itself) to avoid deadlocking the caller's mutex.
type StreamingNodeResourceManager interface {
	// Acquire starts resource preparation for a new query view.
	Acquire(req AcquireResource)

	// Recover starts WAL catch-up for a recovered query view.
	Recover(req RecoverResource)

	// Release releases resources held by a query view being dropped.
	Release(req ReleaseResource)

	// UpdateMinDataVersion evicts prepared DataVersion runtimes lower than the
	// minimum version still required by live SN query views on the vchannel.
	UpdateMinDataVersion(req UpdateMinDataVersionResource)

	// ReleaseLoad releases prepared runtimes for a vchannel when it has no
	// locally retained QueryView DataVersions.
	ReleaseLoad(req ReleaseLoadResource)
}
