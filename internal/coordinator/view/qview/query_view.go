package qview

//type QueryViewEvent struct{}
//
//type BalanceInfo struct{}
//
//type QueryViewManagerAtCoord interface {
//	// ReleaseReplica will release the replica of the query view manager.
//	// The Release opeartion will block until the all the queryview of this replica down.
//	ReleaseReplica(ctx context.Context, replicaID int64) error
//
//	// Apply will apply the query view into underlying state machine.
//	// QueryViewManager will automatically sync the new query view into related query nodes.
//	// Return error if the manager can not handle the query view right now.
//	Apply(qv QueryViewOfShardAtCoord) error
//
//	// WatchEvent returns a channel that will send the query view critical event for balancer.
//	// Such as: some view is up, some view is unavailable, some view is unrecoverable.
//	// some partition is ready, some field is ready and so on.
//	WatchEvent() <-chan QueryViewEvent
//
//	// GetLatestBalanceInfo returns the latest balance info of the query view manager.
//	// The QueryNode workloads (mem, disk), segment distribution like old GetDistribution rpc.
//	// But it's just merged all the underlying queryviews, not by rpcs.
//	GetLatestBalanceInfo() (BalanceInfo, error)
//}
//
//type QueryViewManagerAtStreamingNode interface {
//	// Block until latest query view is available and return.
//	// Or using a largest version query view at Up state to use.
//	// For query plan.
//	SelectQueryView(ctx context.Context, collectionID int64, vchannel string) (QueryViewOfShardAtStreamingNode, error)
//
//	// GetQueryView returns the query view of the replica.
//	// Report error if the query view is not available.
//	GetQueryView(replicaID int64, v QueryViewVersion) (QueryViewOfShardAtStreamingNode, error)
//}
//
//// QueryViewManagerAtQueryNode is the interface that manages the query view at query node.
//type QueryViewManagerAtQueryNode interface {
//	// GetQueryView returns the query view of the replica.
//	// Report error if the query view is not available.
//	GetQueryView(replicaID int64, v QueryViewVersion) (QueryViewOfShardAtQueryNode, error)
//}
//
//type RequestShardRequest struct {
//	CollectionID int64
//	VChannel     string
//	DataVersion  int64
//	Loaded       bool // if shard is loaded, the shard should transform into growing segment mode, otherwise transform into writebuffer mode.
//}
//
//// SegmentManagerAtStreamingNode is the interface that manages the segment at streaming node.
//// The Api is a decalration method for setup expected state of shard.
//type SegmentManagerAtStreamingNode interface {
//	// RequestShard change the state of the shard to loaded or writebuffer.
//	// Converting all the on-flushing segment into loaded if loaded.
//	// return error when first time transform into loaded mode
//	// if current flusher's data version is greater than the given one,
//	// Create background task continously load the segment from stream
//	// and automatically sealed the segment with data version X if the segment is flushed.
//	// When first time transform into loaded mode,
//	// the function should be blocked until the growing segment start to consume message from stream.
//	RequestShard(ctx context.Context, request RequestShardRequest) error
//}
//
//type SubscribeDeleteStreamRequest struct {
//	VChannel string
//	Segments SubscribeDeleteStreamSubRequest
//}
//
//type SubscribeDeleteStreamSubRequest struct {
//	SegmentID     int64
//	Position *msgpb.MsgPosition
//	ApplyCallback func(DeleteRequest...) error // Should be fast fail, if the segment is not ready to delete, will be retried with backoff.
//}
//
//// DeleteStreamManager is the interface that manages the delete stream at streaming node.
//// will be called at querynode segment.
//type DeleteStreamManager interface {
//	SubscribeDeleteStream(ctx context.Context, vchannel string) error
//}
//
