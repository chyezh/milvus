package manager

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

const dirtyThreshold = 30 * 1024 * 1024 // 30MB

var (
	ErrTimeTickTooOld = errors.New("time tick is too old")
	ErrNotEnoughSpace = stats.ErrNotEnoughSpace
	ErrTooLargeInsert = stats.ErrTooLargeInsert
)

// newSegmentAllocManagerFromProto creates a new segment assignment meta from proto.
// if the segment is growing, the stat should be registered to stats manager,
// so it will be returned.
func newSegmentAllocManagerFromProto(
	pchannel types.PChannelInfo,
	inner *streamingpb.SegmentAssignmentMeta,
	metrics *metricsutil.SegmentAssignMetrics,
) (m *segmentAllocManager, growingStat *stats.SegmentStats) {
	stat := utils.NewSegmentStatFromProto(inner.Stat)
	// Growing segment's stat should be registered to stats manager.
	// Async sealed policy will use it.
	growingStat = stat
	return &segmentAllocManager{
		pchannel:   pchannel,
		inner:      inner,
		ackSem:     atomic.NewInt32(0),
		txnSem:     atomic.NewInt32(0),
		dirtyBytes: 0,
		metrics:    metrics,
	}, growingStat
}

// newSegmentAllocManager creates a new segment assignment meta.
func newSegmentAllocManager(
	pchannel types.PChannelInfo,
	msg message.ImmutableCreateSegmentMessageV2,
	metrics *metricsutil.SegmentAssignMetrics,
) *segmentAllocManager {
	createSegmentBody := msg.MustBody()
	segment := createSegmentBody.Segments[0]

	now := int64(tsoutil.PhysicalTime(msg.TimeTick()).Nanosecond())
	return &segmentAllocManager{
		pchannel: pchannel,
		inner: &streamingpb.SegmentAssignmentMeta{
			CollectionId: createSegmentBody.CollectionId,
			PartitionId:  segment.PartitionId,
			SegmentId:    segment.SegmentId,
			Vchannel:     msg.VChannel(),
			Stat: &streamingpb.SegmentAssignmentStat{
				MaxBinarySize:                    uint64(segment.MaxSegmentSizeBytes),
				CreateTimestampNanoseconds:       now,
				LastModifiedTimestampNanoseconds: now,
				CreateSegmentTimeTick:            msg.TimeTick(),
			},
			StorageVersion: segment.StorageVersion,
		},
		ackSem:     atomic.NewInt32(0),
		dirtyBytes: 0,
		txnSem:     atomic.NewInt32(0),
		metrics:    metrics,
	}
}

// segmentAllocManager is the meta of segment assignment,
// only used to recover the assignment status on streaming node.
// !!! Not Concurrent Safe
// The state transfer is as follows:
// Pending -> Growing -> Sealed -> Flushed.
//
// The recovery process is as follows:
//
// | State | DataCoord View | Writable | WAL Status | Recovery |
// |-- | -- | -- | -- | -- |
// | Pending | Not exist | No | Not exist | 1. Check datacoord if exist; transfer into growing if exist. |
// | Growing | Exist | Yes | Insert Message Exist; Seal Message Not Exist | nothing |
// | Sealed  | Exist | No | Insert Message Exist; Seal Message Maybe Exist | Resend a Seal Message and transfer into Flushed. |
// | Flushed | Exist | No | Insert Message Exist; Seal Message Exist | Already physically deleted, nothing to do |
type segmentAllocManager struct {
	pchannel   types.PChannelInfo
	inner      *streamingpb.SegmentAssignmentMeta
	ackSem     *atomic.Int32 // the ackSem is increased when segment allocRows, decreased when the segment is acked.
	dirtyBytes uint64        // records the dirty bytes that didn't persist.
	txnSem     *atomic.Int32 // the runnint txn count of the segment.
	metrics    *metricsutil.SegmentAssignMetrics
	sealPolicy policy.SealPolicy
}

// WithSealPolicy sets the seal policy of the segment assignment meta.
func (s *segmentAllocManager) WithSealPolicy(policy policy.SealPolicy) *segmentAllocManager {
	s.sealPolicy = policy
	return s
}

// SealPolicy returns the seal policy of the segment assignment meta.
func (s *segmentAllocManager) SealPolicy() policy.SealPolicy {
	return s.sealPolicy
}

// GetCollectionID returns the collection id of the segment assignment meta.
func (s *segmentAllocManager) GetCollectionID() int64 {
	return s.inner.GetCollectionId()
}

// GetPartitionID returns the partition id of the segment assignment meta.
func (s *segmentAllocManager) GetPartitionID() int64 {
	return s.inner.GetPartitionId()
}

// GetSegmentID returns the segment id of the segment assignment meta.
func (s *segmentAllocManager) GetSegmentID() int64 {
	return s.inner.GetSegmentId()
}

func (s *segmentAllocManager) GetStorageVersion() int64 {
	return s.inner.GetStorageVersion()
}

// GetVChannel returns the vchannel of the segment assignment meta.
func (s *segmentAllocManager) GetVChannel() string {
	return s.inner.GetVchannel()
}

// Stat get the stat of segments.
// Pending segment will return nil.
// Growing segment will return a snapshot.
// Sealed segment will return the final.
func (s *segmentAllocManager) GetStat() *stats.SegmentStats {
	return resource.Resource().SegmentAssignStatsManager().GetStatsOfSegment(s.GetSegmentID())
}

// AckSem returns the ack sem.
func (s *segmentAllocManager) AckSem() int32 {
	return s.ackSem.Load()
}

// TxnSem returns the txn sem.
func (s *segmentAllocManager) TxnSem() int32 {
	return s.txnSem.Load()
}

// IsDrityEnough returns true if the segment is dirty enough to persist.
func (s *segmentAllocManager) IsDirtyEnough() bool {
	return s.dirtyBytes > dirtyThreshold
}

// AllocRows ask for rows from current segment.
// Only growing and not fenced segment can alloc rows.
func (s *segmentAllocManager) AllocRows(ctx context.Context, req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	if req.TimeTick <= s.inner.GetStat().CreateSegmentTimeTick {
		// The incoming insert request's timetick is less than the segment's create time tick,
		// return ErrTimeTickTooOld and reallocate new timetick.
		return nil, ErrTimeTickTooOld
	}

	err := resource.Resource().SegmentAssignStatsManager().AllocRows(s.GetSegmentID(), req.InsertMetrics)
	if err != nil {
		return nil, err
	}
	s.dirtyBytes += req.InsertMetrics.BinarySize
	s.ackSem.Inc()

	// register the txn session cleanup to the segment.
	if req.TxnSession != nil {
		s.txnSem.Inc()
		req.TxnSession.RegisterCleanup(func() { s.txnSem.Dec() }, req.TimeTick)
	}

	// persist stats if too dirty.
	return &AssignSegmentResult{
		SegmentID:   s.GetSegmentID(),
		Acknowledge: s.ackSem,
	}, nil
}

// Snapshot returns the snapshot of the segment assignment meta.
func (s *segmentAllocManager) Snapshot() *streamingpb.SegmentAssignmentMeta {
	copied := proto.Clone(s.inner).(*streamingpb.SegmentAssignmentMeta)
	copied.Stat = utils.NewProtoFromSegmentStat(s.GetStat())
	return copied
}
