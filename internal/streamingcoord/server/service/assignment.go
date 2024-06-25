package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/collector"
	"github.com/milvus-io/milvus/internal/util/streamingutil/layout"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

// NewAssignmentService returns a new assignment service.
func NewAssignmentService(
	balancer balancer.Balancer,
	collector *collector.Collector,
) streamingpb.StreamingCoordAssignmentServiceServer {
	return &assignmentServiceImpl{
		balancer:  balancer,
		collector: collector,
	}
}

type AssignmentService interface {
	streamingpb.StreamingCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	balancer  balancer.Balancer
	collector *collector.Collector
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(request *streamingpb.AssignmentDiscoverRequest, streamServer streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer) error {
	return s.watch(streamServer.Context(), func(state *streamingpb.AssignmentDiscoverResponse) error {
		return streamServer.Send(state)
	})
}

// ReportStreamingError reports an error to the assignment manager.
func (s *assignmentServiceImpl) ReportStreamingError(ctx context.Context, request *streamingpb.ReportStreamingErrorRequest) (*streamingpb.ReportStreamingErrorResponse, error) {
	// Trigger a collector immediately.
	s.collector.Trigger()
	return &streamingpb.ReportStreamingErrorResponse{}, nil
}

// watch watches the state of all log nodes.
// The callback will be called when the state changes.
// Watch exits when the context is done or callback returns error.
func (s *assignmentServiceImpl) watch(ctx context.Context, cb func(state *streamingpb.AssignmentDiscoverResponse) error) error {
	metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	defer metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()).Dec()

	return s.balancer.WatchBalanceResult(ctx, func(v *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error {
		state := &streamingpb.AssignmentDiscoverResponse{
			Version: &streamingpb.VersionPair{
				Global: v.Global,
				Local:  v.Local,
			},
			Addresses: make([]*streamingpb.StreamingNodeAssignment, 0, len(nodeStatus)),
		}
		for _, node := range nodeStatus {
			channels := make([]*streamingpb.PChannelInfo, 0, len(node.Channels))
			for _, ch := range node.Channels {
				channels = append(channels, ch)
			}
			state.Addresses = append(state.Addresses, &streamingpb.StreamingNodeAssignment{
				ServerId: node.ServerID,
				Address:  node.Address,
				Channels: channels,
			})
		}
		return cb(state)
	})
}
