package service

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

var errReplicateConfigurationSame = errors.New("same replicate configuration")

// NewAssignmentService returns a new assignment service.
func NewAssignmentService() streamingpb.StreamingCoordAssignmentServiceServer {
	assignmentService := &assignmentServiceImpl{
		listenerTotal: metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()),
	}
	registry.RegisterPutReplicateConfigV2AckCallback(assignmentService.putReplicateConfiguration)
	return assignmentService
}

type AssignmentService interface {
	streamingpb.StreamingCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	streamingpb.UnimplementedStreamingCoordAssignmentServiceServer

	listenerTotal prometheus.Gauge
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(server streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer) error {
	s.listenerTotal.Inc()
	defer s.listenerTotal.Dec()

	balancer, err := balance.GetWithContext(server.Context())
	if err != nil {
		return err
	}
	return discover.NewAssignmentDiscoverServer(balancer, server).Execute()
}

// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
func (s *assignmentServiceImpl) UpdateReplicateConfiguration(ctx context.Context, req *streamingpb.UpdateReplicateConfigurationRequest) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	config := req.GetConfiguration()

	log.Ctx(ctx).Info("UpdateReplicateConfiguration received", replicateutil.ConfigLogFields(config)...)

	// check if the configuration is same.
	// so even if current cluster is not primary, we can still make a same return.
	err := s.checkIfReplicateConfigurationSame(ctx, config)
	if err != nil {
		if errors.Is(err, errReplicateConfigurationSame) {
			log.Ctx(ctx).Info("configuration is same, ignored")
			return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
		}
		return nil, err
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return nil, err
	}
	defer broadcaster.Close()

	msg, err := s.validateReplicateConfiguration(ctx, config)
	if err != nil {
		if errors.Is(err, errReplicateConfigurationSame) {
			log.Ctx(ctx).Info("configuration is same after cluster resource key is acquired, ignored")
			return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
		}
		return nil, err
	}
	_, err = broadcaster.Broadcast(ctx, msg)
	if err != nil {
		return nil, err
	}
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

// checkIfReplicateConfigurationSame checks if the replicate configuration is the same as the latest assignment.
// return true if the replicate configuration is the same as the latest assignment.
func (s *assignmentServiceImpl) checkIfReplicateConfigurationSame(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	latestAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return err
	}
	if proto.Equal(config, latestAssignment.ReplicateConfiguration) {
		return errReplicateConfigurationSame
	}
	return nil
}

// validateReplicateConfiguration validates the replicate configuration.
func (s *assignmentServiceImpl) validateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) (message.BroadcastMutableMessage, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	// get all pchannels
	latestAssignment, err := balancer.GetLatestChannelAssignment()
	if err != nil {
		return nil, err
	}

	// double check if the configuration is same after resource key is acquired.
	if proto.Equal(config, latestAssignment.ReplicateConfiguration) {
		return nil, errReplicateConfigurationSame
	}

	pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, channel *channel.PChannelMeta) string {
		return channel.Name()
	})

	// validate the configuration itself
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	validator := replicateutil.NewReplicateConfigValidator(config, currentClusterID, pchannels)
	if err := validator.Validate(); err != nil {
		log.Ctx(ctx).Warn("UpdateReplicateConfiguration fail", zap.Error(err))
		return nil, err
	}

	// TODO: validate the incoming configuration is compatible with the current config.
	if _, err := replicateutil.NewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), config); err != nil {
		return nil, err
	}
	b := message.NewPutReplicateConfigMessageBuilderV2().
		WithHeader(&message.PutReplicateConfigMessageHeader{
			ReplicateConfiguration: config,
		}).
		WithBody(&message.PutReplicateConfigMessageBody{}).
		WithBroadcast(pchannels).
		MustBuildBroadcast()
	return b, nil
}

// putReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) putReplicateConfiguration(ctx context.Context, result message.BroadcastResultPutReplicateConfigMessageV2) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	return balancer.UpdateReplicateConfiguration(ctx, result)
}

// UpdateWALBalancePolicy is used to update the WAL balance policy.
func (s *assignmentServiceImpl) UpdateWALBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return balancer.UpdateBalancePolicy(ctx, req)
}
