package service

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/grpcutil/resolver"
	streamingdiscoverer "github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

const (
	// ChannelAssignmentResolverScheme is the scheme for channel assignment resolver.
	// targets: channel-assignment://external-grpc-client
	ChannelAssignmentResolverScheme = "channel-assignment"
)

// NewChannelAssignmentBuilder creates a new resolver builder for channel assignment.
func NewChannelAssignmentBuilder(w types.AssignmentDiscoverWatcher) resolver.Builder {
	d := streamingdiscoverer.NewChannelAssignmentDiscoverer(w)
	b := resolver.NewBuilder(ChannelAssignmentResolverScheme, d,
		log.With(log.FieldComponent("grpc-resolver"), zap.String("scheme", ChannelAssignmentResolverScheme)))
	return b
}
