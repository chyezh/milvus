package discoverer

import (
	"context"
	"path"

	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

// NewSessionDiscoverer returns a new Discoverer for the milvus session registration.
func NewSessionDiscoverer(etcdCli *clientv3.Client, role string, minimumVersion string) Discoverer {
	prefix := path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, role)
	return &asyncDiscover{
		&sessionDiscoverer{
			lifetime:     lifetime.NewLifetime(lifetime.Working),
			etcdCli:      etcdCli,
			prefix:       prefix,
			versionRange: semver.MustParseRange(">=" + minimumVersion),
			logger:       log.With(zap.String("role", role), zap.String("expectedVersion", minimumVersion)),
			revision:     0,
			peerSessions: make(map[string]*sessionutil.Session),
		},
	}
}

// NewChannelAssignmentDiscoverer returns a new Discoverer for the channel assignment registration.
func NewChannelAssignmentDiscoverer(logCoordManager AssignmentDiscoverWatcher) Discoverer {
	return &asyncDiscover{
		&channelAssignmentDiscoverer{
			lifetime:          lifetime.NewLifetime(lifetime.Working),
			assignmentWatcher: logCoordManager,
			lastDiscovery:     nil,
		},
	}
}

// Discoverer is the interface for the service discovery.
type Discoverer interface {
	DiscoverAPI

	// AsyncDiscover is a non-blocking version of Discover.
	AsyncDiscover(ctx context.Context, ch chan<- VersionedState) <-chan error
}

type DiscoverAPI interface {
	NewVersionedState() VersionedState

	// Discover watches the service discovery on these goroutine.
	// Block until the discovery is canceled or break down.
	// Once the discovery is done, it will return an error with context.Canceled.
	// It will return an error if the discovery is break down.
	Discover(ctx context.Context, ch chan<- VersionedState) error

	// Close closes the discoverer.
	Close() error
}

// AsyncDiscover is a non-blocking version of Discover.
type asyncDiscover struct {
	DiscoverAPI
}

// AsyncDiscover is a non-blocking version of Discover.
func (a *asyncDiscover) AsyncDiscover(ctx context.Context, ch chan<- VersionedState) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- a.Discover(ctx, ch)
	}()
	return errCh
}

// VersionedState is the state with version.
type VersionedState struct {
	Version util.Version
	State   resolver.State
}

// Sessions returns the sessions in the state.
// Should only be called when using session discoverer.
func (s *VersionedState) Sessions() map[int64]*sessionutil.Session {
	sessions := make(map[int64]*sessionutil.Session)
	for _, v := range s.State.Addresses {
		session := attributes.GetSessionFromAttributes(v.BalancerAttributes)
		if session == nil {
			log.Error("no session found in resolver state, skip it", zap.String("address", v.Addr))
			continue
		}
		sessions[session.ServerID] = session
	}
	return sessions
}

func (s *VersionedState) ChannelAssignmentInfo() map[int64]*logpb.LogNodeAssignment {
	assignments := make(map[int64]*logpb.LogNodeAssignment)
	for _, v := range s.State.Addresses {
		assignment := attributes.GetChannelAssignmentInfoFromAttributes(v.BalancerAttributes)
		if assignment == nil {
			log.Error("no assignment found in resolver state, skip it", zap.String("address", v.Addr))
			continue
		}
		assignments[assignment.ServerID] = assignment
	}
	return assignments
}
