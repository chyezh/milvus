package discoverer

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"google.golang.org/grpc/resolver"
)

type AssignmentDiscoverWatcher interface {
	// AssignmentDiscover watches the assignment discovery.
	// The watcher will be closed when context cancel or error occurs.
	AssignmentDiscover(ctx context.Context, watcher chan<- *logpb.AssignmentDiscoverResponse) error
}

// channelAssignmentDiscoverer is the discoverer for channel assignment.
type channelAssignmentDiscoverer struct {
	lifetime          lifetime.Lifetime[lifetime.State]
	assignmentWatcher AssignmentDiscoverWatcher
	// last discovered state and last version discovery.
	lastDiscovery *logpb.AssignmentDiscoverResponse
}

func (d *channelAssignmentDiscoverer) NewVersionedState() VersionedState {
	return VersionedState{
		Version: util.NewVersionInt64Pair(),
		State:   resolver.State{},
	}
}

// channelAssignmentDiscoverer implements the resolver.Discoverer interface.
func (d *channelAssignmentDiscoverer) Discover(ctx context.Context, ch chan<- VersionedState) error {
	if d.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel assignment discoverer is closed")
	}

	watcher := make(chan *logpb.AssignmentDiscoverResponse, 1)
	err := d.assignmentWatcher.AssignmentDiscover(ctx, watcher)
	if err != nil {
		return errors.Wrap(err, "at creating discoverer")
	}
	for {
		// Always sent the current state to the channel first.
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "cancel the discovery")
		case ch <- d.parseState():
		}

		// wait for the next discovery.
		resp, ok := <-watcher
		if !ok {
			return errors.New("watch fail, at receiving the next discovery")
		}
		d.lastDiscovery = resp
	}
}

// Close closes the discoverer.
func (d *channelAssignmentDiscoverer) Close() error {
	d.lifetime.SetState(lifetime.Stopped)
	d.lifetime.Wait()
	return nil
}

// parseState parses the addresses from the discovery response.
// Always perform a copy here.
func (d *channelAssignmentDiscoverer) parseState() VersionedState {
	if d.lastDiscovery == nil {
		return d.NewVersionedState()
	}

	addrs := make([]resolver.Address, 0, len(d.lastDiscovery.Addresses))
	for _, addr := range d.lastDiscovery.Addresses {
		attr := new(attributes.Attributes)
		channels := make(map[string]logpb.PChannelInfo, len(addr.Channels))
		for _, ch := range addr.Channels {
			channels[ch.Name] = *ch
		}
		attr = attributes.WithChannelAssignmentInfo(attr, addr)
		addrs = append(addrs, resolver.Address{
			Addr:               addr.Address,
			BalancerAttributes: attr,
		})
	}
	return VersionedState{
		Version: &util.VersionInt64Pair{
			Global: d.lastDiscovery.Version.Global,
			Local:  d.lastDiscovery.Version.Local,
		},
		State: resolver.State{
			Addresses: addrs,
		},
	}
}
