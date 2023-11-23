package resolver

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/internal/util/syncutil"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

var _ Resolver = (*resolverWithDiscoverer)(nil)

func newResolverWithDiscoverer(scheme string, d discoverer.Discoverer, retryInterval time.Duration) *resolverWithDiscoverer {
	ctx, cancel := context.WithCancel(context.Background())
	return &resolverWithDiscoverer{
		ctx:             ctx,
		cancel:          cancel,
		logger:          log.With(zap.String("scheme", scheme)),
		registerCh:      make(chan *watchBasedGRPCResolver),
		discoverer:      d,
		retryInterval:   retryInterval,
		latestStateCond: syncutil.NewContextCond(&sync.Mutex{}),
		latestState:     d.NewVersionedState(),
	}
}

// resolverWithDiscoverer is the resolver for bkproxy service.
type resolverWithDiscoverer struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *log.MLogger

	registerCh chan *watchBasedGRPCResolver

	discoverer    discoverer.Discoverer // the discoverer method for the bkproxy service
	retryInterval time.Duration

	latestStateCond *syncutil.ContextCond
	latestState     discoverer.VersionedState
}

// GetLatestState returns the latest state of the resolver.
func (r *resolverWithDiscoverer) GetLatestState() VersionedState {
	r.latestStateCond.L.Lock()
	state := r.latestState
	r.latestStateCond.L.Unlock()
	return state
}

// Watch watch the state change of the resolver.
func (r *resolverWithDiscoverer) Watch(ctx context.Context, cb func(VersionedState)) {
	state := r.GetLatestState()
	cb(state)
	version := state.Version
	for {
		if err := r.watchStateChange(ctx, version); err != nil {
			return
		}
		state := r.GetLatestState()
		cb(state)
		version = state.Version
	}
}

// watchStateChange block util the state is changed.
func (r *resolverWithDiscoverer) watchStateChange(ctx context.Context, version util.Version) error {
	r.latestStateCond.L.Lock()
	for version.Equal(r.latestState.Version) {
		if err := r.latestStateCond.Wait(ctx); err != nil {
			return err
		}
	}
	r.latestStateCond.L.Unlock()
	return nil
}

// registerNewWatcher registers a new grpc resolver.
// registerNewWatcher should always be call before Close.
func (r *resolverWithDiscoverer) registerNewWatcher(grpcResolver *watchBasedGRPCResolver) {
	r.registerCh <- grpcResolver
}

// Close closes the resolver.
func (r *resolverWithDiscoverer) Close() {
	// Cancel underlying task and close the discovery service.
	r.cancel()
	if err := r.discoverer.Close(); err != nil {
		r.logger.Warn("fail to close discoverer", zap.Error(err))
	}
}

func (r *resolverWithDiscoverer) doDiscoverOnBackground() {
	go r.doDiscover()
}

// doDiscover do the discovery on background.
func (r *resolverWithDiscoverer) doDiscover() {
	defer r.logger.Info("resolver stopped")
	grpcResolvers := make(map[*watchBasedGRPCResolver]struct{}, 0)

	for {
		if r.ctx.Err() != nil {
			// resolver stopped.
			return
		}

		discovers := make(chan discoverer.VersionedState)
		// Stop Discover actively if error happened.
		r.logger.Info("try to start service discover task on background...")
		errCh := r.discoverer.AsyncDiscover(r.ctx, discovers)
		r.logger.Info("service discover task started, listening...")
	L:
		for {
			select {
			case watcher := <-r.registerCh:
				// New grpc resolver registered.
				// Trigger the latest state to the new grpc resolver.
				if err := watcher.Update(r.GetLatestState()); err != nil {
					r.logger.Info("resolver is closed, ignore the new grpc resolver", zap.Error(err))
				} else {
					grpcResolvers[watcher] = struct{}{}
				}
			case state := <-discovers:
				latestState := r.GetLatestState()
				if !state.Version.GT(latestState.Version) {
					// Ignore the old version.
					r.logger.Info("service discover update, ignore old version", zap.Any("state", state))
					continue
				}
				// Update all grpc resolver.
				r.logger.Info("service discover update, update resolver", zap.Any("state", state), zap.Int("resolver_count", len(grpcResolvers)))
				for watcher := range grpcResolvers {
					// update operation do not block.
					if err := watcher.Update(state); err != nil {
						r.logger.Info("resolver is closed, unregister the resolver", zap.Error(err))
						delete(grpcResolvers, watcher)
					}
				}
				r.logger.Info("update resolver done")
				// Update the latest state and notify all waiter.
				r.latestStateCond.LockAndBroadcast()
				r.latestState = state
				r.latestStateCond.L.Unlock()
			case err := <-errCh:
				r.logger.Warn("service discover break down", zap.Error(err))
				break L
			}
		}
		time.Sleep(r.retryInterval)
	}
}
