package balancer

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	versionChecker265 = "<2.6.6-dev"
	versionChecker300 = "<3.0.0-beta"
)

// errVersionWatchDone is an INTERNAL break-signal sentinel used by
// blockUntilRoleGreaterThanVersion to exit the resolver watch callback.
// Never crosses any gRPC boundary.
var errVersionWatchDone = errors.New("done")

// RecoverBalancer recover the balancer working.
func RecoverBalancer(
	ctx context.Context,
	provider ChannelProvider,
) (Balancer, error) {
	policyBuilder := mustGetPolicy(paramtable.Get().StreamingCfg.WALBalancerPolicyName.GetValue())
	policy := policyBuilder.Build()
	logger := resource.Resource().Logger().With(mlog.FieldComponent("balancer"), mlog.String("policy", policyBuilder.Name()))
	policy.SetLogger(logger)

	// Recover the channel view from catalog.
	manager, err := channel.RecoverChannelManager(ctx, provider.GetInitialChannels()...)
	if err != nil {
		return nil, merr.Wrap(err, "fail to recover channel manager")
	}
	manager.SetLogger(resource.Resource().Logger().With(mlog.FieldComponent("channel-manager")))
	ctx, cancel := context.WithCancelCause(context.Background())
	b := &balancerImpl{
		ctx:                    ctx,
		cancel:                 cancel,
		lifetime:               typeutil.NewLifetime(),
		provider:               provider,
		channelMetaManager:     manager,
		policy:                 policy,
		reqCh:                  make(chan *request, 5),
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		freezeNodes:            typeutil.NewConcurrentSet[int64](),
		primaryRGChangedCh:     make(chan struct{}, 1),
	}
	b.SetLogger(logger)
	if err := manager.MarkStreamingHasEnabled(ctx); err != nil {
		return nil, err
	}
	b.watchPrimaryResourceGroupChanges()
	go b.execute()
	return b, nil
}

// balancerImpl is a implementation of Balancer.
type balancerImpl struct {
	mlog.Binder

	ctx                    context.Context
	cancel                 context.CancelCauseFunc
	lifetime               *typeutil.Lifetime
	provider               ChannelProvider
	channelMetaManager     *channel.ChannelManager
	policy                 Policy                                // policy is the balance policy, TODO: should be dynamic in future.
	reqCh                  chan *request                         // reqCh is the request channel, send the operation to background task.
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}] // backgroundTaskNotifier is used to conmunicate with the background task.
	freezeNodes            *typeutil.ConcurrentSet[int64]        // freezeNodes is the nodes that will be frozen, no more wal will be assigned to these nodes and wal will be removed from these nodes.
	primaryRGChangedCh     chan struct{}                         // primaryRGChangedCh wakes the balance loop when streaming.primaryResourceGroup changes.
	primaryRGChangeHandler config.EventHandler
}

func (b *balancerImpl) watchPrimaryResourceGroupChanges() {
	key := paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key
	handler := config.NewHandler("streamingcoord.balancer.primary-rg", func(_ *config.Event) {
		select {
		case b.primaryRGChangedCh <- struct{}{}:
		default:
		}
	})
	b.primaryRGChangeHandler = handler
	paramtable.Get().Watch(key, handler)
}

// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
func (b *balancerImpl) RegisterStreamingEnabledNotifier(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
	b.channelMetaManager.RegisterStreamingEnabledNotifier(notifier)
}

func (b *balancerImpl) GetLatestChannelAssignment() (*WatchChannelAssignmentsCallbackParam, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	return b.channelMetaManager.GetLatestChannelAssignment()
}

// ReplicateRole returns the replicate role of the balancer.
func (b *balancerImpl) ReplicateRole() replicateutil.Role {
	return b.channelMetaManager.ReplicateRole()
}

// GetAllStreamingNodes fetches all streaming node info with resource group (including frozen nodes).
func (b *balancerImpl) GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	return resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(ctx)
}

// GetAvailableStreamingNodes fetches streaming node info with resource group excluding frozen nodes.
func (b *balancerImpl) GetAvailableStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	nodes, err := resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(ctx)
	if err != nil {
		return nil, err
	}

	filtered := make(map[int64]*types.StreamingNodeInfoWithResourceGroup, len(nodes))
	for nodeID, info := range nodes {
		if !b.freezeNodes.Contain(nodeID) {
			filtered[nodeID] = info
		}
	}
	return filtered, nil
}

// ConfirmPrimaryResourceGroupReady returns nil iff every RW pchannel is currently
// assigned to a streaming node that belongs to the configured primary resource group.
// If streaming.primaryResourceGroup is not configured, returns nil.
func (b *balancerImpl) ConfirmPrimaryResourceGroupReady(ctx context.Context) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	primaryRG := paramtable.Get().StreamingCfg.PrimaryResourceGroup.GetValue()
	if primaryRG == "" {
		return nil
	}
	nodes, err := resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(ctx)
	if err != nil {
		return err
	}
	assignment, err := b.channelMetaManager.GetLatestChannelAssignment()
	if err != nil {
		return err
	}
	for _, rel := range assignment.Relations {
		// Only RW pchannels carry WAL writes; RO pchannels (e.g., CDC source) are
		// not constrained by the local primary RG.
		if rel.Channel.AccessMode != types.AccessModeRW {
			continue
		}
		node, ok := nodes[rel.Node.ServerID]
		if !ok {
			return status.NewInner("pchannel %s: assigned node %d not found in streaming nodes",
				rel.Channel.Name, rel.Node.ServerID)
		}
		if node.ResourceGroup != primaryRG {
			return status.NewInner("pchannel %s still on rg=%s, expected primary rg=%s (WAL migration in progress)",
				rel.Channel.Name, node.ResourceGroup, primaryRG)
		}
	}
	return nil
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
func (b *balancerImpl) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	return b.channelMetaManager.GetLatestWALLocated(ctx, pchannel)
}

// WaitUntilWALbasedDDLReady waits until the WAL based DDL is ready.
func (b *balancerImpl) WaitUntilWALbasedDDLReady(ctx context.Context) error {
	if b.channelMetaManager.IsStreamingVersionAtLeast(channel.StreamingVersion265) {
		return nil
	}
	if err := b.channelMetaManager.WaitUntilStreamingEnabled(ctx); err != nil {
		return err
	}
	if err := b.blockUntilRoleGreaterThanVersion(ctx, typeutil.StreamingNodeRole, versionChecker265); err != nil {
		return err
	}
	return b.channelMetaManager.MarkStreamingVersion(ctx, channel.StreamingVersion265)
}

// WaitUntilSchemaDropReady waits until every Proxy can attach schema version
// to insert messages, so schema-drop DDL cannot race with legacy writes.
func (b *balancerImpl) WaitUntilSchemaDropReady(ctx context.Context) error {
	if b.channelMetaManager.IsStreamingVersionAtLeast(channel.StreamingVersion300) {
		return nil
	}
	if err := b.WaitUntilWALbasedDDLReady(ctx); err != nil {
		return err
	}
	if err := b.blockUntilRoleGreaterThanVersion(ctx, typeutil.ProxyRole, versionChecker300); err != nil {
		return err
	}
	return b.channelMetaManager.MarkStreamingVersion(ctx, channel.StreamingVersion300)
}

// WatchChannelAssignments watches the balance result.
func (b *balancerImpl) WatchChannelAssignments(ctx context.Context, cb WatchChannelAssignmentsCallback) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	return b.channelMetaManager.WatchAssignmentResult(ctx, cb)
}

// UpdateReplicateConfiguration updates the replicate configuration.
func (b *balancerImpl) UpdateReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()

	if err := b.channelMetaManager.UpdateReplicateConfiguration(ctx, result); err != nil {
		return err
	}
	return nil
}

// AllocVirtualChannels allocates virtual channels for a collection.
func (b *balancerImpl) AllocVirtualChannels(ctx context.Context, param AllocVChannelParam) ([]string, error) {
	return b.channelMetaManager.AllocVirtualChannels(ctx, param)
}

// UpdateBalancePolicy update the balance policy.
func (b *balancerImpl) UpdateBalancePolicy(ctx context.Context, req *types.UpdateWALBalancePolicyRequest) (*types.UpdateWALBalancePolicyResponse, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	resp, err := b.sendRequestAndWaitFinish(ctx, newOpUpdateBalancePolicy(ctx, req))
	if err != nil {
		return nil, err
	}
	return resp.(*types.UpdateWALBalancePolicyResponse), nil
}

// MarkAsUnavailable mark the pchannels as unavailable.
func (b *balancerImpl) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	_, err := b.sendRequestAndWaitFinish(ctx, newOpMarkAsUnavailable(ctx, pChannels))
	return err
}

// Trigger trigger a re-balance.
func (b *balancerImpl) Trigger(ctx context.Context) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	ctx, cancel := contextutil.MergeContext(ctx, b.ctx)
	defer cancel()
	_, err := b.sendRequestAndWaitFinish(ctx, newOpTrigger(ctx))
	return err
}

// sendRequestAndWaitFinish send a request to the background task and wait for it to finish.
func (b *balancerImpl) sendRequestAndWaitFinish(ctx context.Context, newReq *request) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.reqCh <- newReq:
	}
	resp, err := newReq.future.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return resp.resp, resp.err
}

// Close close the balancer.
func (b *balancerImpl) Close() {
	if b.primaryRGChangeHandler != nil {
		paramtable.Get().Unwatch(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, b.primaryRGChangeHandler)
	}
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	b.provider.Close()
	// cancel all watch opeartion by context.
	b.cancel(ErrBalancerClosed)
	b.lifetime.Wait()

	b.backgroundTaskNotifier.Cancel()
	b.backgroundTaskNotifier.BlockUntilFinish()
}

// execute the balancer.
func (b *balancerImpl) execute() {
	b.Logger().Info(b.backgroundTaskNotifier.Context(), "balancer start to execute")
	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.Logger().Info(b.backgroundTaskNotifier.Context(), "balancer execute finished")
	}()

	if err := b.blockUntilExpectedInitialStreamingNodeNumReached(b.backgroundTaskNotifier.Context()); err != nil {
		b.Logger().Warn(b.backgroundTaskNotifier.Context(), "fail to block until expected initial streaming node number reached", mlog.Err(err))
		return
	}

	balanceTimer := typeutil.NewBackoffTimer(&backoffConfigFetcher{})
	nodeChanged, err := resource.Resource().StreamingNodeManagerClient().WatchNodeChanged(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.Logger().Warn(b.backgroundTaskNotifier.Context(), "fail to watch node changed", mlog.Err(err))
		return
	}
	statsManager, err := channel.StaticPChannelStatsManager.GetWithContext(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.Logger().Warn(b.backgroundTaskNotifier.Context(), "fail to get pchannel stats manager", mlog.Err(err))
		return
	}
	channelChanged := statsManager.WatchAtChannelCountChanged()

	for {
		// Wait for next balance trigger.
		// Maybe trigger by timer or by request.
		nextTimer, nextBalanceInterval := balanceTimer.NextTimer()

		b.Logger().Info(b.backgroundTaskNotifier.Context(), "balance wait", mlog.Duration("nextBalanceInterval", nextBalanceInterval))
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case newReq := <-b.reqCh:
			newReq.apply(b)
			b.applyAllRequest()
		case <-nextTimer:
			// balance triggered by timer.
		case _, ok := <-nodeChanged:
			if !ok {
				return // nodeChanged is only closed if context cancel.
				// in other word, balancer is closed.
			}
			// trigger the watch update.
			b.channelMetaManager.TriggerWatchUpdate()
			// balance triggered by new streaming node changed.
		case <-channelChanged.WaitChan():
			// balance triggered by channel changed.
			channelChanged.Sync()
		case <-b.primaryRGChangedCh:
			b.Logger().Info(
				b.backgroundTaskNotifier.Context(),
				"balance triggered by primary resource group config change",
				mlog.String("primaryResourceGroup", paramtable.Get().StreamingCfg.PrimaryResourceGroup.GetValue()),
			)
		case newChannels, ok := <-b.provider.NewIncomingChannels():
			if !ok {
				return
			}
			if err := b.channelMetaManager.AddPChannels(b.backgroundTaskNotifier.Context(), newChannels); err != nil {
				b.Logger().Warn(b.backgroundTaskNotifier.Context(), "failed to add dynamic channels", mlog.Err(err), mlog.Strings("channels", newChannels))
			}
			// new pchannels added dynamically, trigger rebalance
		}
		if err := b.balanceUntilNoChanged(b.backgroundTaskNotifier.Context()); err != nil {
			if b.backgroundTaskNotifier.Context().Err() != nil {
				// balancer is closed.
				return
			}
			b.Logger().Warn(b.backgroundTaskNotifier.Context(), "fail to apply balance, start a backoff...", mlog.Err(err))
			balanceTimer.EnableBackoff()
			continue
		}
		b.Logger().Info(b.backgroundTaskNotifier.Context(), "apply balance success")
		balanceTimer.DisableBackoff()
	}
}

// blockUntilExpectedInitialStreamingNodeNumReached block until the expected initial streaming node number is reached.
func (b *balancerImpl) blockUntilExpectedInitialStreamingNodeNumReached(ctx context.Context) error {
	if b.channelMetaManager.IsStreamingEnabledOnce() {
		b.Logger().Info(ctx, "streaming has been enabled once, skip waiting initial streaming node number reached")
		return nil
	}

	expectedInitialStreamingNodeNum := paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.GetAsInt()
	if expectedInitialStreamingNodeNum <= 0 {
		b.Logger().Info(ctx, "no expected initial streaming node number, skip waiting initial streaming node number reached")
		return nil
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	logger := b.Logger().With(mlog.Int("expectedInitialStreamingNodeNum", expectedInitialStreamingNodeNum))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes, err := resource.Resource().StreamingNodeManagerClient().GetAllStreamingNodes(ctx)
			if err != nil {
				logger.Warn(ctx, "fail to get all streaming nodes, ignore the error and continue to wait...", mlog.Err(err))
				continue
			}
			if len(nodes) >= expectedInitialStreamingNodeNum {
				logger.Info(ctx, "expected initial streaming node number reached, stop waiting...", mlog.Int("streamingNodeNum", len(nodes)))
				return nil
			}
			logger.Info(ctx, "streaming node number is not enough, continue to wait...", mlog.Int("streamingNodeNum", len(nodes)))
		}
	}
}

// blockUntilRoleGreaterThanVersion blocks until every session of the role is outside versionChecker.
func (b *balancerImpl) blockUntilRoleGreaterThanVersion(ctx context.Context, role string, versionChecker string) error {
	logger := b.Logger().With(mlog.String("role", role))
	logger.Info(ctx, "start to wait that the nodes is greater than version", mlog.String("version", versionChecker))
	// Check if there's any proxy or data node with version < 2.6.0.
	rb := resolver.NewSessionBuilder(resource.Resource().ETCD(),
		discoverer.OptSDPrefix(sessionutil.GetSessionPrefixByRole(role)),
		discoverer.OptSDVersionRange(versionChecker))
	defer rb.Close()

	r := rb.Resolver()
	err := r.Watch(ctx, func(vs resolver.VersionedState) error {
		if len(vs.Sessions()) == 0 {
			return errVersionWatchDone
		}
		logger.Info(ctx, "session changes", mlog.Int("sessionCount", len(vs.Sessions())))
		return nil
	})
	if err != nil && !errors.Is(err, errVersionWatchDone) {
		logger.Info(ctx, "fail to wait that the nodes is greater than version", mlog.String("version", versionChecker), mlog.Err(err))
		return err
	}
	logger.Info(ctx, "all nodes is greater than version when watching", mlog.String("version", versionChecker))
	return nil
}

// applyAllRequest apply all request in the request channel.
func (b *balancerImpl) applyAllRequest() {
	for {
		select {
		case newReq := <-b.reqCh:
			newReq.apply(b)
		default:
			return
		}
	}
}

// balanceUntilNoChanged try to balance until there's changed.
func (b *balancerImpl) balanceUntilNoChanged(ctx context.Context) error {
	for {
		layoutChanged, err := b.balance(ctx)
		if err != nil {
			return err
		}
		if !layoutChanged {
			return nil
		}
	}
}

// Trigger a balance of layout.
// Return a nil chan to avoid
// Return a channel to notify the balance trigger again.
func (b *balancerImpl) balance(ctx context.Context) (bool, error) {
	b.Logger().Info(ctx, "start to balance")
	pchannelView := b.channelMetaManager.CurrentPChannelsView()

	rgName := paramtable.Get().StreamingCfg.PrimaryResourceGroup.GetValue()
	b.Logger().Info(ctx, "collect all status...", mlog.String("resourceGroupHint", rgName))
	nodeStatus, err := b.fetchStreamingNodeStatus(ctx, "")
	if err != nil {
		return false, err
	}

	plans, err := planWALReplicas(pchannelView, nodeStatus, rgName, b.policy)
	if err != nil {
		return false, err
	}
	modifiedChannels, err := b.channelMetaManager.AssignReplicaPlans(ctx, plans)
	if err != nil {
		return false, merr.Wrap(err, "persist WAL replica plans")
	}

	// Preparation and retirement are asynchronous. Re-deliver unchanged
	// durable plans as well, including after a coordinator restart.
	if err := b.applyBalanceResultToStreamingNode(ctx, b.channelMetaManager.CurrentPChannelsView().Channels); err != nil {
		return false, err
	}
	for _, meta := range b.channelMetaManager.CurrentPChannelsView().Channels {
		if !meta.IsAssigned() {
			// Use the normal bounded retry backoff while a writer prepares,
			// instead of waiting an entire periodic rebalance interval.
			return false, merr.WrapErrServiceUnavailableMsg("WAL %s writer is preparing", meta.Name())
		}
	}
	return len(modifiedChannels) != 0, nil
}

// fetchStreamingNodeStatus fetch the streaming node status.
func (b *balancerImpl) fetchStreamingNodeStatus(ctx context.Context, rgName string) (map[int64]*types.StreamingNodeStatus, error) {
	nodeStatus, err := resource.Resource().StreamingNodeManagerClient().CollectAllStatus(ctx, rgName)
	if err != nil {
		return nil, merr.Wrap(err, "fail to collect all status")
	}

	// clean up the freeze node that has been removed from session.
	// Use the full session view (all resource groups) instead of nodeStatus:
	// nodeStatus only contains nodes of the primary/fallback resource group
	// (see filterStreamingNodeStatusByResourceGroupHint), so a frozen node in
	// another resource group would otherwise be wrongly unfrozen.
	// Run before marking frozen nodes below so the marking uses the
	// post-cleanup freeze set. Non-fatal: a transient failure to get the
	// session view must not abort the whole balance round, so skip the
	// cleanup this round and log a warning instead.
	allNodes, err := b.GetAllStreamingNodes(ctx)
	if err != nil {
		b.Logger().Warn(ctx, "fail to get all streaming nodes, skip freeze cleanup this round", mlog.Err(err))
	} else {
		b.freezeNodes.Range(func(serverID int64) bool {
			if _, ok := allNodes[serverID]; !ok {
				b.Logger().Info(ctx, "freeze node has been removed from session", mlog.Int64("serverID", serverID))
				b.freezeNodes.Remove(serverID)
			}
			return true
		})
	}

	// mark the frozen node as frozen in the node status.
	for _, node := range nodeStatus {
		if b.freezeNodes.Contain(node.ServerID) && node.IsHealthy() {
			node.Err = types.ErrFrozen
		}
	}

	// File resources are prepared by each SN consumer through the pull path.
	// QN push acknowledgments do not describe whether an SN can own a WAL.
	return nodeStatus, nil
}

// applyBalanceResultToStreamingNode reconciles each channel independently.
func (b *balancerImpl) applyBalanceResultToStreamingNode(ctx context.Context, channels map[types.ChannelID]*channel.PChannelMeta) error {
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, meta := range channels {
		g.Go(func() error { return b.reconcileReplicaPlan(ctx, meta) })
	}
	return g.Wait()
}

// waitNodeLostGracePeriod blocks for streaming.walBalancer.nodeLostGracePeriod or until ctx is done.
func (b *balancerImpl) waitNodeLostGracePeriod(ctx context.Context, meta *channel.PChannelMeta) error {
	grace := paramtable.Get().StreamingCfg.WALBalancerNodeLostGracePeriod.GetAsDurationByParse()
	if grace <= 0 {
		return nil
	}
	b.Logger().Info(ctx, "wait grace period before assigning channel whose streaming node is lost",
		mlog.String("target", meta.CurrentAssignment().String()),
		mlog.Duration("gracePeriod", grace))
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type backoffConfigFetcher struct{}

func (f *backoffConfigFetcher) BackoffConfig() typeutil.BackoffConfig {
	return typeutil.BackoffConfig{
		InitialInterval: paramtable.Get().StreamingCfg.WALBalancerBackoffInitialInterval.GetAsDurationByParse(),
		Multiplier:      paramtable.Get().StreamingCfg.WALBalancerBackoffMultiplier.GetAsFloat(),
		MaxInterval:     paramtable.Get().StreamingCfg.WALBalancerBackoffMaxInterval.GetAsDurationByParse(),
	}
}

func (f *backoffConfigFetcher) DefaultInterval() time.Duration {
	return paramtable.Get().StreamingCfg.WALBalancerTriggerInterval.GetAsDurationByParse()
}
