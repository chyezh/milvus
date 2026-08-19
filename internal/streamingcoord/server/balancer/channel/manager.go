package channel

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	StreamingVersion260 = 1 // streaming version that since 2.6.0, the streaming based WAL is available.
	StreamingVersion265 = 2 // streaming version that since 2.6.5, the WAL based DDL is available.
	StreamingVersion300 = 3 // streaming version that since 3.0.0, schema-drop DDL is available.
)

var (
	ErrChannelNotExist            = errors.New("channel not exist")
	ErrWALReplicaNotExist         = errors.New("wal replica not exist")
	ErrWALReplicaOperationInvalid = errors.New("wal replica operation invalid")
)

type (
	AllocVChannelParam struct {
		CollectionID int64
		Num          int
	}

	WatchChannelAssignmentsCallbackParam struct {
		StreamingVersion       *streamingpb.StreamingVersion
		Version                typeutil.VersionInt64Pair
		CChannelAssignment     *streamingpb.CChannelAssignment
		PChannelView           *PChannelView
		Relations              []types.PChannelInfoAssigned
		WALReplicaRelations    []types.WALReplicaInfoAssigned
		ShardAssignments       map[int64]types.ShardAssignmentInfo
		ReplicateConfiguration *commonpb.ReplicateConfiguration
	}
	WatchChannelAssignmentsCallback func(param WatchChannelAssignmentsCallbackParam) error

	// ShardAssignmentProvider supplies discoverable QueryView shard replicas
	// grouped by pchannel. ChannelManager maps these entries to current SN
	// owners when publishing assignment discovery.
	ShardAssignmentProvider interface {
		ShardAssignmentsByPChannel() map[string][]types.ShardAssignmentEntry
	}

	WALReplicaDependencyProvider interface {
		HasWALReplicaDependency(replicaID ChannelID) bool
	}
)

// RecoverChannelManager creates a new channel manager.
func RecoverChannelManager(ctx context.Context, incomingChannel ...string) (*ChannelManager, error) {
	// streamingVersion is used to identify current streaming service version.
	// Used to check if there's some upgrade happens.
	streamingVersion, err := resource.Resource().StreamingCatalog().GetVersion(ctx)
	if err != nil {
		return nil, err
	}
	cchannelMeta, err := recoverCChannelMeta(ctx, incomingChannel...)
	if err != nil {
		return nil, err
	}
	replicateConfig, err := recoverReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	channels, metrics, err := recoverFromConfigurationAndMeta(ctx, streamingVersion, replicateConfig, incomingChannel...)
	if err != nil {
		return nil, err
	}

	globalVersion := resource.Resource().Session().GetRegisteredRevision()
	cm := &ChannelManager{
		cond:     syncutil.NewContextCond(&sync.Mutex{}),
		channels: channels,
		version: typeutil.VersionInt64Pair{
			Global: globalVersion, // global version should be keep increasing globally, use revision of session to promise it.
			Local:  0,
		},
		metrics:          metrics,
		cchannelMeta:     cchannelMeta,
		streamingVersion: streamingVersion,
		replicateConfig:  replicateConfig,
	}
	replicateRole := replicateutil.RolePrimary
	if replicateConfig != nil {
		replicateRole = replicateConfig.GetCurrentCluster().Role()
	}
	cm.replicateRole.Store(int32(replicateRole))

	// Register the channel manager singleton after recovery.
	register(cm)

	return cm, nil
}

// getClusterChannels returns the pchannel names and the control channel name.
// By default, only channels available in replication are returned.
// Use OptIncludeUnavailableInReplication() to include unavailable channels.
func (cm *ChannelManager) getClusterChannels(opts ...GetClusterChannelsOpt) message.ClusterChannels {
	o := &getClusterChannelsOptions{}
	for _, opt := range opts {
		opt(o)
	}

	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	channels := make([]string, 0, len(cm.channels))
	for _, ch := range cm.channels {
		if !o.includeUnavailableInReplication && !ch.AvailableInReplication() {
			continue
		}
		channels = append(channels, ch.Name())
	}
	return message.ClusterChannels{
		Channels:       channels,
		ControlChannel: funcutil.GetControlChannel(cm.cchannelMeta.Pchannel),
	}
}

// recoverCChannelMeta recovers the control channel meta.
func recoverCChannelMeta(ctx context.Context, incomingChannel ...string) (*streamingpb.CChannelMeta, error) {
	cchannelMeta, err := resource.Resource().StreamingCatalog().GetCChannel(ctx)
	if err != nil {
		return nil, err
	}
	if cchannelMeta == nil {
		if len(incomingChannel) == 0 {
			return nil, status.NewInner("no incoming channel while no control channel meta found")
		}
		cchannelMeta = &streamingpb.CChannelMeta{
			Pchannel: incomingChannel[0],
		}
		if err := resource.Resource().StreamingCatalog().SaveCChannel(ctx, cchannelMeta); err != nil {
			return nil, err
		}
		return cchannelMeta, nil
	}
	return cchannelMeta, nil
}

// recoverFromConfigurationAndMeta recovers the channel manager from configuration and meta.
func recoverFromConfigurationAndMeta(ctx context.Context, streamingVersion *streamingpb.StreamingVersion, replicateConfig *replicateutil.ConfigHelper, incomingChannel ...string) (map[ChannelID]*PChannelMeta, *channelMetrics, error) {
	// Recover metrics.
	metrics := newPChannelMetrics()

	// Get all channels from meta.
	channelMetas, err := resource.Resource().StreamingCatalog().ListPChannel(ctx)
	if err != nil {
		return nil, metrics, err
	}

	// TODO: only support rw channel here now, add ro channel in future.
	channels := make(map[ChannelID]*PChannelMeta, len(channelMetas))
	for _, channel := range channelMetas {
		c := newPChannelMetaFromProto(channel, replicateConfig)
		metrics.AssignPChannelStatus(c)
		channels[c.ChannelID()] = c
	}

	// Get new incoming meta from configuration.
	for _, newChannel := range incomingChannel {
		var c *PChannelMeta
		if streamingVersion == nil {
			// if streaming service has never been enabled, we treat all channels as read-only.
			c = NewPChannelMeta(newChannel, types.AccessModeRO)
		} else {
			// once the streaming service is enabled, we treat all channels as read-write.
			c = NewPChannelMeta(newChannel, types.AccessModeRW)
		}
		c.availableInReplication = isChannelAvailableInReplication(c.Name(), replicateConfig)
		if _, ok := channels[c.ChannelID()]; !ok {
			channels[c.ChannelID()] = c
		}
	}
	return channels, metrics, nil
}

func recoverReplicateConfiguration(ctx context.Context) (*replicateutil.ConfigHelper, error) {
	config, err := resource.Resource().StreamingCatalog().GetReplicateConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	return replicateutil.MustNewConfigHelper(
		paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		config.GetReplicateConfiguration(),
	), nil
}

// isChannelAvailableInReplication returns whether a channel is available for replication.
// A channel is unavailable only when there's a multi-cluster replication topology
// AND the channel is not in the current cluster's PChannel list.
func isChannelAvailableInReplication(channelName string, config *replicateutil.ConfigHelper) bool {
	if config == nil {
		return true
	}
	if !config.IsJoinReplication() {
		return true
	}
	for _, pchannel := range config.GetCurrentCluster().GetPchannels() {
		if pchannel == channelName {
			return true
		}
	}
	return false
}

// ChannelManager manages the channels.
// ChannelManager is the `wal` of channel assignment and unassignment.
// Every operation applied to the streaming node should be recorded in ChannelManager first.
type ChannelManager struct {
	mlog.Binder

	cond             *syncutil.ContextCond
	channels         map[ChannelID]*PChannelMeta
	version          typeutil.VersionInt64Pair
	metrics          *channelMetrics
	cchannelMeta     *streamingpb.CChannelMeta
	streamingVersion *streamingpb.StreamingVersion // used to identify the current streaming service version.
	// null if no streaming service has been run.
	// 1 if streaming service has been run once.
	streamingEnableNotifiers     []*syncutil.AsyncTaskNotifier[struct{}]
	replicateConfig              *replicateutil.ConfigHelper
	replicateRole                atomic.Int32 // lock-free snapshot published after replicateConfig is persisted
	shardAssignmentProvider      ShardAssignmentProvider
	walReplicaDependencyProvider WALReplicaDependencyProvider
}

// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
func (cm *ChannelManager) RegisterStreamingEnabledNotifier(notifier *syncutil.AsyncTaskNotifier[struct{}]) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion != nil {
		// If the streaming service is already enabled once, notify the notifier and ignore it.
		notifier.Cancel()
		return
	}
	cm.streamingEnableNotifiers = append(cm.streamingEnableNotifiers, notifier)
}

// IsStreamingEnabledOnce returns true if streaming is enabled once.
func (cm *ChannelManager) IsStreamingEnabledOnce() bool {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	return cm.streamingVersion != nil
}

// WaitUntilStreamingEnabled waits until the streaming service is enabled.
func (cm *ChannelManager) WaitUntilStreamingEnabled(ctx context.Context) error {
	cm.cond.L.Lock()
	for cm.streamingVersion == nil {
		if err := cm.cond.Wait(ctx); err != nil {
			return err
		}
	}
	cm.cond.L.Unlock()
	return nil
}

// IsStreamingVersionAtLeast returns true if the persisted streaming version is at least version.
func (cm *ChannelManager) IsStreamingVersionAtLeast(version int64) bool {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	return cm.streamingVersion != nil && cm.streamingVersion.Version >= version
}

// ReplicateRole returns the replicate role of the channel manager.
func (cm *ChannelManager) ReplicateRole() replicateutil.Role {
	return replicateutil.Role(cm.replicateRole.Load())
}

// AddPChannels adds new PChannels dynamically. Channels that already exist are skipped.
// Only newly added channels are persisted. Local version is not incremented
// because new PChannels should not trigger service discovery.
func (cm *ChannelManager) AddPChannels(ctx context.Context, newChannels []string) error {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	newMetas := make([]*streamingpb.PChannelMeta, 0, len(newChannels))
	for _, name := range newChannels {
		id := ChannelID{Name: name}
		if _, ok := cm.channels[id]; ok {
			continue
		}
		var meta *PChannelMeta
		if cm.streamingVersion == nil {
			meta = NewPChannelMeta(name, types.AccessModeRO)
		} else {
			meta = NewPChannelMeta(name, types.AccessModeRW)
		}
		meta.availableInReplication = isChannelAvailableInReplication(name, cm.replicateConfig)
		cm.channels[id] = meta
		cm.metrics.AssignPChannelStatus(meta)
		newMetas = append(newMetas, meta.CopyForWrite().IntoRawMeta())
	}

	if len(newMetas) == 0 {
		return nil
	}

	if err := resource.Resource().StreamingCatalog().SavePChannels(ctx, newMetas); err != nil {
		// Rollback in-memory changes on persist failure
		for _, m := range newMetas {
			c := newPChannelMetaFromProto(m, cm.replicateConfig)
			delete(cm.channels, c.ChannelID())
		}
		cm.Logger().Error(ctx, "failed to save new pchannels", mlog.Err(err))
		return err
	}

	cm.Logger().Info(ctx, "dynamically added new pchannels",
		mlog.Int("count", len(newMetas)),
		mlog.Strings("channels", newChannels))
	return nil
}

// TriggerWatchUpdate triggers the watch update.
// Because current watch must see new incoming streaming node right away,
// so a watch updating trigger will be called if there's new incoming streaming node.
func (cm *ChannelManager) TriggerWatchUpdate() {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	cm.version.Local++
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
}

func (cm *ChannelManager) SetShardAssignmentProvider(provider ShardAssignmentProvider) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	cm.shardAssignmentProvider = provider
	cm.version.Local++
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
}

func (cm *ChannelManager) SetWALReplicaDependencyProvider(provider WALReplicaDependencyProvider) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	cm.walReplicaDependencyProvider = provider
}

// MarkStreamingHasEnabled marks the streaming service has been enabled.
func (cm *ChannelManager) MarkStreamingHasEnabled(ctx context.Context) error {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion != nil {
		return nil
	}

	cm.streamingVersion = &streamingpb.StreamingVersion{
		Version: StreamingVersion260,
	}

	if err := resource.Resource().StreamingCatalog().SaveVersion(ctx, cm.streamingVersion); err != nil {
		cm.Logger().Error(ctx, "failed to save streaming version", mlog.Err(err))
		return err
	}

	// notify all notifiers that the streaming service has been enabled.
	for _, notifier := range cm.streamingEnableNotifiers {
		notifier.Cancel()
	}
	// and block until the listener of notifiers are finished.
	for _, notifier := range cm.streamingEnableNotifiers {
		notifier.BlockUntilFinish()
	}
	cm.streamingEnableNotifiers = nil
	return nil
}

// MarkStreamingVersion persists the streaming version after the related cluster-version gate passes.
func (cm *ChannelManager) MarkStreamingVersion(ctx context.Context, version int64) error {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.streamingVersion == nil {
		return status.NewInner("streaming service is not enabled, cannot mark streaming version")
	}
	if cm.streamingVersion.Version >= version {
		return nil
	}
	cm.streamingVersion.Version = version
	if err := resource.Resource().StreamingCatalog().SaveVersion(ctx, cm.streamingVersion); err != nil {
		cm.Logger().Error(ctx, "failed to save streaming version", mlog.Err(err))
		return err
	}
	return nil
}

// CurrentPChannelsView returns the current view of pchannels.
func (cm *ChannelManager) CurrentPChannelsView() *PChannelView {
	cm.cond.L.Lock()
	view := newPChannelView(cm.channels)
	cm.cond.L.Unlock()

	for _, channel := range view.Channels {
		cm.metrics.UpdateVChannelTotal(channel)
	}
	return view
}

// AllocVirtualChannels allocates virtual channels for a collection.
// Only channels that are available in replication are considered.
func (cm *ChannelManager) AllocVirtualChannels(ctx context.Context, param AllocVChannelParam) ([]string, error) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	availableChannels := cm.sortAvailableChannelsByVChannelCount()
	if len(availableChannels) < param.Num {
		return nil, status.NewInner("not enough pchannels to allocate, expected: %d, got: %d", param.Num, len(availableChannels))
	}

	vchannels := make([]string, 0, param.Num)
	for _, channel := range availableChannels {
		if len(vchannels) >= param.Num {
			break
		}
		vchannels = append(vchannels, funcutil.GetVirtualChannel(channel.id.Name, param.CollectionID, len(vchannels)))
	}
	return vchannels, nil
}

// withVChannelCount is a helper struct to sort the channels by the vchannel count.
type withVChannelCount struct {
	id            ChannelID
	vchannelCount int
}

// sortAvailableChannelsByVChannelCount sorts the available channels by the vchannel count.
// Channels that are unavailable in replication are excluded.
func (cm *ChannelManager) sortAvailableChannelsByVChannelCount() []withVChannelCount {
	vchannelCounts := make([]withVChannelCount, 0, len(cm.channels))
	for id, ch := range cm.channels {
		if !ch.AvailableInReplication() {
			continue
		}
		vchannelCounts = append(vchannelCounts, withVChannelCount{
			id:            id,
			vchannelCount: StaticPChannelStatsManager.Get().GetPChannelStats(id).VChannelCount(),
		})
	}
	sort.Slice(vchannelCounts, func(i, j int) bool {
		if vchannelCounts[i].vchannelCount == vchannelCounts[j].vchannelCount {
			// make a stable sort result, so get the order of sort result with same vchannel count by name.
			return vchannelCounts[i].id.Name < vchannelCounts[j].id.Name
		}
		return vchannelCounts[i].vchannelCount < vchannelCounts[j].vchannelCount
	})
	return vchannelCounts
}

// AssignPChannels update the pchannels to servers and return the modified pchannels.
// When the balancer want to assign a pchannel into a new server.
// It should always call this function to update the pchannel assignment first.
// Otherwise, the pchannel assignment tracing is lost at meta.
func (cm *ChannelManager) AssignPChannels(ctx context.Context, pChannelToStreamingNode map[ChannelID]types.PChannelInfoAssigned) (map[ChannelID]*PChannelMeta, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannelToStreamingNode))
	for id, assign := range pChannelToStreamingNode {
		pchannel, ok := cm.channels[id]
		if !ok {
			return nil, ErrChannelNotExist
		}
		mutablePchannel := pchannel.CopyForWrite()
		if mutablePchannel.TryAssignToServerID(assign.Channel.AccessMode, assign.Node) {
			pChannelMetas = append(pChannelMetas, mutablePchannel.IntoRawMeta())
		}
	}

	err := cm.updatePChannelMeta(ctx, pChannelMetas)
	if err != nil {
		return nil, err
	}
	updates := make(map[ChannelID]*PChannelMeta, len(pChannelMetas))
	for _, pchannel := range pChannelMetas {
		meta := newPChannelMetaFromProto(pchannel, cm.replicateConfig)
		updates[meta.ChannelID()] = meta
		cm.metrics.AssignPChannelStatus(meta)
	}
	return updates, nil
}

// AssignPChannelsDone clear up the history data of the pchannels and transfer the state into assigned.
// When the balancer want to cleanup the history data of a pchannel.
// It should always remove the pchannel on the server first.
// Otherwise, the pchannel assignment tracing is lost at meta.
func (cm *ChannelManager) AssignPChannelsDone(ctx context.Context, pChannels []ChannelID) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannels))
	for _, channelID := range pChannels {
		pchannel, ok := cm.channels[channelID]
		if !ok {
			return ErrChannelNotExist
		}
		mutablePChannel := pchannel.CopyForWrite()
		mutablePChannel.AssignToServerDone()
		pChannelMetas = append(pChannelMetas, mutablePChannel.IntoRawMeta())
	}

	if err := cm.updatePChannelMeta(ctx, pChannelMetas); err != nil {
		return err
	}

	// Update metrics.
	for _, pchannel := range pChannelMetas {
		cm.metrics.AssignPChannelStatus(newPChannelMetaFromProto(pchannel, cm.replicateConfig))
	}
	return nil
}

// CreateReadOnlyWALReplica creates a secondary WAL replica entry for the given PChannel.
func (cm *ChannelManager) CreateReadOnlyWALReplica(ctx context.Context, pchannel string, resourceGroup string) (ChannelID, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	pchannelMeta, ok := cm.channels[ChannelID{Name: pchannel}]
	if !ok {
		return ChannelID{}, ErrChannelNotExist
	}
	mutablePChannel := pchannelMeta.CopyForWrite()
	replicaID := mutablePChannel.CreateReadOnlyWALReplica(resourceGroup)
	rawMeta := mutablePChannel.IntoRawMeta()
	if err := cm.updatePChannelMeta(ctx, []*streamingpb.PChannelMeta{rawMeta}); err != nil {
		return ChannelID{}, err
	}
	return ChannelID{Name: pchannel, WALReplicaID: replicaID}, nil
}

// AssignWALReplicas prepares WAL replicas on target StreamingNodes.
func (cm *ChannelManager) AssignWALReplicas(ctx context.Context, assignments map[ChannelID]types.StreamingNodeInfo) (map[ChannelID]*PChannelMeta, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	modifiedKeys := make([]ChannelID, 0, len(assignments))
	for id, node := range assignments {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return nil, ErrChannelNotExist
		}
		replica, ok := pchannel.WALReplica(id.WALReplicaID)
		if !ok {
			return nil, ErrWALReplicaNotExist
		}
		if id.WALReplicaID == pchannel.PrimaryReplicaID() ||
			replica.GetAccessMode() != streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY {
			return nil, ErrWALReplicaOperationInvalid
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
			mutablePChannels[pchannelID] = mutablePChannel
		}
		if mutablePChannel.TryAssignWALReplicaToServerID(id.WALReplicaID, node) {
			modifiedKeys = append(modifiedKeys, id)
		}
	}

	pChannelMetas := rawMetasFromMutablePChannels(mutablePChannels)
	if err := cm.updatePChannelMeta(ctx, pChannelMetas); err != nil {
		return nil, err
	}
	updates := make(map[ChannelID]*PChannelMeta, len(modifiedKeys))
	for _, id := range modifiedKeys {
		updates[id] = cm.channels[ChannelID{Name: id.Name}]
	}
	return updates, nil
}

// AssignWALReplicasDone makes prepared WAL replica targets serviceable when the assignment epoch matches.
func (cm *ChannelManager) AssignWALReplicasDone(ctx context.Context, replicas map[ChannelID]int64) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	for id, assignmentEpoch := range replicas {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return ErrChannelNotExist
		}
		if _, ok := pchannel.WALReplica(id.WALReplicaID); !ok {
			return ErrWALReplicaNotExist
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
		}
		if mutablePChannel.AssignWALReplicaToServerDone(id.WALReplicaID, assignmentEpoch) {
			mutablePChannels[pchannelID] = mutablePChannel
		}
	}

	if len(mutablePChannels) == 0 {
		return nil
	}
	return cm.updatePChannelMeta(ctx, rawMetasFromMutablePChannels(mutablePChannels))
}

// ClearWALReplicaHistories clears cleanup histories after old WAL replica runtimes are released.
func (cm *ChannelManager) ClearWALReplicaHistories(ctx context.Context, replicas []ChannelID) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	for _, id := range replicas {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return ErrChannelNotExist
		}
		if _, ok := pchannel.WALReplica(id.WALReplicaID); !ok {
			return ErrWALReplicaNotExist
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
		}
		if mutablePChannel.ClearWALReplicaHistories(id.WALReplicaID) {
			mutablePChannels[pchannelID] = mutablePChannel
		}
	}

	return cm.updatePChannelMeta(ctx, rawMetasFromMutablePChannels(mutablePChannels))
}

// MarkWALReplicasAsUnavailable marks reported read-only WAL replicas as unavailable.
func (cm *ChannelManager) MarkWALReplicasAsUnavailable(ctx context.Context, replicas []ChannelID, assignmentEpoch int64) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	for _, id := range replicas {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return ErrChannelNotExist
		}
		if _, ok := pchannel.WALReplica(id.WALReplicaID); !ok {
			return ErrWALReplicaNotExist
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
		}
		if mutablePChannel.MarkWALReplicaAsUnavailable(id.WALReplicaID, assignmentEpoch) {
			mutablePChannels[pchannelID] = mutablePChannel
		}
	}

	return cm.updatePChannelMeta(ctx, rawMetasFromMutablePChannels(mutablePChannels))
}

// MarkWALPrimaryReplicaAsUnavailable marks a failed primary WAL replica open as unavailable.
func (cm *ChannelManager) MarkWALPrimaryReplicaAsUnavailable(ctx context.Context, replicaID ChannelID, assignmentEpoch int64) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	pchannelID := ChannelID{Name: replicaID.Name}
	pchannel, ok := cm.channels[pchannelID]
	if !ok {
		return ErrChannelNotExist
	}
	if _, ok := pchannel.WALReplica(replicaID.WALReplicaID); !ok {
		return ErrWALReplicaNotExist
	}
	mutablePChannel := pchannel.CopyForWrite()
	if !mutablePChannel.MarkPrimaryWALReplicaAsUnavailable(replicaID.WALReplicaID, assignmentEpoch) {
		return ErrWALReplicaOperationInvalid
	}
	return cm.updatePChannelMeta(ctx, []*streamingpb.PChannelMeta{mutablePChannel.IntoRawMeta()})
}

// SwitchWALPrimaryReplica promotes a serviceable read-only replica as the PChannel primary writer.
func (cm *ChannelManager) SwitchWALPrimaryReplica(ctx context.Context, pchannel string, targetReplicaID int64) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	pchannelID := ChannelID{Name: pchannel}
	pchannelMeta, ok := cm.channels[pchannelID]
	if !ok {
		return ErrChannelNotExist
	}
	if _, ok := pchannelMeta.WALReplica(targetReplicaID); !ok {
		return ErrWALReplicaNotExist
	}
	if pchannelMeta.PrimaryReplicaID() == targetReplicaID {
		target, _ := pchannelMeta.WALReplica(targetReplicaID)
		if target.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE {
			switch target.GetState() {
			case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED:
				return nil
			}
		}
		return ErrWALReplicaOperationInvalid
	}
	if !cm.isTargetWALReplicaReadyForPrimaryLocked(pchannel, pchannelMeta.PrimaryReplicaID(), targetReplicaID) {
		return ErrWALReplicaOperationInvalid
	}
	mutablePChannel := pchannelMeta.CopyForWrite()
	if !mutablePChannel.SwitchPrimaryWALReplica(targetReplicaID) {
		return ErrWALReplicaOperationInvalid
	}
	return cm.updatePChannelMeta(ctx, []*streamingpb.PChannelMeta{mutablePChannel.IntoRawMeta()})
}

type primaryServingShardKey struct {
	collectionID int64
	shardIndex   int32
}

func (cm *ChannelManager) isTargetWALReplicaReadyForPrimaryLocked(pchannel string, oldPrimaryReplicaID int64, targetReplicaID int64) bool {
	if cm.shardAssignmentProvider == nil {
		return true
	}
	entries := cm.shardAssignmentProvider.ShardAssignmentsByPChannel()[pchannel]
	if len(entries) == 0 {
		return true
	}
	oldPrimaryShards := make(map[primaryServingShardKey]struct{})
	targetShards := make(map[primaryServingShardKey]struct{})
	for _, entry := range entries {
		key := primaryServingShardKey{
			collectionID: entry.CollectionID,
			shardIndex:   entry.ShardIndex,
		}
		switch entry.WALReplicaID {
		case oldPrimaryReplicaID:
			oldPrimaryShards[key] = struct{}{}
		case targetReplicaID:
			targetShards[key] = struct{}{}
		}
	}
	for key := range oldPrimaryShards {
		if _, ok := targetShards[key]; !ok {
			return false
		}
	}
	return true
}

// MarkWALReplicasAsDropping marks non-primary read-only WAL replicas as dropping.
func (cm *ChannelManager) MarkWALReplicasAsDropping(ctx context.Context, replicas []ChannelID) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	for _, id := range replicas {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return ErrChannelNotExist
		}
		if _, ok := pchannel.WALReplica(id.WALReplicaID); !ok {
			return ErrWALReplicaNotExist
		}
		if cm.hasWALReplicaDependencyLocked(id) {
			return ErrWALReplicaOperationInvalid
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
			mutablePChannels[pchannelID] = mutablePChannel
		}
		if !mutablePChannel.MarkWALReplicaAsDropping(id.WALReplicaID) {
			return ErrWALReplicaOperationInvalid
		}
	}

	return cm.updatePChannelMeta(ctx, rawMetasFromMutablePChannels(mutablePChannels))
}

// RemoveWALReplicas removes dropping WAL replica entries from PChannel meta.
func (cm *ChannelManager) RemoveWALReplicas(ctx context.Context, replicas []ChannelID) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	mutablePChannels := make(map[ChannelID]*mutablePChannel)
	for _, id := range replicas {
		pchannelID := ChannelID{Name: id.Name}
		pchannel, ok := cm.channels[pchannelID]
		if !ok {
			return ErrChannelNotExist
		}
		if _, ok := pchannel.WALReplica(id.WALReplicaID); !ok {
			return ErrWALReplicaNotExist
		}
		if cm.hasWALReplicaDependencyLocked(id) {
			return ErrWALReplicaOperationInvalid
		}
		mutablePChannel := mutablePChannels[pchannelID]
		if mutablePChannel == nil {
			mutablePChannel = pchannel.CopyForWrite()
			mutablePChannels[pchannelID] = mutablePChannel
		}
		if !mutablePChannel.RemoveWALReplica(id.WALReplicaID) {
			return ErrWALReplicaOperationInvalid
		}
	}

	return cm.updatePChannelMeta(ctx, rawMetasFromMutablePChannels(mutablePChannels))
}

func (cm *ChannelManager) hasWALReplicaDependencyLocked(replicaID ChannelID) bool {
	if cm.walReplicaDependencyProvider == nil {
		return false
	}
	return cm.walReplicaDependencyProvider.HasWALReplicaDependency(replicaID)
}

func rawMetasFromMutablePChannels(mutablePChannels map[ChannelID]*mutablePChannel) []*streamingpb.PChannelMeta {
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(mutablePChannels))
	for _, mutablePChannel := range mutablePChannels {
		pChannelMetas = append(pChannelMetas, mutablePChannel.IntoRawMeta())
	}
	return pChannelMetas
}

// MarkAsUnavailable mark the pchannels as unavailable.
func (cm *ChannelManager) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	// modified channels.
	pChannelMetas := make([]*streamingpb.PChannelMeta, 0, len(pChannels))
	for _, channel := range pChannels {
		pchannel, ok := cm.channels[channel.ChannelID()]
		if !ok {
			return ErrChannelNotExist
		}
		mutablePChannel := pchannel.CopyForWrite()
		mutablePChannel.MarkAsUnavailable(channel.Term)
		pChannelMetas = append(pChannelMetas, mutablePChannel.IntoRawMeta())
	}

	if err := cm.updatePChannelMeta(ctx, pChannelMetas); err != nil {
		return err
	}
	for _, pchannel := range pChannelMetas {
		cm.metrics.AssignPChannelStatus(newPChannelMetaFromProto(pchannel, cm.replicateConfig))
	}
	return nil
}

// updatePChannelMeta updates the pchannel metas.
func (cm *ChannelManager) updatePChannelMeta(ctx context.Context, pChannelMetas []*streamingpb.PChannelMeta) error {
	if len(pChannelMetas) == 0 {
		return nil
	}

	if err := resource.Resource().StreamingCatalog().SavePChannels(ctx, pChannelMetas); err != nil {
		cm.Logger().Error(ctx, "failed to save pchannels", mlog.Err(err))
		return err
	}

	// update in-memory copy and increase the version.
	for _, pchannel := range pChannelMetas {
		c := newPChannelMetaFromProto(pchannel, cm.replicateConfig)
		cm.channels[c.ChannelID()] = c
	}
	cm.version.Local++
	// update metrics.
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
	return nil
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
func (cm *ChannelManager) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	pChannelMeta, ok := cm.channels[types.ChannelID{Name: pchannel}]
	if !ok {
		return 0, false
	}
	if pChannelMeta.IsAssigned() {
		return pChannelMeta.CurrentServerID(), true
	}
	return 0, false
}

// GetLatestChannelAssignment returns the latest channel assignment.
func (cm *ChannelManager) GetLatestChannelAssignment() (*WatchChannelAssignmentsCallbackParam, error) {
	var result WatchChannelAssignmentsCallbackParam
	if _, err := cm.applyAssignments(func(param WatchChannelAssignmentsCallbackParam) error {
		result = param
		return nil
	}); err != nil {
		return nil, err
	}
	return &result, nil
}

func (cm *ChannelManager) WatchAssignmentResult(ctx context.Context, cb WatchChannelAssignmentsCallback) error {
	// push the first balance result to watcher callback function if balance result is ready.
	version, err := cm.applyAssignments(cb)
	if err != nil {
		return err
	}
	for {
		// wait for version change, and apply the latest assignment to callback.
		if err := cm.waitChanges(ctx, version); err != nil {
			return err
		}
		if version, err = cm.applyAssignments(cb); err != nil {
			return err
		}
	}
}

// UpdateReplicateConfiguration updates the in-memory replicate configuration.
func (cm *ChannelManager) UpdateReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
	msg := result.Message
	config := replicateutil.MustNewConfigHelper(paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), msg.Header().ReplicateConfiguration)
	cm.cond.L.Lock()
	defer cm.cond.L.Unlock()

	if cm.replicateConfig != nil && proto.Equal(config.GetReplicateConfiguration(), cm.replicateConfig.GetReplicateConfiguration()) {
		// check if the replicate configuration is changed.
		// if not changed, return it directly.
		return nil
	}

	appendResults := lo.MapKeys(result.Results, func(_ *message.AppendResult, key string) string {
		return funcutil.ToPhysicalChannel(key)
	})
	newIncomingCDCTasks := cm.getNewIncomingTask(config, appendResults)

	// Check if this is a force promote based on message header
	isForcePromote := msg.Header().ForcePromote

	var configMeta *streamingpb.ReplicateConfigurationMeta
	if isForcePromote {
		// For force promotes, mark the config with force flags
		configMeta = &streamingpb.ReplicateConfigurationMeta{
			ReplicateConfiguration: config.GetReplicateConfiguration(),
			ForcePromoted:          true,
		}
		cm.Logger().Info(ctx, "Applying force promote to replicate configuration",
			replicateutil.ConfigLogField(config.GetReplicateConfiguration()),
		)
	} else {
		// For normal replicate configuration updates, don't set force flags
		configMeta = &streamingpb.ReplicateConfigurationMeta{
			ReplicateConfiguration: config.GetReplicateConfiguration(),
			ForcePromoted:          false,
		}
	}

	if err := resource.Resource().StreamingCatalog().SaveReplicateConfiguration(ctx, configMeta, newIncomingCDCTasks); err != nil {
		cm.Logger().Error(ctx, "failed to save replicate configuration", mlog.Err(err))
		return err
	}

	cm.Logger().Info(ctx, "Saved replicate configuration", replicateutil.ConfigLogField(config.GetReplicateConfiguration()))

	cm.replicateConfig = config
	// Recompute availableInReplication for all channels after config update
	for _, ch := range cm.channels {
		ch.availableInReplication = isChannelAvailableInReplication(ch.Name(), cm.replicateConfig)
	}
	cm.cond.UnsafeBroadcast()
	cm.version.Local++
	cm.metrics.UpdateAssignmentVersion(cm.version.Local)
	cm.replicateRole.Store(int32(config.GetCurrentCluster().Role()))
	return nil
}

// getNewIncomingTask gets the new incoming task from replicatingTasks.
func (cm *ChannelManager) getNewIncomingTask(newConfig *replicateutil.ConfigHelper, appendResults map[string]*message.AppendResult) []*streamingpb.ReplicatePChannelMeta {
	incoming := newConfig.GetCurrentCluster()
	var current *replicateutil.MilvusCluster
	if cm.replicateConfig != nil {
		current = cm.replicateConfig.GetCurrentCluster()
	}
	incomingReplicatingTasks := make([]*streamingpb.ReplicatePChannelMeta, 0, len(incoming.TargetClusters()))
	for _, targetCluster := range incoming.TargetClusters() {
		// Determine which pchannels are new and need CDC tasks.
		// If the target cluster already exists, only create tasks for newly appended pchannels.
		newPchannels := targetCluster.GetPchannels()
		skipGetReplicateCheckpoint := false
		if current != nil {
			if currentTarget := current.TargetCluster(targetCluster.GetClusterId()); currentTarget != nil {
				existingCount := len(currentTarget.GetPchannels())
				if existingCount >= len(newPchannels) {
					// No new pchannels, skip this target cluster.
					continue
				}
				// Only process newly appended pchannels (validator ensures existing pchannels are preserved at same positions).
				newPchannels = newPchannels[existingCount:]
				// For pchannel-increasing tasks, the secondary WAL for new pchannels hasn't received
				// the AlterReplicateConfig yet, so GetReplicateInfo would fail. Skip it and use
				// InitializedCheckpoint directly. The secondary filters out duplicates on restart.
				skipGetReplicateCheckpoint = true
			}
		}
		for _, pchannel := range newPchannels {
			sourceClusterID := targetCluster.SourceCluster().ClusterId
			sourcePChannel := targetCluster.MustGetSourceChannel(pchannel)
			checkpointTimeTick := appendResults[sourcePChannel].TimeTick
			if skipGetReplicateCheckpoint {
				// For pchannel-increasing tasks, the CDC scanner uses DeliverFilterTimeTickGT
				// (strictly greater than). Subtract 1 so the AlterReplicateConfig message itself
				// (whose TimeTick == appendResults.TimeTick) is included in the scan.
				// The secondary needs this message on ALL pchannels for the broadcast to complete.
				checkpointTimeTick--
			}
			incomingReplicatingTasks = append(incomingReplicatingTasks, &streamingpb.ReplicatePChannelMeta{
				SourceChannelName: sourcePChannel,
				TargetChannelName: pchannel,
				TargetCluster:     targetCluster.MilvusCluster,
				// The checkpoint is set as the initialized checkpoint for one cdc-task,
				// when the startup of one cdc-task, the checkpoint returned from the target cluster is nil,
				// so we set the initialized checkpoint here to start operation from here.
				// the InitializedCheckpoint is always keep same semantic with the checkpoint at target cluster.
				// so the cluster id is the source cluster id (aka. current cluster id)
				InitializedCheckpoint: &commonpb.ReplicateCheckpoint{
					ClusterId: sourceClusterID,
					Pchannel:  sourcePChannel,
					MessageId: appendResults[sourcePChannel].LastConfirmedMessageID.IntoProto(),
					TimeTick:  checkpointTimeTick,
				},
				SkipGetReplicateCheckpoint: skipGetReplicateCheckpoint,
			})
		}
	}
	return incomingReplicatingTasks
}

// applyAssignments applies the assignments.
func (cm *ChannelManager) applyAssignments(cb WatchChannelAssignmentsCallback) (typeutil.VersionInt64Pair, error) {
	cm.cond.L.Lock()
	assignments := make([]types.PChannelInfoAssigned, 0, len(cm.channels))
	for _, c := range cm.channels {
		if c.IsAssigned() {
			assignments = append(assignments, c.CurrentAssignment())
		}
	}
	walReplicaAssignments := buildWALReplicaAssignments(cm.channels)
	version := cm.version
	cchannelAssignment := proto.Clone(cm.cchannelMeta).(*streamingpb.CChannelMeta)
	pchannelViews := newPChannelView(cm.channels)
	shardAssignmentProvider := cm.shardAssignmentProvider
	cm.cond.L.Unlock()

	var replicateConfig *commonpb.ReplicateConfiguration
	if cm.replicateConfig != nil {
		replicateConfig = cm.replicateConfig.GetReplicateConfiguration()
	}
	shardAssignments := buildShardAssignments(assignments, walReplicaAssignments, shardAssignmentProvider)
	return version, cb(WatchChannelAssignmentsCallbackParam{
		StreamingVersion: cm.streamingVersion,
		Version:          version,
		CChannelAssignment: &streamingpb.CChannelAssignment{
			Meta: cchannelAssignment,
		},
		PChannelView:           pchannelViews,
		Relations:              assignments,
		WALReplicaRelations:    walReplicaAssignments,
		ShardAssignments:       shardAssignments,
		ReplicateConfiguration: replicateConfig,
	})
}

func buildWALReplicaAssignments(channels map[ChannelID]*PChannelMeta) []types.WALReplicaInfoAssigned {
	assignments := make([]types.WALReplicaInfoAssigned, 0, len(channels))
	for _, channel := range channels {
		pchannel := channel.Name()
		term := channel.CurrentTerm()
		for _, replica := range channel.Replicas() {
			if !isWALReplicaServiceableForDiscovery(replica) {
				continue
			}
			assignments = append(assignments, types.WALReplicaInfoAssigned{
				Replica: types.WALReplicaInfo{
					ChannelID: types.ChannelID{
						Name:         pchannel,
						WALReplicaID: replica.GetReplicaId(),
					},
					AccessMode:        types.AccessMode(replica.GetAccessMode()),
					ResourceGroup:     replica.GetResourceGroup(),
					PChannelWriteTerm: term,
					AssignmentEpoch:   replica.GetAssignmentEpoch(),
					State:             replica.GetState(),
				},
				Node: types.NewStreamingNodeInfoFromProto(replica.GetActiveNode()),
			})
		}
	}
	sort.Slice(assignments, func(i, j int) bool {
		left := assignments[i]
		right := assignments[j]
		if left.Replica.ChannelID != right.Replica.ChannelID {
			return left.Replica.ChannelID.LT(right.Replica.ChannelID)
		}
		return left.Node.ServerID < right.Node.ServerID
	})
	return assignments
}

func isWALReplicaServiceableForDiscovery(replica *streamingpb.WALReplicaAssignment) bool {
	if replica.GetActiveNode() == nil {
		return false
	}
	switch replica.GetState() {
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED:
		return true
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
		return replica.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY
	default:
		return false
	}
}

func buildShardAssignments(
	assignments []types.PChannelInfoAssigned,
	walReplicaAssignments []types.WALReplicaInfoAssigned,
	provider ShardAssignmentProvider,
) map[int64]types.ShardAssignmentInfo {
	if provider == nil {
		return nil
	}
	byPChannel := provider.ShardAssignmentsByPChannel()
	if len(byPChannel) == 0 {
		return nil
	}

	byNode := make(map[int64]types.ShardAssignmentInfo)
	if len(walReplicaAssignments) > 0 {
		ownerByReplica := make(map[types.ChannelID]int64, len(walReplicaAssignments))
		for _, assignment := range walReplicaAssignments {
			ownerByReplica[assignment.Replica.ChannelID] = assignment.Node.ServerID
		}
		for pchannel, entries := range byPChannel {
			for _, entry := range entries {
				nodeID, ok := ownerByReplica[types.ChannelID{
					Name:         pchannel,
					WALReplicaID: entry.WALReplicaID,
				}]
				if !ok {
					continue
				}
				appendShardAssignmentEntry(byNode, nodeID, pchannel, entry)
			}
		}
		if len(byNode) == 0 {
			return nil
		}
		return byNode
	}

	for _, assignment := range assignments {
		entries := byPChannel[assignment.Channel.Name]
		if len(entries) == 0 {
			continue
		}
		for _, entry := range entries {
			appendShardAssignmentEntry(byNode, assignment.Node.ServerID, assignment.Channel.Name, entry)
		}
	}
	if len(byNode) == 0 {
		return nil
	}
	return byNode
}

func appendShardAssignmentEntry(
	byNode map[int64]types.ShardAssignmentInfo,
	nodeID int64,
	pchannel string,
	entry types.ShardAssignmentEntry,
) {
	nodeAssignment := byNode[nodeID]
	for i := range nodeAssignment.PChannelAssignments {
		if nodeAssignment.PChannelAssignments[i].PChannel == pchannel {
			nodeAssignment.PChannelAssignments[i].Entries = append(nodeAssignment.PChannelAssignments[i].Entries, entry)
			byNode[nodeID] = nodeAssignment
			return
		}
	}
	nodeAssignment.PChannelAssignments = append(nodeAssignment.PChannelAssignments, types.PChannelShardAssignment{
		PChannel: pchannel,
		Entries:  []types.ShardAssignmentEntry{entry},
	})
	byNode[nodeID] = nodeAssignment
}

// waitChanges waits for the layout to be updated.
func (cm *ChannelManager) waitChanges(ctx context.Context, version typeutil.Version) error {
	cm.cond.L.Lock()
	for version.EQ(cm.version) {
		if err := cm.cond.Wait(ctx); err != nil {
			return err
		}
	}
	cm.cond.L.Unlock()
	return nil
}
