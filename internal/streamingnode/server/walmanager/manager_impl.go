package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/partialupdate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var errWALManagerClosed = status.NewOnShutdownError("wal manager is closed")

// OpenManager create a WAL Manager, which now uses dynamic opener that can handle multiple WALNames at runtime.
// The specific WALName will be determined when opening each channel based on checkpoint's MessageID.WALName
func OpenManager() (Manager, error) {
	resource.Resource().Logger().Info(context.TODO(), "open wal manager with dynamic opener")
	// Create dynamic opener directly with interceptors
	opener := adaptor.NewOpenerAdaptor(newInterceptorBuilders())
	return newManager(opener), nil
}

// newInterceptorBuilders keeps shard validation ahead of partial-update write
// tracking while both remain inside the TimeTick publication boundary.
func newInterceptorBuilders() []interceptors.InterceptorBuilder {
	return []interceptors.InterceptorBuilder{
		redo.NewInterceptorBuilder(),
		lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
		shard.NewInterceptorBuilder(),
		partialupdate.NewInterceptorBuilder(),
	}
}

// newManager create a wal manager.
func newManager(opener wal.Opener) Manager {
	return &managerImpl{
		lifetime: typeutil.NewGenericLifetime[managerState](managerOpenable | managerRemoveable | managerGetable),
		wltMap:   typeutil.NewConcurrentMap[types.ChannelID, *walLifetime](),
		opener:   opener,
		logger:   resource.Resource().Logger().With(mlog.FieldComponent("wal-manager")),
	}
}

// All management operation for a wal will be serialized with order of term.
type managerImpl struct {
	lifetime *typeutil.GenericLifetime[managerState]

	wltMap *typeutil.ConcurrentMap[types.ChannelID, *walLifetime]
	opener wal.Opener // wal allocator
	logger *mlog.Logger
}

// Open opens a wal instance for the channel on this Manager.
func (m *managerImpl) Open(ctx context.Context, channel types.PChannelInfo) (err error) {
	return m.OpenWALReplica(ctx, channel, 0, 0)
}

// OpenWALReplica opens a wal replica instance for the channel on this Manager.
func (m *managerImpl) OpenWALReplica(ctx context.Context, channel types.PChannelInfo, walReplicaID int64, assignmentEpoch int64) (err error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isOpenable) {
		return errWALManagerClosed
	}
	channelID := walReplicaChannelID(channel, walReplicaID)
	defer func() {
		m.lifetime.Done()
		if err != nil {
			m.logger.Warn(ctx, "open wal failed", mlog.Err(err), mlog.String("channel", channel.String()), mlog.String("channelID", channelID.String()))
			return
		}
		m.logger.Info(ctx, "open wal success", mlog.String("channel", channel.String()), mlog.String("channelID", channelID.String()))
	}()

	return m.getWALLifetime(channelID).Open(ctx, channel, walReplicaID, assignmentEpoch)
}

// Remove removes the wal instance for the channel.
func (m *managerImpl) Remove(ctx context.Context, channel types.PChannelInfo) (err error) {
	return m.RemoveWALReplica(ctx, channel, 0, 0)
}

// RemoveWALReplica removes the wal replica instance for the channel.
func (m *managerImpl) RemoveWALReplica(ctx context.Context, channel types.PChannelInfo, walReplicaID int64, assignmentEpoch int64) (err error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isRemoveable) {
		return errWALManagerClosed
	}
	channelID := walReplicaChannelID(channel, walReplicaID)
	defer func() {
		m.lifetime.Done()
		if err != nil {
			m.logger.Warn(ctx, "remove wal failed", mlog.Err(err), mlog.String("channel", channel.Name), mlog.Int64("term", channel.Term), mlog.String("channelID", channelID.String()))
			return
		}
		m.logger.Info(ctx, "remove wal success", mlog.String("channel", channel.Name), mlog.Int64("term", channel.Term), mlog.String("channelID", channelID.String()))
	}()

	return m.getWALLifetime(channelID).Remove(ctx, channel.Term, assignmentEpoch)
}

// GetAvailableWAL returns a available wal instance for the channel.
// Return nil if the wal instance is not found.
func (m *managerImpl) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	return m.GetAvailableWALReplica(channel, 0)
}

// GetAvailableWALReplica returns a available wal replica instance for the channel.
// Return nil if the wal instance is not found.
func (m *managerImpl) GetAvailableWALReplica(channel types.PChannelInfo, walReplicaID int64) (wal.WAL, error) {
	// reject operation if manager is closing.
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()

	l := m.getWALLifetime(walReplicaChannelID(channel, walReplicaID)).GetWAL()
	if l == nil || !l.IsAvailable() {
		return nil, status.NewChannelNotExist(channel.Name)
	}

	currentChannel := l.Channel()
	currentTerm := currentChannel.Term
	if currentTerm != channel.Term && !isReadOnlyWALReplicaTermCompatible(channel, currentChannel) {
		return nil, status.NewUnmatchedChannelTerm(channel.Name, channel.Term, currentTerm)
	}
	if currentChannel.AccessMode != channel.AccessMode {
		return nil, status.NewChannelNotExist(channel.Name)
	}
	// wal's lifetime is fully managed by wal manager,
	// so wrap the wal instance to prevent it from being closed by other components.
	return nopCloseWAL{l}, nil
}

func isReadOnlyWALReplicaTermCompatible(requested types.PChannelInfo, current types.PChannelInfo) bool {
	return requested.AccessMode == types.AccessModeRO && current.AccessMode == types.AccessModeRO
}

// GetAvailableRawWALByPChannel returns the available raw wal instance for the pchannel.
func (m *managerImpl) GetAvailableRawWALByPChannel(pchannel string) (wal.WAL, error) {
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()

	l := m.getWALLifetime(types.ChannelID{Name: pchannel}).GetWAL()
	if l == nil || !l.IsAvailable() {
		return nil, status.NewChannelNotExist(pchannel)
	}
	return l, nil
}

func (m *managerImpl) Metrics() (*types.StreamingNodeMetrics, error) {
	if !m.lifetime.AddIf(isGetable) {
		return nil, errWALManagerClosed
	}
	defer m.lifetime.Done()

	metrics := make(map[types.ChannelID]types.WALMetrics)
	m.wltMap.Range(func(channelID types.ChannelID, lt *walLifetime) bool {
		if l := lt.GetWAL(); l != nil {
			metrics[channelID] = l.Metrics()
		}
		return true
	})
	return &types.StreamingNodeMetrics{
		WALMetrics: metrics,
	}, nil
}

// Close these manager and release all managed WAL.
func (m *managerImpl) Close() {
	m.lifetime.SetState(managerRemoveable)
	m.lifetime.Wait()
	// close all underlying walLifetime.
	m.wltMap.Range(func(channelID types.ChannelID, wlt *walLifetime) bool {
		wlt.Close()
		return true
	})
	m.lifetime.SetState(managerStopped)
	m.lifetime.Wait()

	// close all underlying wal instance by allocator if there's resource leak.
	m.opener.Close()
}

// getWALLifetime returns the wal lifetime for the channel.
func (m *managerImpl) getWALLifetime(channelID types.ChannelID) *walLifetime {
	if wlt, loaded := m.wltMap.Get(channelID); loaded {
		return wlt
	}

	// Perform a cas here.
	newWLT := newWALLifetime(m.opener, channelID.String(), m.logger)
	wlt, loaded := m.wltMap.GetOrInsert(channelID, newWLT)
	// if loaded, lifetime is exist, close the redundant lifetime.
	if loaded {
		newWLT.Close()
	}
	return wlt
}

func walReplicaChannelID(channel types.PChannelInfo, walReplicaID int64) types.ChannelID {
	return types.ChannelID{
		Name:         channel.Name,
		WALReplicaID: walReplicaID,
	}
}

type managerState int32

const (
	managerStopped    managerState = 0
	managerOpenable   managerState = 0x1
	managerRemoveable managerState = 0x1 << 1
	managerGetable    managerState = 0x1 << 2
)

func isGetable(state managerState) bool {
	return state&managerGetable != 0
}

func isRemoveable(state managerState) bool {
	return state&managerRemoveable != 0
}

func isOpenable(state managerState) bool {
	return state&managerOpenable != 0
}

// wal can be only closed by the wal manager.
// So wrap the wal instance to prevent it from being closed by other components.
type nopCloseWAL struct {
	wal.WAL
}

func (w nopCloseWAL) UnwrapWAL() wal.WAL {
	return w.WAL
}

func (w nopCloseWAL) Close() {
	// do nothing
}
