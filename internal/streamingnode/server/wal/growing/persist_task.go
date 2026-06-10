package growing

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type recoveryCatalog interface {
	SaveVChannels(ctx context.Context, pchannel string, vchannels map[string]*streamingpb.VChannelMeta) error
	DropVChannels(ctx context.Context, pchannel string, vchannels map[string]*streamingpb.VChannelMeta) error
	SaveTransformLogMeta(ctx context.Context, pchannel string, metas map[string]*streamingpb.VChannelTransformLogMeta) error
	DropTransformLogMeta(ctx context.Context, pchannel string, vchannels []string) error
	SaveSegmentAssignments(ctx context.Context, pchannel string, segments map[int64]*streamingpb.SegmentAssignmentMeta) error
	DropSegmentAssignments(ctx context.Context, pchannel string, segmentIDs []int64) error
}

type dirtySnapshot struct {
	VChannels          map[string]*streamingpb.VChannelMeta
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
	TransformLogs      map[string]*streamingpb.VChannelTransformLogMeta
	vchannelOwners     map[string]*vChannelView
	segmentOwners      map[int64]*segmentView
	transformLogOwners map[string]*transformLogView
}

type dirtyOwners struct {
	vchannelOwners     map[string]*vChannelView
	segmentOwners      map[int64]*segmentView
	transformLogOwners map[string]*transformLogView
}

func (s *dirtySnapshot) empty() bool {
	return s == nil || (len(s.VChannels) == 0 && len(s.SegmentAssignments) == 0 && len(s.TransformLogs) == 0)
}

func (s *dirtyOwners) empty() bool {
	return s == nil || (len(s.vchannelOwners) == 0 && len(s.segmentOwners) == 0 && len(s.transformLogOwners) == 0)
}

func (m *Manager) newPersistTask(
	channelName string,
	catalog recoveryCatalog,
	logger *log.MLogger,
	precondition preconditioned.Precondition,
	onPersisted func(),
) preconditioned.Task {
	if !m.hasPendingPersistWork() {
		return nil
	}
	return &persistTask{
		channelName:  channelName,
		catalog:      catalog,
		logger:       logger,
		manager:      m,
		precondition: precondition,
		onPersisted:  onPersisted,
	}
}

func (m *Manager) hasPendingPersistWork() bool {
	if !m.collectDirtyOwners().empty() {
		return true
	}
	for _, segment := range m.segmentViews {
		if segment.HasReadyTombstoneFinalize() {
			return true
		}
	}
	for _, vchannel := range m.vchannelViews {
		if vchannel.HasReadyTombstoneFinalize() {
			return true
		}
	}
	return false
}

func (m *Manager) collectDirtyOwners() *dirtyOwners {
	segmentOwners := make(map[int64]*segmentView)
	vchannelOwners := make(map[string]*vChannelView)
	transformLogOwners := make(map[string]*transformLogView)
	for _, segment := range m.segmentViews {
		if segment.HasDirty() {
			segmentOwners[segment.ID()] = segment
		}
	}
	for _, vchannel := range m.vchannelViews {
		if vchannel.HasDirty() {
			vchannelOwners[vchannel.Name()] = vchannel
		}
	}
	for vchannel, transformLog := range m.transformLogs {
		if transformLog.log.HasDirty() {
			transformLogOwners[vchannel] = transformLog
		}
	}
	return &dirtyOwners{
		vchannelOwners:     vchannelOwners,
		segmentOwners:      segmentOwners,
		transformLogOwners: transformLogOwners,
	}
}

func (s *dirtyOwners) consumeSnapshot() *dirtySnapshot {
	if s.empty() {
		return nil
	}
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	transformLogs := make(map[string]*streamingpb.VChannelTransformLogMeta)
	segmentOwners := make(map[int64]*segmentView)
	vchannelOwners := make(map[string]*vChannelView)
	transformLogOwners := make(map[string]*transformLogView)
	for segmentID, segment := range s.segmentOwners {
		dirtySnapshot := segment.ConsumeDirtyAndGetSnapshot()
		if dirtySnapshot != nil {
			segments[segmentID] = dirtySnapshot
			segmentOwners[segmentID] = segment
		}
	}
	for vchannelName, vchannel := range s.vchannelOwners {
		dirtySnapshot := vchannel.ConsumeDirtyAndGetSnapshot()
		if dirtySnapshot != nil {
			vchannels[vchannelName] = dirtySnapshot
			vchannelOwners[vchannelName] = vchannel
		}
	}
	for vchannel, transformLog := range s.transformLogOwners {
		dirtySnapshot := transformLog.log.ConsumeDirtyAndGetSnapshot()
		if dirtySnapshot != nil {
			transformLogs[vchannel] = dirtySnapshot
			transformLogOwners[vchannel] = transformLog
		}
	}
	snapshot := &dirtySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		TransformLogs:      transformLogs,
		vchannelOwners:     vchannelOwners,
		segmentOwners:      segmentOwners,
		transformLogOwners: transformLogOwners,
	}
	if snapshot.empty() {
		return nil
	}
	return snapshot
}

func (m *Manager) markSnapshotPersisted(snapshot *dirtySnapshot) {
	if snapshot == nil {
		return
	}
	for segmentID, meta := range snapshot.SegmentAssignments {
		if owner := snapshot.segmentOwners[segmentID]; owner != nil {
			owner.MarkSnapshotPersisted(meta)
		}
	}
	for vchannel, meta := range snapshot.VChannels {
		if owner := snapshot.vchannelOwners[vchannel]; owner != nil {
			owner.MarkSnapshotPersisted(meta)
		}
	}
	for vchannel, meta := range snapshot.TransformLogs {
		if owner := snapshot.transformLogOwners[vchannel]; owner != nil {
			owner.log.MarkSnapshotPersisted(meta)
		}
	}
}

type persistTask struct {
	channelName  string
	catalog      recoveryCatalog
	logger       *log.MLogger
	manager      *Manager
	precondition preconditioned.Precondition
	onPersisted  func()
}

func (t *persistTask) Name() string {
	return "growing-persist"
}

func (t *persistTask) Precondition() preconditioned.Precondition {
	return t.precondition
}

func (t *persistTask) Run(ctx context.Context) error {
	owners := t.manager.collectDirtyOwners()
	if owners.empty() {
		t.manager.finalizeTombstones()
		owners = t.manager.collectDirtyOwners()
	}
	snapshot := owners.consumeSnapshot()
	if snapshot.empty() {
		return nil
	}
	logger := t.logger
	if logger == nil {
		logger = log.Ctx(ctx)
	}
	logger = logger.With(
		zap.String("op", "persistGrowing"),
		zap.String("channel", t.channelName),
		zap.Int("vchannelCount", len(snapshot.VChannels)),
		zap.Int("segmentCount", len(snapshot.SegmentAssignments)),
		zap.Int("transformLogCount", len(snapshot.TransformLogs)),
	)
	for {
		if err := t.persistSnapshot(ctx, logger, snapshot); err != nil {
			return err
		}
		t.manager.markSnapshotPersisted(snapshot)
		snapshot = t.manager.collectDirtyOwners().consumeSnapshot()
		if snapshot.empty() {
			break
		}
	}
	if t.manager.finalizeTombstones() {
		t.manager.RequirePersist()
	}
	if t.onPersisted != nil {
		t.onPersisted()
	}
	return nil
}

func (t *persistTask) persistSnapshot(ctx context.Context, logger *log.MLogger, snapshot *dirtySnapshot) error {
	if len(snapshot.TransformLogs) > 0 {
		if err := retryOperationWithBackoff(ctx,
			logger.With(zap.String("op", "persistTransformLogs"), zap.Strings("vchannels", lo.Keys(snapshot.TransformLogs))),
			func(ctx context.Context) error {
				return t.catalog.SaveTransformLogMeta(ctx, t.channelName, snapshot.TransformLogs)
			}); err != nil {
			return err
		}
	}
	if len(snapshot.VChannels) > 0 {
		if err := retryOperationWithBackoff(ctx,
			logger.With(zap.String("op", "persistVChannels"), zap.Strings("vchannels", lo.Keys(snapshot.VChannels))),
			func(ctx context.Context) error {
				return t.catalog.SaveVChannels(ctx, t.channelName, snapshot.VChannels)
			}); err != nil {
			return err
		}
	}
	if len(snapshot.SegmentAssignments) > 0 {
		if err := retryOperationWithBackoff(ctx,
			logger.With(zap.String("op", "persistSegmentAssignments"), zap.Int64s("segmentIds", lo.Keys(snapshot.SegmentAssignments))),
			func(ctx context.Context) error {
				return t.catalog.SaveSegmentAssignments(ctx, t.channelName, snapshot.SegmentAssignments)
			}); err != nil {
			return err
		}
	}
	return nil
}

func retryOperationWithBackoff(ctx context.Context, logger *log.MLogger, op func(ctx context.Context) error) error {
	backoff := newBackoff()
	for {
		err := op(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		nextInterval := backoff.NextBackOff()
		logger.Warn("failed to persist operation, wait for retry...", zap.Duration("nextRetryInterval", nextInterval), zap.Error(err))
		select {
		case <-time.After(nextInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	return backoff
}
