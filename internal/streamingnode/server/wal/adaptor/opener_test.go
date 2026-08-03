package adaptor

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenerAdaptorFailure(t *testing.T) {
	basicOpener := mock_walimpls.NewMockOpenerImpls(t)
	errExpected := errors.New("test")
	basicOpener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, boo *walimpls.OpenOption) (walimpls.WALImpls, error) {
		return nil, errExpected
	})

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	opener := adaptImplsToOpener(basicOpener, nil)
	l, err := opener.Open(context.Background(), &wal.OpenOption{})
	assert.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)
}

func TestOpenRWWALCleansRecoveredShardManagerOnReplicateRecoveryFailure(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "replicate-recovery-failure-cleanup",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	walImpls := &recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}
	resMgr, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{PChannel: channel.Name})
	require.NoError(t, err)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().Close().Return().Once()
	rs.EXPECT().VChannelManager().Return(resMgr).Once()
	snapshot := &recovery.RecoverySnapshot{
		VChannels:          map[string]*streamingpb.VChannelMeta{},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
		TxnBuffer: utility.NewTxnBuffer(
			mlog.With(),
			metricsutil.NewScanMetrics(channel).NewScannerMetrics(),
		),
	}
	mockRecoverStorage := mockey.Mock(recovery.RecoverRecoveryStorage).
		Return(rs, snapshot, nil).
		Build()
	defer mockRecoverStorage.UnPatch()

	errExpected := errors.New("replicate recovery failed")
	mockRecoverReplicateManager := mockey.Mock(replicates.RecoverReplicateManager).
		Return(nil, errExpected).
		Build()
	defer mockRecoverReplicateManager.UnPatch()

	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	l, err := opener.openRWWAL(context.Background(), walImpls, &wal.OpenOption{Channel: channel})
	require.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)

	sealOperator := mock_utils.NewMockSealOperator(t)
	sealOperator.EXPECT().Channel().Return(channel).Maybe()
	registered := assert.NotPanics(t, func() {
		resource.Resource().SegmentStatsManager().RegisterSealOperator(sealOperator, nil, nil)
	})
	if registered {
		resource.Resource().SegmentStatsManager().UnregisterSealOperator(sealOperator)
	}
}

func TestOpenRWWALFiltersRecoveredQueryViewsByWALReplica(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "rw-filter-query-view",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	view1 := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp).IntoProto()
	view1.StreamingNode.WalReplicaId = 1
	view2 := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp).IntoProto()
	view2.StreamingNode.WalReplicaId = 2
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return([]*viewpb.QueryViewOfShard{view1, view2}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	walImpls := &recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}
	resMgr, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{PChannel: channel.Name})
	require.NoError(t, err)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().Close().Return().Once()
	rs.EXPECT().VChannelManager().Return(resMgr).Once()
	snapshot := &recovery.RecoverySnapshot{
		VChannels:          map[string]*streamingpb.VChannelMeta{},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
		TxnBuffer: utility.NewTxnBuffer(
			mlog.With(),
			metricsutil.NewScanMetrics(channel).NewScannerMetrics(),
		),
	}
	mockRecoverStorage := mockey.Mock(recovery.RecoverRecoveryStorage).
		Return(rs, snapshot, nil).
		Build()
	defer mockRecoverStorage.UnPatch()

	emptyHandler := snview.RecoverPChannelSNQueryViewHandler(channel.Name, catalog, resMgr, nil)
	var recoveredViews []*viewpb.QueryViewOfShard
	mockRecoverQueryViews := mockey.Mock(snview.RecoverPChannelSNQueryViewHandler).
		To(func(_ string, _ metastore.StreamingNodeCataLog, _ snview.StreamingNodeResourceManager, views []*viewpb.QueryViewOfShard) *snview.SNQueryViewHandler {
			recoveredViews = views
			return emptyHandler
		}).
		Build()
	defer mockRecoverQueryViews.UnPatch()

	errExpected := errors.New("replicate recovery failed")
	mockRecoverReplicateManager := mockey.Mock(replicates.RecoverReplicateManager).
		Return(nil, errExpected).
		Build()
	defer mockRecoverReplicateManager.UnPatch()

	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	opened, err := opener.openRWWAL(context.Background(), walImpls, &wal.OpenOption{Channel: channel, WALReplicaID: 1})

	require.ErrorIs(t, err, errExpected)
	assert.Nil(t, opened)
	require.Len(t, recoveredViews, 1)
	assert.Equal(t, int64(1), recoveredViews[0].GetStreamingNode().GetWalReplicaId())
}

func TestOpenRWWALWaitsForRecoveredQueryViewResources(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "by-dev-rootcoord-dml_0",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	persistedView := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp).IntoProto()
	persistedView.StreamingNode.WalReplicaId = 2
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return([]*viewpb.QueryViewOfShard{
		persistedView,
	}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	walImpls := &recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}
	releasePrepare := make(chan struct{})
	prepareStarted := make(chan struct{}, 1)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	resMgr, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: channel.Name,
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			queryPlanTestVChannel: newOpenROTestVChannelMeta(),
		},
		QueryRuntimeModuleBuilders: []queryresource.QueryRuntimeModuleBuilder{
			blockingQueryRuntimeModuleBuilderForOpenTest{
				started: prepareStarted,
				release: releasePrepare,
			},
		},
		NodeScheduler: scheduler,
	})
	require.NoError(t, err)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().Close().Return().Once()
	rs.EXPECT().VChannelManager().Return(resMgr).Once()
	snapshot := &recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			queryPlanTestVChannel: newOpenROTestVChannelMeta(),
		},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
		TxnBuffer: utility.NewTxnBuffer(
			mlog.With(),
			metricsutil.NewScanMetrics(channel).NewScannerMetrics(),
		),
	}
	mockRecoverStorage := mockey.Mock(recovery.RecoverRecoveryStorage).
		Return(rs, snapshot, nil).
		Build()
	defer mockRecoverStorage.UnPatch()

	replicateCalled := make(chan struct{})
	errExpected := errors.New("replicate recovery failed after query view recovery")
	mockRecoverReplicateManager := mockey.Mock(replicates.RecoverReplicateManager).
		To(func(*replicates.ReplicateManagerRecoverParam) (replicates.ReplicatesManager, error) {
			close(replicateCalled)
			return nil, errExpected
		}).
		Build()
	defer mockRecoverReplicateManager.UnPatch()

	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	done := make(chan error, 1)
	go func() {
		opened, err := opener.openRWWAL(context.Background(), walImpls, &wal.OpenOption{Channel: channel, WALReplicaID: 2})
		if opened != nil {
			opened.Close()
		}
		done <- err
	}()

	require.Eventually(t, func() bool {
		select {
		case <-prepareStarted:
			return true
		case <-replicateCalled:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	select {
	case <-replicateCalled:
		t.Fatal("openRWWAL should wait for recovered query view resources before continuing")
	default:
	}

	close(releasePrepare)
	require.ErrorIs(t, <-done, errExpected)
}

func TestOpenROWALRecoversQueryViewHandlerAndVChannelModules(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "by-dev-rootcoord-dml_0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}
	schema := queryPlanTestSchema()
	persistedView := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp).IntoProto()
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListVChannel(mock.Anything, channel.Name).Return([]*streamingpb.VChannelMeta{
		{
			Vchannel: queryPlanTestVChannel,
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 10,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 10,
						State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
					},
					{
						PartitionId: 20,
						State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             schema,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
			CheckpointTimeTick: 1,
		},
	}, nil)
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListSegmentDataVersionSummaries(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListTransformLogMeta(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return([]*viewpb.QueryViewOfShard{
		persistedView,
	}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	opened, err := opener.openROWAL(context.Background(), &recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Fatal("read-only WAL must not append recovery barrier")
			return nil, nil
		},
	}, &wal.OpenOption{Channel: channel, WALReplicaID: 2})

	require.NoError(t, err)
	defer opened.Close()
	roWAL := opened.(*roWALAdaptorImpl)
	require.NotNil(t, roWAL.QueryViewHandler())
	require.NotNil(t, roWAL.viewResourceManager)
	assert.NotNil(t, roWAL.viewResourceManager.Module(queryPlanTestVChannel))
	key := shard.WALReplicaFunctionRunnerKey(queryPlanTestVChannel, 2)
	ok, err := function.GetManager().RunWithRunner(context.Background(), 10, key, firstVectorFieldID(t, schema), func(function.FunctionRunner) error {
		t.Fatal("plain vector field should not have a function runner")
		return nil
	})
	require.NoError(t, err)
	assert.False(t, ok)

	stream, err := roWAL.TransformLog().AcquireStream(context.Background(), channel.Name, 2)
	require.NoError(t, err)
	defer stream.Close()

	handler := newOpenROTransformLogHandler()
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           queryPlanTestVChannel,
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)
	defer sub.Close()
	assert.Eventually(t, func() bool {
		select {
		case event := <-handler.events:
			return event.SyncUp != nil
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func firstVectorFieldID(t *testing.T, schema *schemapb.CollectionSchema) int64 {
	t.Helper()
	for _, field := range schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_FloatVector ||
			field.GetDataType() == schemapb.DataType_BinaryVector ||
			field.GetDataType() == schemapb.DataType_Float16Vector ||
			field.GetDataType() == schemapb.DataType_BFloat16Vector ||
			field.GetDataType() == schemapb.DataType_SparseFloatVector ||
			field.GetDataType() == schemapb.DataType_Int8Vector {
			return field.GetFieldID()
		}
	}
	t.Fatalf("schema %s has no vector field", schema.GetName())
	return 0
}

func TestOpenROWALTailsPChannelWALForLocalQueryResources(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "by-dev-rootcoord-dml_0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(&streamingpb.WALCheckpoint{
		MessageId: walimplstest.NewTestMessageID(1).IntoProto(),
		TimeTick:  1,
		DataCheckpoint: &streamingpb.WALConsumeCheckpoint{
			MessageId: walimplstest.NewTestMessageID(1).IntoProto(),
			TimeTick:  1,
		},
	}, nil)
	catalog.EXPECT().ListVChannel(mock.Anything, channel.Name).Return([]*streamingpb.VChannelMeta{
		newOpenROTestVChannelMeta(),
	}, nil)
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListSegmentDataVersionSummaries(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListTransformLogMeta(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	partitionMsg := newOpenROTestCreatePartitionMessage(t, queryPlanTestVChannel, 30, 20)
	timeTickMsg := message.CreateTestTimeTickSyncMessage(t, 10, 20, walimplstest.NewTestMessageID(20)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(21))
	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	opened, err := opener.openROWAL(context.Background(), &readOnlyScannerWALImpls{
		channel: channel,
		msgs:    []message.ImmutableMessage{partitionMsg, timeTickMsg},
	}, &wal.OpenOption{Channel: channel, WALReplicaID: 2})
	require.NoError(t, err)
	defer opened.Close()

	roWAL := opened.(*roWALAdaptorImpl)
	assert.Eventually(t, func() bool {
		snapshot := roWAL.viewResourceManager.SwitchIntoMetaAndData()
		composite, ok := snapshot.(moduleapi.CompositeModuleSnapshot)
		if !ok {
			return false
		}
		for _, item := range composite {
			vchannelSnapshot, ok := item.(*moduleapi.VChannelModuleSnapshot)
			if !ok {
				continue
			}
			meta := vchannelSnapshot.VChannels[queryPlanTestVChannel]
			for _, partition := range meta.GetCollectionInfo().GetPartitions() {
				if partition.GetPartitionId() == 30 {
					return true
				}
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

func TestOpenROWALBuildsQueryRuntimeFromReplayedTransformBarrier(t *testing.T) {
	initCoreForOpenROQueryRuntimeTest(t)

	channel := types.PChannelInfo{
		Name:       "by-dev-rootcoord-dml_0",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(&streamingpb.WALCheckpoint{
		MessageId: walimplstest.NewTestMessageID(1).IntoProto(),
		TimeTick:  1,
		DataCheckpoint: &streamingpb.WALConsumeCheckpoint{
			MessageId: walimplstest.NewTestMessageID(1).IntoProto(),
			TimeTick:  1,
		},
	}, nil)
	catalog.EXPECT().ListVChannel(mock.Anything, channel.Name).Return([]*streamingpb.VChannelMeta{
		newOpenROTestVChannelMeta(),
	}, nil)
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListSegmentDataVersionSummaries(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListTransformLogMeta(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	createSegment := newOpenROTestCreateSegmentMessage(t, queryPlanTestVChannel, 100, 10)
	insert := newOpenROTestInsertMessage(t, queryPlanTestVChannel, 100, 20)
	manualFlush := newOpenROTestManualFlushMessage(t, queryPlanTestVChannel, 30)
	timeTickMsg := message.CreateTestTimeTickSyncMessage(t, 10, 30, walimplstest.NewTestMessageID(30)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(31))
	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	opened, err := opener.openROWAL(context.Background(), &readOnlyScannerWALImpls{
		channel: channel,
		msgs:    []message.ImmutableMessage{createSegment, insert, manualFlush, timeTickMsg},
	}, &wal.OpenOption{Channel: channel, WALReplicaID: 2})
	require.NoError(t, err)
	defer opened.Close()

	roWAL := opened.(*roWALAdaptorImpl)
	stream, err := roWAL.TransformLog().AcquireStream(context.Background(), channel.Name, 2)
	require.NoError(t, err)
	defer stream.Close()
	handler := newOpenROTransformLogHandler()
	sub, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
		VChannel:           queryPlanTestVChannel,
		StartAfterTimeTick: 0,
		Handler:            handler,
	})
	require.NoError(t, err)
	defer sub.Close()
	assert.Eventually(t, func() bool {
		select {
		case event := <-handler.events:
			return event.SyncUp != nil && event.SyncUp.TimeTick >= 30
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	view := newQueryPlanTestView(viewpb.QueryViewState_QueryViewStateUp)
	key := view.QueryViewKey()
	ready := make(chan struct{}, 1)
	roWAL.viewResourceManager.Acquire(snview.AcquireResource{
		Key:  key,
		Meta: view.IntoProto().GetMeta(),
		OnReady: func() {
			ready <- struct{}{}
		},
	})
	require.Eventually(t, func() bool {
		select {
		case <-ready:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	runtime, ok := roWAL.viewResourceManager.GetQueryRuntime(key)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, runtime.WaitMVCCVisible(ctx, 0, 30))
}

func initCoreForOpenROQueryRuntimeTest(t *testing.T) {
	t.Helper()
	initcore.InitExecExpressionFunctionFactory()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
}

type readOnlyScannerWALImpls struct {
	channel types.PChannelInfo
	msgs    []message.ImmutableMessage
}

func (w *readOnlyScannerWALImpls) WALName() message.WALName {
	return message.WALNameTest
}

func (w *readOnlyScannerWALImpls) Channel() types.PChannelInfo {
	return w.channel
}

func (w *readOnlyScannerWALImpls) Close() {
}

func (w *readOnlyScannerWALImpls) Append(context.Context, message.MutableMessage) (message.MessageID, error) {
	panic("read-only scanner wal should not append")
}

func (w *readOnlyScannerWALImpls) Read(context.Context, walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	ch := make(chan message.ImmutableMessage, len(w.msgs))
	for _, msg := range w.msgs {
		ch <- msg
	}
	return blockingMessageScanner{ch: ch}, nil
}

func (w *readOnlyScannerWALImpls) Truncate(context.Context, message.MessageID) error {
	panic("read-only scanner wal should not truncate")
}

type blockingMessageScanner struct {
	ch <-chan message.ImmutableMessage
}

func (s blockingMessageScanner) Name() string {
	return "open-ro-test-scanner"
}

func (s blockingMessageScanner) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

func (s blockingMessageScanner) Error() error {
	return nil
}

func (s blockingMessageScanner) Done() <-chan struct{} {
	return make(chan struct{})
}

func (s blockingMessageScanner) Close() error {
	return nil
}

type openROTransformLogHandler struct {
	events    chan wal.TransformLogStreamEvent
	closed    chan struct{}
	closeOnce sync.Once
}

func newOpenROTransformLogHandler() *openROTransformLogHandler {
	return &openROTransformLogHandler{
		events: make(chan wal.TransformLogStreamEvent, 8),
		closed: make(chan struct{}),
	}
}

func (h *openROTransformLogHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.events <- event
	return nil
}

func (h *openROTransformLogHandler) Close() {
	h.closeOnce.Do(func() {
		close(h.closed)
	})
}

type blockingQueryRuntimeModuleBuilderForOpenTest struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (b blockingQueryRuntimeModuleBuilderForOpenTest) NewRuntime() (queryresource.QueryRuntimeModule, error) {
	return &blockingQueryRuntimeModuleForOpenTest{
		started: b.started,
		release: b.release,
	}, nil
}

type blockingQueryRuntimeModuleForOpenTest struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (m *blockingQueryRuntimeModuleForOpenTest) Prepare(ctx context.Context, _ walview.VChannelWALView) error {
	select {
	case m.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.release:
		return nil
	}
}

func (*blockingQueryRuntimeModuleForOpenTest) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {
}
func (*blockingQueryRuntimeModuleForOpenTest) Advance(qviews.DataVersion) {}
func (*blockingQueryRuntimeModuleForOpenTest) Close()                     {}

func newOpenROTestVChannelMeta() *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:           queryPlanTestVChannel,
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 10,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{
					PartitionId: 10,
					State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
				},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             queryPlanTestSchema(),
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				},
			},
		},
	}
}

func newOpenROTestCreatePartitionMessage(t *testing.T, vchannel string, partitionID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 10,
			PartitionId:  partitionID,
		}).
		WithBody(&msgpb.CreatePartitionRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

func newOpenROTestCreateSegmentMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   10,
			PartitionId:    10,
			SegmentId:      segmentID,
			StorageVersion: 1,
			Level:          datapb.SegmentLevel_L1,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

func newOpenROTestInsertMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 10,
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        1,
					BinarySize:  128,
					SegmentAssignment: &message.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

func newOpenROTestManualFlushMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 10,
			SegmentIds:   []int64{100},
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

func TestDetermineLastConfirmedMessageID(t *testing.T) {
	txnBuffer := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
	lastConfirmedMessageID := determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(5), lastConfirmedMessageID)
	beginMsg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1).
		WithTxnContext(message.TxnContext{
			TxnID:     1,
			Keepalive: time.Hour,
		}).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	beginMsg2 := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{
			TxnID:     2,
			Keepalive: time.Hour,
		}).
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(2)).
		IntoImmutableMessage(rmq.NewRmqID(2))

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg2),
	}, 4)

	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(2), lastConfirmedMessageID)

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg),
	}, 4)
	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(1), lastConfirmedMessageID)
}

func TestHandleAlterWALFlushingStageWaitsRecoveryDataCheckpoint(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-flushing-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
			return checkpoint.GetAlterWalState().GetStage() == streamingpb.AlterWALStage_ADVANCE_CHECKPOINT &&
				checkpoint.GetDataCheckpoint().GetTimeTick() == 100 &&
				rmq.NewRmqID(2).EQ(message.MustUnmarshalMessageID(checkpoint.GetDataCheckpoint().GetMessageId()))
		})).
		Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	roWAL := adaptImplsToROWAL(&recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}, func() {})
	rs := &stalledRecoveryStorage{
		checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(2),
			TimeTick:  100,
		},
	}

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  100,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Test,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			},
		},
		AlterWALInfo: &recovery.AlterWALInfo{
			FoundAlterWALMsg: true,
			TargetWALName:    commonpb.WALName_Test,
			AlterWALTs:       100,
		},
	}

	param := &interceptors.InterceptorBuildParam{RecoveryStorage: rs}
	resources := &walOpenResources{roWAL: roWAL, param: param}
	err := (&openerAdaptorImpl{}).handleAlterWALFlushingStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		roWAL,
		rs,
		resources,
		snapshot,
	)

	require.NoError(t, err)
	assert.Equal(t, streamingpb.AlterWALStage_ADVANCE_CHECKPOINT, snapshot.Checkpoint.AlterWalState.Stage)
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	assert.Equal(t, uint64(100), snapshot.Checkpoint.DataCheckpoint.TimeTick)
	assert.True(t, rmq.NewRmqID(2).EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
}

func TestHandleAlterWALAdvanceCheckpointsStageMovesDataCheckpointToNewWAL(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-advance-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().
		ListVChannel(mock.Anything, channel.Name).
		Return(nil, nil)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
			dataCP := checkpoint.GetDataCheckpoint()
			msgID := checkpoint.GetMessageId()
			dataMessageID := dataCP.GetMessageId()
			return checkpoint.GetAlterWalState() == nil &&
				checkpoint.GetTimeTick() == 100 &&
				dataCP.GetTimeTick() == 100 &&
				msgID.GetId() == dataMessageID.GetId() &&
				msgID.GetWALName() == dataMessageID.GetWALName() &&
				msgID.GetWALName() == commonpb.WALName_RocksMQ
		})).
		Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  100,
			DataCheckpoint: &utility.WALConsumeCheckpoint{
				MessageID: rmq.NewRmqID(2),
				TimeTick:  100,
			},
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_RocksMQ,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_ADVANCE_CHECKPOINT,
			},
		},
	}

	err := (&openerAdaptorImpl{}).handleAlterWALAdvanceCheckpointsStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		snapshot,
	)

	require.NoError(t, err)
}

func TestHandleAlterWALFlushingStageTimesOutWhenDataCheckpointStalls(t *testing.T) {
	oldCheckInterval := walSwitchFlushCheckInterval
	oldTimeout := walSwitchFlushTimeout
	walSwitchFlushCheckInterval = 10 * time.Millisecond
	walSwitchFlushTimeout = 30 * time.Millisecond
	defer func() {
		walSwitchFlushCheckInterval = oldCheckInterval
		walSwitchFlushTimeout = oldTimeout
	}()

	channel := types.PChannelInfo{
		Name:       "alter-wal-flushing-timeout-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	rs := &stalledRecoveryStorage{
		checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(2),
			TimeTick:  10,
		},
	}
	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  10,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Test,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			},
		},
		AlterWALInfo: &recovery.AlterWALInfo{
			FoundAlterWALMsg: true,
			TargetWALName:    commonpb.WALName_Test,
			AlterWALTs:       100,
		},
	}
	start := time.Now()
	param := &interceptors.InterceptorBuildParam{RecoveryStorage: rs}
	resources := &walOpenResources{param: param}
	err := (&openerAdaptorImpl{}).handleAlterWALFlushingStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		nil,
		rs,
		resources,
		snapshot,
	)
	resources.Close()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout waiting for flush completion")
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.Equal(t, streamingpb.AlterWALStage_FLUSHING, snapshot.Checkpoint.AlterWalState.Stage)
}

type stalledRecoveryStorage struct {
	checkpoint *recovery.WALCheckpoint
}

func (s *stalledRecoveryStorage) Metrics() recovery.RecoveryMetrics {
	return recovery.RecoveryMetrics{}
}

func (s *stalledRecoveryStorage) GetDataCheckpoint(ctx context.Context) *recovery.WALCheckpoint {
	return s.checkpoint
}

func (s *stalledRecoveryStorage) TransformLog() wal.TransformLogAccesser {
	return wal.NewTransformLogErrorAccesser(errors.New("transform log unavailable"))
}

func (s *stalledRecoveryStorage) VChannelManager() *vchannel.PChannelRecoveryManager {
	return nil
}

func (s *stalledRecoveryStorage) Close() {
}
