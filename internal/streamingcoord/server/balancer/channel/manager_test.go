package channel

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestReplicateRoleDoesNotWaitForChannelManagerLock(t *testing.T) {
	m := &ChannelManager{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	roleCh := make(chan replicateutil.Role, 1)
	go func() {
		roleCh <- m.ReplicateRole()
	}()

	select {
	case role := <-roleCh:
		assert.Equal(t, replicateutil.RolePrimary, role)
	case <-time.After(time.Second):
		t.Fatal("ReplicateRole blocked on the channel manager lock")
	}
}

func TestChannelManager(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, errors.New("recover failure"))
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	m, err := RecoverChannelManager(ctx)
	assert.Nil(t, m)
	assert.Error(t, err)

	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
			},
		}, nil
	})
	m, err = RecoverChannelManager(ctx)
	assert.NotNil(t, m)
	assert.NoError(t, err)

	// Test getClusterChannels and singleton GetClusterChannels.
	cc := m.getClusterChannels()
	assert.Equal(t, []string{"test-channel"}, cc.Channels)
	assert.NotEmpty(t, cc.ControlChannel)
	assert.True(t, strings.HasPrefix(cc.ControlChannel, "test"))
	singletonCC := GetClusterChannels()
	assert.Equal(t, cc, singletonCC)

	// Test update non exist pchannel
	modified, err := m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("non-exist-channel"): {
		Channel: types.PChannelInfo{
			Name:       "non-exist-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.Nil(t, modified)
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("non-exist-channel")})
	assert.ErrorIs(t, err, ErrChannelNotExist)
	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "non-exist-channel",
		Term: 2,
	}})
	assert.ErrorIs(t, err, ErrChannelNotExist)

	// Test success.
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Unset()
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pm []*streamingpb.PChannelMeta) error {
		return nil
	})
	modified, err = m.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	assert.NotNil(t, modified)
	assert.NoError(t, err)
	assert.Len(t, modified, 1)
	err = m.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})
	assert.NoError(t, err)

	nodeID, ok := m.GetLatestWALLocated(ctx, "test-channel")
	assert.True(t, ok)
	assert.NotZero(t, nodeID)

	err = m.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	assert.NoError(t, err)

	view := m.CurrentPChannelsView()
	assert.NotNil(t, view)
	assert.Equal(t, len(view.Channels), 1)
	channel, ok := view.Channels[newChannelID("test-channel")]
	assert.True(t, ok)
	assert.NotNil(t, channel)

	nodeID, ok = m.GetLatestWALLocated(ctx, "test-channel")
	assert.False(t, ok)
	assert.Zero(t, nodeID)

	t.Run("UpdateReplicateConfiguration", func(t *testing.T) {
		param, err := m.GetLatestChannelAssignment()
		oldLocalVersion := param.Version.Local
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// Test update replicate configurations
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
			},
		}
		msg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()

		result := message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}

		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 2)
				for _, task := range replicatingTasks {
					result := result.Results[task.GetSourceChannelName()]
					assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
					assert.Equal(t, result.TimeTick, task.InitializedCheckpoint.TimeTick)
					assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev2", 1))
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev2")
				}
				return nil
			})
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		param, err = m.GetLatestChannelAssignment()
		assert.Equal(t, param.Version.Local, oldLocalVersion+1)
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// test idempotency
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		param, err = m.GetLatestChannelAssignment()
		assert.Equal(t, param.Version.Local, oldLocalVersion+1)
		assert.NoError(t, err)
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// Add more pchannels into existing clusters.
		cfg = &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2", "by-dev-test-channel-3"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2", "by-dev2-test-channel-3"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
			},
		}
		msg = message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2", "by-dev-test-channel-3"}).
			MustBuildBroadcast()
		result = message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
				"by-dev-test-channel-3": {
					MessageID:              walimplstest.NewTestMessageID(5),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(6),
					TimeTick:               1,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 1) // only one new pchannel task for the appended channel.
				for _, task := range replicatingTasks {
					assert.Equal(t, task.GetSourceChannelName(), "by-dev-test-channel-3")
					result := result.Results[task.GetSourceChannelName()]
					assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
					// For pchannel-increasing tasks, TimeTick is decremented by 1 so the CDC scanner
					// includes the AlterReplicateConfig message itself (DeliverFilterTimeTickGT is strict).
					assert.Equal(t, result.TimeTick-1, task.InitializedCheckpoint.TimeTick)
					assert.True(t, task.GetSkipGetReplicateCheckpoint())
					assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev2", 1))
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev2")
				}
				return nil
			})

		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+2)

		// Add new cluster into existing config.
		cfg = &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
				{ClusterId: "by-dev3", Pchannels: []string{"by-dev3-test-channel-1", "by-dev3-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
				{SourceClusterId: "by-dev", TargetClusterId: "by-dev3"},
			},
		}
		msg = message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()
		result = message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 2) // here should be two new incoming tasks.
				for _, task := range replicatingTasks {
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev3")
					result := result.Results[task.GetSourceChannelName()]
					assert.True(t, result.LastConfirmedMessageID.EQ(message.MustUnmarshalMessageID(task.InitializedCheckpoint.MessageId)))
					assert.Equal(t, result.TimeTick, task.InitializedCheckpoint.TimeTick)
					assert.Equal(t, task.GetTargetChannelName(), strings.Replace(task.GetSourceChannelName(), "by-dev", "by-dev3", 1))
					assert.Equal(t, task.GetTargetCluster().GetClusterId(), "by-dev3")
				}
				return nil
			})

		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)

		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+3)
		assert.True(t, proto.Equal(param.ReplicateConfiguration, cfg))
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)

		// switch into secondary
		cfg = &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
				{ClusterId: "by-dev2", Pchannels: []string{"by-dev2-test-channel-1", "by-dev2-test-channel-2"}},
				{ClusterId: "by-dev3", Pchannels: []string{"by-dev3-test-channel-1", "by-dev3-test-channel-2"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev2", TargetClusterId: "by-dev"},
				{SourceClusterId: "by-dev2", TargetClusterId: "by-dev3"},
			},
		}
		msg = message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: cfg,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()
		result = message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(1),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
					TimeTick:               1,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(3),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(4),
					TimeTick:               1,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, cfg))
				assert.Len(t, replicatingTasks, 0) // here should be two new incoming tasks.
				return nil
			})
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)
		err = m.UpdateReplicateConfiguration(ctx, result)
		assert.NoError(t, err)

		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+4)
		assert.True(t, proto.Equal(param.ReplicateConfiguration, cfg))
		assert.Equal(t, m.ReplicateRole(), replicateutil.RoleSecondary)

		// Force promote to standalone primary
		forcePromoteCfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-test-channel-1", "by-dev-test-channel-2"}},
			},
		}
		forcePromoteMsg := message.NewAlterReplicateConfigMessageBuilderV2().
			WithHeader(&message.AlterReplicateConfigMessageHeader{
				ReplicateConfiguration: forcePromoteCfg,
				ForcePromote:           true,
			}).
			WithBody(&message.AlterReplicateConfigMessageBody{}).
			WithBroadcast([]string{"by-dev-test-channel-1", "by-dev-test-channel-2"}).
			MustBuildBroadcast()
		forcePromoteResult := message.BroadcastResultAlterReplicateConfigMessageV2{
			Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(forcePromoteMsg),
			Results: map[string]*message.AppendResult{
				"by-dev-test-channel-1": {
					MessageID:              walimplstest.NewTestMessageID(10),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(11),
					TimeTick:               100,
				},
				"by-dev-test-channel-2": {
					MessageID:              walimplstest.NewTestMessageID(12),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(13),
					TimeTick:               100,
				},
			},
		}
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Unset()
		catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, config *streamingpb.ReplicateConfigurationMeta, replicatingTasks []*streamingpb.ReplicatePChannelMeta) error {
				assert.True(t, proto.Equal(config.ReplicateConfiguration, forcePromoteCfg))
				assert.True(t, config.ForcePromoted)
				assert.Len(t, replicatingTasks, 0) // standalone primary has no replication targets
				return nil
			})
		err = m.UpdateReplicateConfiguration(ctx, forcePromoteResult)
		assert.NoError(t, err)

		param, err = m.GetLatestChannelAssignment()
		assert.NoError(t, err)
		assert.Equal(t, param.Version.Local, oldLocalVersion+5)
		assert.True(t, proto.Equal(param.ReplicateConfiguration, forcePromoteCfg))
		assert.Equal(t, m.ReplicateRole(), replicateutil.RolePrimary)
	})
}

func TestAllocVirtualChannels(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil).Maybe()
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil).Maybe()

	ctx := context.Background()
	newIncomingTopics := util.GetAllTopicsFromConfiguration()
	m, err := RecoverChannelManager(ctx, newIncomingTopics.Collect()...)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	allocVChannels, err := m.AllocVirtualChannels(ctx, AllocVChannelParam{
		CollectionID: 1,
		Num:          256,
	})
	assert.Error(t, err)
	assert.Nil(t, allocVChannels, 0)

	StaticPChannelStatsManager.Get().AddVChannel("by-dev-rootcoord-dml_0_100v0", "by-dev-rootcoord-dml_0_101v0", "by-dev-rootcoord-dml_1_100v1")

	allocVChannels, err = m.AllocVirtualChannels(ctx, AllocVChannelParam{
		CollectionID: 1,
		Num:          4,
	})
	assert.NoError(t, err)
	assert.Len(t, allocVChannels, 4)
	assert.Equal(t, allocVChannels[0], "by-dev-rootcoord-dml_10_1v0")
	assert.Equal(t, allocVChannels[1], "by-dev-rootcoord-dml_11_1v1")
	assert.Equal(t, allocVChannels[2], "by-dev-rootcoord-dml_12_1v2")
	assert.Equal(t, allocVChannels[3], "by-dev-rootcoord-dml_13_1v3")
}

func TestStreamingEnableChecker(t *testing.T) {
	ctx := context.Background()
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))
	// Test recover failure.
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)

	assert.False(t, m.IsStreamingEnabledOnce())

	n := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n)
	assert.NoError(t, n.Context().Err())

	go func() {
		defer n.Finish(struct{}{})
		<-n.Context().Done()
	}()

	err = m.MarkStreamingHasEnabled(ctx)
	assert.NoError(t, err)

	n2 := syncutil.NewAsyncTaskNotifier[struct{}]()
	m.RegisterStreamingEnabledNotifier(n2)
	assert.Error(t, n.Context().Err())
	assert.Error(t, n2.Context().Err())
}

func TestChannelManagerWatch(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Unset()
	catalog.EXPECT().ListPChannel(mock.Anything).RunAndReturn(func(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
		return []*streamingpb.PChannelMeta{
			{
				Channel: &streamingpb.PChannelInfo{
					Name: "test-channel",
					Term: 1,
				},
				Node: &streamingpb.StreamingNodeInfo{
					ServerId: 1,
				},
				State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}, nil
	})
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	manager, err := RecoverChannelManager(context.Background())
	assert.NoError(t, err)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	called := make(chan struct{}, 1)
	go func() {
		defer close(done)
		err := manager.WatchAssignmentResult(ctx, func(param WatchChannelAssignmentsCallbackParam) error {
			select {
			case called <- struct{}{}:
			default:
			}
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	manager.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{newChannelID("test-channel"): {
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	}})
	manager.AssignPChannelsDone(ctx, []ChannelID{newChannelID("test-channel")})

	<-called
	manager.MarkAsUnavailable(ctx, []types.PChannelInfo{{
		Name: "test-channel",
		Term: 2,
	}})
	<-called
	cancel()
	<-done
}

func TestBuildShardAssignmentsMapsPChannelEntriesToOwner(t *testing.T) {
	provider := staticShardAssignmentProvider{
		assignments: map[string][]types.ShardAssignmentEntry{
			"p1": {
				{CollectionID: 100, ShardIndex: 0, ReplicaID: 10},
			},
			"p2": {
				{CollectionID: 101, ShardIndex: 1, ReplicaID: 11},
			},
		},
	}

	assignments := buildShardAssignments([]types.PChannelInfoAssigned{
		{
			Channel: types.PChannelInfo{Name: "p1"},
			Node:    types.StreamingNodeInfo{ServerID: 1},
		},
		{
			Channel: types.PChannelInfo{Name: "p2"},
			Node:    types.StreamingNodeInfo{ServerID: 1},
		},
		{
			Channel: types.PChannelInfo{Name: "p3"},
			Node:    types.StreamingNodeInfo{ServerID: 2},
		},
	}, nil, provider)

	require.Len(t, assignments, 1)
	assert.Equal(t, types.ShardAssignmentInfo{
		PChannelAssignments: []types.PChannelShardAssignment{
			{
				PChannel: "p1",
				Entries: []types.ShardAssignmentEntry{
					{CollectionID: 100, ShardIndex: 0, ReplicaID: 10},
				},
			},
			{
				PChannel: "p2",
				Entries: []types.ShardAssignmentEntry{
					{CollectionID: 101, ShardIndex: 1, ReplicaID: 11},
				},
			},
		},
	}, assignments[1])
}

func TestBuildShardAssignmentsMapsEntriesToWALReplicaOwner(t *testing.T) {
	provider := staticShardAssignmentProvider{
		assignments: map[string][]types.ShardAssignmentEntry{
			"p1": {
				{CollectionID: 100, ShardIndex: 0, ReplicaID: 10, WALReplicaID: 0},
				{CollectionID: 100, ShardIndex: 0, ReplicaID: 20, WALReplicaID: 2},
			},
		},
	}

	assignments := buildShardAssignments([]types.PChannelInfoAssigned{
		{
			Channel:      types.PChannelInfo{Name: "p1"},
			WALReplicaID: 0,
			Node:         types.StreamingNodeInfo{ServerID: 1},
		},
	}, []types.WALReplicaInfoAssigned{
		{
			Replica: types.WALReplicaInfo{
				ChannelID:  types.ChannelID{Name: "p1", WALReplicaID: 0},
				AccessMode: types.AccessModeRW,
			},
			Node: types.StreamingNodeInfo{ServerID: 1},
		},
		{
			Replica: types.WALReplicaInfo{
				ChannelID:  types.ChannelID{Name: "p1", WALReplicaID: 2},
				AccessMode: types.AccessModeRO,
			},
			Node: types.StreamingNodeInfo{ServerID: 2},
		},
	}, provider)

	assert.Equal(t, types.ShardAssignmentInfo{
		PChannelAssignments: []types.PChannelShardAssignment{
			{
				PChannel: "p1",
				Entries: []types.ShardAssignmentEntry{
					{CollectionID: 100, ShardIndex: 0, ReplicaID: 10, WALReplicaID: 0},
				},
			},
		},
	}, assignments[1])
	assert.Equal(t, types.ShardAssignmentInfo{
		PChannelAssignments: []types.PChannelShardAssignment{
			{
				PChannel: "p1",
				Entries: []types.ShardAssignmentEntry{
					{CollectionID: 100, ShardIndex: 0, ReplicaID: 20, WALReplicaID: 2},
				},
			},
		},
	}, assignments[2])
}

func TestBuildWALReplicaAssignmentsPublishesServiceableActiveOwners(t *testing.T) {
	channels := map[ChannelID]*PChannelMeta{
		newChannelID("p1"): newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel:          &streamingpb.PChannelInfo{Name: "p1", Term: 7},
			PrimaryReplicaId: 1,
			NextReplicaId:    5,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 10, Address: "localhost:10"},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:     2,
					AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ResourceGroup: "rg1",
					ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 11, Address: "localhost:11"},
					State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:     3,
					AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ResourceGroup: "rg1",
					ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 12, Address: "localhost:12"},
					TargetNode:    &streamingpb.StreamingNodeInfo{ServerId: 13, Address: "localhost:13"},
					State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
				{
					ReplicaId:  4,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					TargetNode: &streamingpb.StreamingNodeInfo{ServerId: 14, Address: "localhost:14"},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		}, nil),
		newChannelID("p2"): newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel:          &streamingpb.PChannelInfo{Name: "p2", Term: 8},
			PrimaryReplicaId: 0,
			NextReplicaId:    1,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					TargetNode: &streamingpb.StreamingNodeInfo{ServerId: 20, Address: "localhost:20"},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		}, nil),
	}

	assert.ElementsMatch(t, []types.WALReplicaInfoAssigned{
		{
			Replica: types.WALReplicaInfo{
				ChannelID:         types.ChannelID{Name: "p1", WALReplicaID: 1},
				AccessMode:        types.AccessModeRW,
				PChannelWriteTerm: 7,
				State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			Node: types.StreamingNodeInfo{ServerID: 10, Address: "localhost:10"},
		},
		{
			Replica: types.WALReplicaInfo{
				ChannelID:         types.ChannelID{Name: "p1", WALReplicaID: 2},
				AccessMode:        types.AccessModeRO,
				ResourceGroup:     "rg1",
				PChannelWriteTerm: 7,
				State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			Node: types.StreamingNodeInfo{ServerID: 11, Address: "localhost:11"},
		},
		{
			Replica: types.WALReplicaInfo{
				ChannelID:         types.ChannelID{Name: "p1", WALReplicaID: 3},
				AccessMode:        types.AccessModeRO,
				ResourceGroup:     "rg1",
				PChannelWriteTerm: 7,
				State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
			},
			Node: types.StreamingNodeInfo{ServerID: 12, Address: "localhost:12"},
		},
	}, buildWALReplicaAssignments(channels))
}

func TestChannelManagerReassignsNonDefaultPrimaryReplicaFromLegacyPath(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       10,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 6},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 1,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 5},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					AssignmentEpoch: 3,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 6},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
					ResourceGroup:   "rg-secondary",
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		},
	)

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	modified, err := manager.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{
		newChannelID("test-channel"): {
			Channel: types.PChannelInfo{
				Name:       "test-channel",
				Term:       10,
				AccessMode: types.AccessModeRW,
			},
			Node: types.StreamingNodeInfo{ServerID: 5},
		},
	})

	require.NoError(t, err)
	require.Len(t, modified, 1)
	require.NotNil(t, saved)
	view := manager.CurrentPChannelsView()
	pchannel := view.Channels[newChannelID("test-channel")]
	assert.Equal(t, int64(11), pchannel.CurrentTerm())
	assert.Equal(t, int64(1), pchannel.PrimaryReplicaID())
	assert.Equal(t, int64(5), pchannel.CurrentServerID())
	primary, ok := pchannel.WALReplica(1)
	require.True(t, ok)
	assert.Equal(t, int64(6), primary.GetActiveNode().GetServerId())
	assert.Equal(t, int64(5), primary.GetTargetNode().GetServerId())
	assert.Equal(t, int64(4), primary.GetAssignmentEpoch())
	assert.Equal(t, int64(1), saved.GetPrimaryReplicaId())
}

func TestChannelManagerReadOnlyWALReplicaLifecycle(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       1,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved []*streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			saved = append(saved, proto.Clone(metas[0]).(*streamingpb.PChannelMeta))
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	replicaKey, err := manager.CreateReadOnlyWALReplica(ctx, "test-channel", "rg-a")
	require.NoError(t, err)
	assert.Equal(t, ChannelID{Name: "test-channel", WALReplicaID: 1}, replicaKey)
	require.Len(t, saved, 1)
	assert.Len(t, saved[0].GetReplicas(), 2)
	assert.Equal(t, "rg-a", saved[0].GetReplicas()[1].GetResourceGroup())

	updates, err := manager.AssignWALReplicas(ctx, map[ChannelID]types.StreamingNodeInfo{
		replicaKey: {ServerID: 2, Address: "localhost:2"},
	})
	require.NoError(t, err)
	assert.Contains(t, updates, replicaKey)
	require.Len(t, saved, 2)
	assert.Equal(t, int64(1), saved[1].GetChannel().GetTerm())
	assert.Equal(t, int64(2), saved[1].GetReplicas()[1].GetTargetNode().GetServerId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, saved[1].GetReplicas()[1].GetState())

	require.NoError(t, manager.AssignWALReplicasDone(ctx, map[ChannelID]int64{replicaKey: 1}))
	require.Len(t, saved, 3)
	assert.Equal(t, int64(2), saved[2].GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, saved[2].GetReplicas()[1].GetTargetNode())

	assignment, err := manager.GetLatestChannelAssignment()
	require.NoError(t, err)
	assert.Contains(t, assignment.WALReplicaRelations, types.WALReplicaInfoAssigned{
		Replica: types.WALReplicaInfo{
			ChannelID:         replicaKey,
			AccessMode:        types.AccessModeRO,
			ResourceGroup:     "rg-a",
			PChannelWriteTerm: 1,
			AssignmentEpoch:   1,
			State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
		Node: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"},
	})

	require.NoError(t, manager.MarkWALReplicasAsDropping(ctx, []ChannelID{replicaKey}))
	require.Len(t, saved, 4)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, saved[3].GetReplicas()[1].GetState())

	require.NoError(t, manager.RemoveWALReplicas(ctx, []ChannelID{replicaKey}))
	require.Len(t, saved, 5)
	assert.Len(t, saved[4].GetReplicas(), 1)
	assert.Equal(t, int64(0), saved[4].GetReplicas()[0].GetReplicaId())
}

func TestChannelManagerMarksReadOnlyWALReplicaUnavailableWithoutTouchingPrimary(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:       0,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					AssignmentEpoch: 3,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 5,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			require.Len(t, metas, 1)
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	require.NoError(t, manager.MarkWALReplicasAsUnavailable(ctx, []ChannelID{replicaID}, 5))

	require.NotNil(t, saved)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, saved.GetState())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, saved.GetChannel().GetAccessMode())
	assert.Equal(t, int64(7), saved.GetChannel().GetTerm())
	assert.Equal(t, int64(0), saved.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, saved.GetReplicas()[0].GetState())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, saved.GetReplicas()[1].GetState())
	assert.Equal(t, int64(5), saved.GetReplicas()[1].GetAssignmentEpoch())
}

func TestChannelManagerMarksReadOnlyWALReplicaActiveUnavailableDuringAssigning(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:       0,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					AssignmentEpoch: 3,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 5,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2},
					TargetNode:      &streamingpb.StreamingNodeInfo{ServerId: 3},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			require.Len(t, metas, 1)
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	require.NoError(t, manager.MarkWALReplicasAsUnavailable(ctx, []ChannelID{replicaID}, 5))

	require.NotNil(t, saved)
	replica := saved.GetReplicas()[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, replica.GetState())
	assert.Nil(t, replica.GetActiveNode())
	assert.Equal(t, int64(3), replica.GetTargetNode().GetServerId())
	assert.Equal(t, int64(5), replica.GetAssignmentEpoch())

	assignment, err := manager.GetLatestChannelAssignment()
	require.NoError(t, err)
	for _, relation := range assignment.WALReplicaRelations {
		assert.NotEqual(t, replicaID, relation.Replica.ChannelID)
	}
}

func TestChannelManagerRejectsInvalidReadOnlyWALReplicaOperations(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{Name: "test-channel", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	_, err = manager.CreateReadOnlyWALReplica(ctx, "missing-channel", "rg-a")
	assert.ErrorIs(t, err, ErrChannelNotExist)

	_, err = manager.AssignWALReplicas(ctx, map[ChannelID]types.StreamingNodeInfo{
		{Name: "test-channel", WALReplicaID: 100}: {ServerID: 2},
	})
	assert.ErrorIs(t, err, ErrWALReplicaNotExist)

	err = manager.MarkWALReplicasAsDropping(ctx, []ChannelID{{Name: "test-channel"}})
	assert.ErrorIs(t, err, ErrWALReplicaOperationInvalid)
}

func TestChannelManagerRejectsOrdinaryAssignForPrimaryWALReplica(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:  &streamingpb.StreamingNodeInfo{ServerId: 1},
			State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	_, err = manager.AssignWALReplicas(ctx, map[ChannelID]types.StreamingNodeInfo{
		{Name: "test-channel"}: {ServerID: 2},
	})
	assert.ErrorIs(t, err, ErrWALReplicaOperationInvalid)

	view := manager.CurrentPChannelsView()
	pchannel := view.Channels[ChannelID{Name: "test-channel"}]
	assert.Equal(t, int64(7), pchannel.CurrentTerm())
	assert.Equal(t, int64(1), pchannel.CurrentServerID())
}

func TestChannelManagerSwitchWALPrimaryReplica(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel:          &streamingpb.PChannelInfo{Name: "test-channel", Term: 7},
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			require.Len(t, metas, 1)
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	err = manager.SwitchWALPrimaryReplica(ctx, "test-channel", 1)
	require.NoError(t, err)
	require.NotNil(t, saved)
	assert.Equal(t, int64(8), saved.GetChannel().GetTerm())
	assert.Equal(t, int64(1), saved.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, saved.GetState())
	assert.Equal(t, int64(2), saved.GetNode().GetServerId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, saved.GetReplicas()[0].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, saved.GetReplicas()[1].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, saved.GetReplicas()[1].GetState())

	view := manager.CurrentPChannelsView()
	pchannel := view.Channels[ChannelID{Name: "test-channel"}]
	assert.Equal(t, int64(8), pchannel.CurrentTerm())
	assert.Equal(t, int64(1), pchannel.PrimaryReplicaID())
	assert.Equal(t, int64(2), pchannel.CurrentServerID())

	locatedNodeID, ok := manager.GetLatestWALLocated(ctx, "test-channel")
	assert.False(t, ok)
	assert.Zero(t, locatedNodeID)
}

func TestChannelManagerSwitchWALPrimaryReplicaNoopsWhenTargetAlreadyPrimary(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       8,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 2},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
			PrimaryReplicaId: 1,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	require.NoError(t, manager.SwitchWALPrimaryReplica(ctx, "test-channel", 1))
	view := manager.CurrentPChannelsView()
	pchannel := view.Channels[ChannelID{Name: "test-channel"}]
	assert.Equal(t, int64(8), pchannel.CurrentTerm())
	assert.Equal(t, int64(1), pchannel.PrimaryReplicaID())
}

func TestChannelManagerReopensUnavailablePrimaryWALReplica(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       8,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 2},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
			PrimaryReplicaId: 1,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					AssignmentEpoch: 4,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			require.Len(t, metas, 1)
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	modified, err := manager.AssignPChannels(ctx, map[ChannelID]types.PChannelInfoAssigned{
		{Name: "test-channel"}: {
			Channel: types.PChannelInfo{
				Name:       "test-channel",
				Term:       8,
				AccessMode: types.AccessModeRW,
			},
			Node: types.StreamingNodeInfo{ServerID: 3},
		},
	})

	require.NoError(t, err)
	require.Len(t, modified, 1)
	require.NotNil(t, saved)
	assert.Equal(t, int64(9), saved.GetChannel().GetTerm())
	assert.Equal(t, int64(1), saved.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, saved.GetState())
	assert.Equal(t, int64(3), saved.GetNode().GetServerId())
	primary := saved.GetReplicas()[1]
	assert.Equal(t, int64(1), primary.GetReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, primary.GetAccessMode())
	assert.Equal(t, int64(5), primary.GetAssignmentEpoch())
	assert.Equal(t, int64(2), primary.GetActiveNode().GetServerId())
	assert.Equal(t, int64(3), primary.GetTargetNode().GetServerId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, primary.GetState())
}

func TestChannelManagerRejectsPrimarySwitchUntilTargetHasPrimaryServingShards(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel:          &streamingpb.PChannelInfo{Name: "test-channel", Term: 7},
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)
	manager.SetShardAssignmentProvider(staticShardAssignmentProvider{
		assignments: map[string][]types.ShardAssignmentEntry{
			"test-channel": {
				{CollectionID: 10, ShardIndex: 0, ReplicaID: 100, WALReplicaID: 0},
			},
		},
	})

	err = manager.SwitchWALPrimaryReplica(ctx, "test-channel", 1)

	assert.ErrorIs(t, err, ErrWALReplicaOperationInvalid)
	view := manager.CurrentPChannelsView()
	pchannel := view.Channels[ChannelID{Name: "test-channel"}]
	assert.Equal(t, int64(7), pchannel.CurrentTerm())
	assert.Equal(t, int64(0), pchannel.PrimaryReplicaID())
}

func TestChannelManagerSwitchWALPrimaryReplicaWhenTargetHasPrimaryServingShards(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel:          &streamingpb.PChannelInfo{Name: "test-channel", Term: 7},
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	var saved *streamingpb.PChannelMeta
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			require.Len(t, metas, 1)
			saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
			return nil
		})

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)
	manager.SetShardAssignmentProvider(staticShardAssignmentProvider{
		assignments: map[string][]types.ShardAssignmentEntry{
			"test-channel": {
				{CollectionID: 10, ShardIndex: 0, ReplicaID: 100, WALReplicaID: 0},
				{CollectionID: 10, ShardIndex: 0, ReplicaID: 101, WALReplicaID: 1},
			},
		},
	})

	err = manager.SwitchWALPrimaryReplica(ctx, "test-channel", 1)

	require.NoError(t, err)
	require.NotNil(t, saved)
	assert.Equal(t, int64(8), saved.GetChannel().GetTerm())
	assert.Equal(t, int64(1), saved.GetPrimaryReplicaId())
}

func TestChannelManagerDoesNotPublishSwitchingPrimaryInLegacyRelations(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:       0,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					AssignmentEpoch: 3,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 7,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchWALPrimaryReplica(ctx, "test-channel", 1))

	assignment, err := manager.GetLatestChannelAssignment()
	require.NoError(t, err)
	assert.Empty(t, assignment.Relations, "legacy pchannel assignment must not expose the new primary before RW open is done")
	assert.NotContains(t, assignment.WALReplicaRelations, types.WALReplicaInfoAssigned{
		Replica: types.WALReplicaInfo{
			ChannelID:         types.ChannelID{Name: "test-channel", WALReplicaID: 1},
			AccessMode:        types.AccessModeRW,
			PChannelWriteTerm: 8,
			AssignmentEpoch:   8,
			State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
		},
		Node: types.StreamingNodeInfo{ServerID: 2},
	})
}

func TestChannelManagerAllowsAssignDoneWithQueryViewDependency(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel:          &streamingpb.PChannelInfo{Name: "test-channel", Term: 7},
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:  1,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 2},
					TargetNode: &streamingpb.StreamingNodeInfo{ServerId: 3},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)
	manager.SetWALReplicaDependencyProvider(staticWALReplicaDependencyProvider{
		dependencies: map[ChannelID]bool{replicaID: true},
	})

	err = manager.AssignWALReplicasDone(ctx, map[ChannelID]int64{replicaID: 0})
	require.NoError(t, err)

	view := manager.CurrentPChannelsView()
	replica, ok := view.Channels[ChannelID{Name: "test-channel"}].WALReplica(1)
	require.True(t, ok)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, replica.GetState())
	assert.Equal(t, int64(3), replica.GetActiveNode().GetServerId())
	assert.Nil(t, replica.GetTargetNode())
}

func TestChannelManagerIgnoresStaleWALReplicaAssignDone(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel:          &streamingpb.PChannelInfo{Name: "test-channel", Term: 7},
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:       1,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 1,
					ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2},
					TargetNode:      &streamingpb.StreamingNodeInfo{ServerId: 3},
					State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	saveCount := 0
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, metas []*streamingpb.PChannelMeta) error {
			saveCount++
			return nil
		}).Once()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	_, err = manager.AssignWALReplicas(ctx, map[ChannelID]types.StreamingNodeInfo{
		replicaID: {ServerID: 4},
	})
	require.NoError(t, err)

	err = manager.AssignWALReplicasDone(ctx, map[ChannelID]int64{replicaID: 1})
	require.NoError(t, err)

	view := manager.CurrentPChannelsView()
	replica, ok := view.Channels[ChannelID{Name: "test-channel"}].WALReplica(1)
	require.True(t, ok)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, replica.GetState())
	assert.Equal(t, int64(2), replica.GetAssignmentEpoch())
	assert.Equal(t, int64(4), replica.GetTargetNode().GetServerId())
	assert.Equal(t, 1, saveCount)
}

func TestChannelManagerRejectsDroppingWALReplicaWithQueryViewDependency(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:     1,
					AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ResourceGroup: "rg-a",
					ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Maybe()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	manager.SetWALReplicaDependencyProvider(staticWALReplicaDependencyProvider{
		dependencies: map[ChannelID]bool{replicaID: true},
	})

	err = manager.MarkWALReplicasAsDropping(ctx, []ChannelID{replicaID})
	assert.ErrorIs(t, err, ErrWALReplicaOperationInvalid)

	view := manager.CurrentPChannelsView()
	replica, ok := view.Channels[ChannelID{Name: "test-channel"}].WALReplica(1)
	require.True(t, ok)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, replica.GetState())
}

func TestChannelManagerRejectsRemovingDroppingWALReplicaWithQueryViewDependency(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	replicaID := ChannelID{Name: "test-channel", WALReplicaID: 1}
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{
				Name:       "test-channel",
				Term:       7,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			},
			Node:             &streamingpb.StreamingNodeInfo{ServerId: 1},
			State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			PrimaryReplicaId: 0,
			NextReplicaId:    2,
			Replicas: []*streamingpb.WALReplicaAssignment{
				{
					ReplicaId:  0,
					AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1},
					State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				{
					ReplicaId:     1,
					AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					ResourceGroup: "rg-a",
					ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2},
					State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
			},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil).Once()

	manager, err := RecoverChannelManager(ctx)
	require.NoError(t, err)

	require.NoError(t, manager.MarkWALReplicasAsDropping(ctx, []ChannelID{replicaID}))
	manager.SetWALReplicaDependencyProvider(staticWALReplicaDependencyProvider{
		dependencies: map[ChannelID]bool{replicaID: true},
	})

	err = manager.RemoveWALReplicas(ctx, []ChannelID{replicaID})
	assert.ErrorIs(t, err, ErrWALReplicaOperationInvalid)

	view := manager.CurrentPChannelsView()
	replica, ok := view.Channels[ChannelID{Name: "test-channel"}].WALReplica(1)
	require.True(t, ok)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, replica.GetState())
}

type staticShardAssignmentProvider struct {
	assignments map[string][]types.ShardAssignmentEntry
}

func (p staticShardAssignmentProvider) ShardAssignmentsByPChannel() map[string][]types.ShardAssignmentEntry {
	return p.assignments
}

type staticWALReplicaDependencyProvider struct {
	dependencies map[ChannelID]bool
}

func (p staticWALReplicaDependencyProvider) HasWALReplicaDependency(replicaID ChannelID) bool {
	return p.dependencies[replicaID]
}

func TestChannelManager_AddPChannels(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{Name: "test-channel", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)
	assert.NotNil(t, m)

	// Initial state: 1 channel
	view := m.CurrentPChannelsView()
	assert.Len(t, view.Channels, 1)

	// Add new channels
	err = m.AddPChannels(ctx, []string{"new-channel-1", "new-channel-2"})
	assert.NoError(t, err)

	// Should now have 3 channels
	view = m.CurrentPChannelsView()
	assert.Len(t, view.Channels, 3)

	// Adding existing channels should be idempotent
	err = m.AddPChannels(ctx, []string{"test-channel", "new-channel-1"})
	assert.NoError(t, err)
	view = m.CurrentPChannelsView()
	assert.Len(t, view.Channels, 3) // No change

	// Adding a mix of existing and new
	err = m.AddPChannels(ctx, []string{"test-channel", "brand-new-channel"})
	assert.NoError(t, err)
	view = m.CurrentPChannelsView()
	assert.Len(t, view.Channels, 4)
}

func TestChannelManager_AddPChannels_ROWhenStreamingNotEnabled(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	// streamingVersion is nil => streaming never enabled
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)

	err = m.AddPChannels(ctx, []string{"new-ro-channel"})
	assert.NoError(t, err)

	view := m.CurrentPChannelsView()
	ch, ok := view.Channels[ChannelID{Name: "new-ro-channel"}]
	assert.True(t, ok)
	assert.Equal(t, types.AccessModeRO, ch.ChannelInfo().AccessMode)
}

func TestChannelManager_AddPChannels_PersistFailureRollback(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "test-channel",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{
			Channel: &streamingpb.PChannelInfo{Name: "test-channel", Term: 1},
			Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
		},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	persistErr := errors.New("persist failure")
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(persistErr)

	m, err := RecoverChannelManager(ctx, "test-channel")
	assert.NoError(t, err)

	// Attempt to add channels; persist fails
	err = m.AddPChannels(ctx, []string{"fail-channel-1", "fail-channel-2"})
	assert.ErrorIs(t, err, persistErr)

	// Channels should be rolled back — still only the original channel
	view := m.CurrentPChannelsView()
	assert.Len(t, view.Channels, 1)
	_, ok := view.Channels[ChannelID{Name: "test-channel"}]
	assert.True(t, ok)
	_, ok = view.Channels[ChannelID{Name: "fail-channel-1"}]
	assert.False(t, ok)
}

func TestAddPChannels_UnavailableInReplication(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{
		Pchannel: "ch1",
	}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{Channel: &streamingpb.PChannelInfo{Name: "ch1", Term: 1}, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch2", Term: 1}, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}},
	}, nil)
	replicateCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch3", "ch4"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev2", TargetClusterId: "by-dev"},
		},
	}
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: replicateCfg}, nil)
	catalog.EXPECT().SavePChannels(mock.Anything, mock.Anything).Return(nil)

	m, err := RecoverChannelManager(ctx, "ch1", "ch2")
	assert.NoError(t, err)
	assert.Equal(t, replicateutil.RoleSecondary, m.ReplicateRole())

	// ch1 and ch2 should be available (in replicateConfig)
	assert.True(t, m.channels[ChannelID{Name: "ch1"}].AvailableInReplication())
	assert.True(t, m.channels[ChannelID{Name: "ch2"}].AvailableInReplication())

	// Dynamically add ch5 — not in replicateConfig, should be unavailable
	err = m.AddPChannels(ctx, []string{"ch5"})
	assert.NoError(t, err)
	assert.False(t, m.channels[ChannelID{Name: "ch5"}].AvailableInReplication())
}

func TestRecovery_NoReplicateConfig_AllAvailable(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "ch1"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{Channel: &streamingpb.PChannelInfo{Name: "ch1", Term: 1}, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}},
	}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	m, err := RecoverChannelManager(ctx, "ch1")
	assert.NoError(t, err)
	assert.True(t, m.channels[ChannelID{Name: "ch1"}].AvailableInReplication())
}

func TestAllocVirtualChannels_SkipsUnavailableChannels(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "ch1"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{Channel: &streamingpb.PChannelInfo{Name: "ch1", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch2", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch3", Term: 1}},
	}, nil)
	replicateCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch4", "ch5"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		},
	}
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: replicateCfg}, nil)

	m, err := RecoverChannelManager(ctx, "ch1", "ch2", "ch3")
	assert.NoError(t, err)

	// ch3 is unavailable — only ch1, ch2 are allocatable
	vchannels, err := m.AllocVirtualChannels(ctx, AllocVChannelParam{CollectionID: 1, Num: 2})
	assert.NoError(t, err)
	assert.Len(t, vchannels, 2)
	for _, vc := range vchannels {
		assert.False(t, strings.HasPrefix(vc, "ch3"))
	}

	// Requesting more than available channels should fail
	_, err = m.AllocVirtualChannels(ctx, AllocVChannelParam{CollectionID: 2, Num: 3})
	assert.Error(t, err)
}

func TestGetClusterChannels_ExcludesUnavailable(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "ch1"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{Channel: &streamingpb.PChannelInfo{Name: "ch1", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch2", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch3", Term: 1}},
	}, nil)
	replicateCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch4", "ch5"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		},
	}
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: replicateCfg}, nil)

	m, err := RecoverChannelManager(ctx, "ch1", "ch2", "ch3")
	assert.NoError(t, err)

	// getClusterChannels should only return ch1, ch2
	cc := m.getClusterChannels()
	assert.Len(t, cc.Channels, 2)
	assert.ElementsMatch(t, []string{"ch1", "ch2"}, cc.Channels)

	// getClusterChannels with OptIncludeUnavailableInReplication should return all 3
	allCC := m.getClusterChannels(OptIncludeUnavailableInReplication())
	assert.Len(t, allCC.Channels, 3)
	assert.ElementsMatch(t, []string{"ch1", "ch2", "ch3"}, allCC.Channels)
}

func TestUpdateReplicateConfiguration_FlipsAvailability(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptSession(s))

	ctx := context.Background()
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "ch1"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: 1}, nil)
	catalog.EXPECT().ListPChannel(mock.Anything).Return([]*streamingpb.PChannelMeta{
		{Channel: &streamingpb.PChannelInfo{Name: "ch1", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch2", Term: 1}},
		{Channel: &streamingpb.PChannelInfo{Name: "ch3", Term: 1}},
	}, nil)
	// Initial config: only ch1, ch2 in current cluster
	replicateCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch4", "ch5"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		},
	}
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(
		&streamingpb.ReplicateConfigurationMeta{ReplicateConfiguration: replicateCfg}, nil)

	m, err := RecoverChannelManager(ctx, "ch1", "ch2", "ch3")
	assert.NoError(t, err)

	// ch3 should be unavailable initially
	assert.False(t, m.channels[ChannelID{Name: "ch3"}].AvailableInReplication())
	assert.True(t, m.channels[ChannelID{Name: "ch1"}].AvailableInReplication())
	assert.True(t, m.channels[ChannelID{Name: "ch2"}].AvailableInReplication())

	// Update config to include ch3
	newCfg := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2", "ch3"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch4", "ch5", "ch6"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		},
	}
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{ReplicateConfiguration: newCfg}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"ch1", "ch2", "ch3"}).
		MustBuildBroadcast()
	result := message.BroadcastResultAlterReplicateConfigMessageV2{
		Message: message.MustAsBroadcastAlterReplicateConfigMessageV2(msg),
		Results: map[string]*message.AppendResult{
			"ch1": {MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(2), TimeTick: 1},
			"ch2": {MessageID: walimplstest.NewTestMessageID(3), LastConfirmedMessageID: walimplstest.NewTestMessageID(4), TimeTick: 1},
			"ch3": {MessageID: walimplstest.NewTestMessageID(5), LastConfirmedMessageID: walimplstest.NewTestMessageID(6), TimeTick: 1},
		},
	}
	catalog.EXPECT().SaveReplicateConfiguration(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	err = m.UpdateReplicateConfiguration(ctx, result)
	assert.NoError(t, err)

	// ch3 should now be available
	assert.True(t, m.channels[ChannelID{Name: "ch3"}].AvailableInReplication())
	// ch1, ch2 still available
	assert.True(t, m.channels[ChannelID{Name: "ch1"}].AvailableInReplication())
	assert.True(t, m.channels[ChannelID{Name: "ch2"}].AvailableInReplication())
}

func TestIsChannelAvailableInReplication(t *testing.T) {
	// No replicateConfig → always available
	assert.True(t, isChannelAvailableInReplication("ch1", nil))

	// Single cluster (no cross-cluster topology) → always available
	singleCluster := replicateutil.MustNewConfigHelper("by-dev", &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
		},
	})
	assert.True(t, isChannelAvailableInReplication("ch1", singleCluster))
	assert.True(t, isChannelAvailableInReplication("ch99", singleCluster))

	// Multi-cluster: channel in current cluster's list → available
	multiCluster := replicateutil.MustNewConfigHelper("by-dev", &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch1", "ch2"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch3", "ch4"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev", TargetClusterId: "by-dev2"},
		},
	})
	assert.True(t, isChannelAvailableInReplication("ch1", multiCluster))
	assert.True(t, isChannelAvailableInReplication("ch2", multiCluster))

	// Multi-cluster: channel NOT in current cluster's list → unavailable
	assert.False(t, isChannelAvailableInReplication("ch5", multiCluster))
	assert.False(t, isChannelAvailableInReplication("new-channel", multiCluster))
}

func newChannelID(name string) ChannelID {
	return ChannelID{
		Name: name,
	}
}
