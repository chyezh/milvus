package assignment

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestWatcher(t *testing.T) {
	r := mock_resolver.NewMockResolver(t)

	ch := make(chan discoverer.VersionedState)
	r.EXPECT().Watch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(s discoverer.VersionedState) error) error {
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				f(v)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	w := NewWatcher(r)
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	a := w.Get(ctx, "test_pchannel")
	assert.Nil(t, a)
	err := w.Watch(ctx, "test_pchannel", nil)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{
				{
					Addr: "test_addr",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 1,
								Address:  "test_addr",
							},
							Channels: map[string]types.PChannelInfo{
								"test_pchannel": {
									Name: "test_pchannel",
									Term: 1,
								},
								"test_pchannel_2": {
									Name: "test_pchannel_2",
									Term: 2,
								},
							},
						},
					),
				},
			},
		},
	}
	err = w.Watch(context.Background(), "test_pchannel", nil)
	assert.NoError(t, err)
	a = w.Get(ctx, "test_pchannel")
	assert.NotNil(t, a)
	assert.Equal(t, int64(1), a.Channel.Term)

	err = w.Watch(context.Background(), "test_pchannel_2", nil)
	assert.NoError(t, err)
	a = w.Get(ctx, "test_pchannel_2")
	assert.NotNil(t, a)
	assert.Equal(t, int64(2), a.Channel.Term)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = w.Watch(ctx, "test_pchannel", a)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWatcherRoutesWALReplicaAssignments(t *testing.T) {
	r := mock_resolver.NewMockResolver(t)

	ch := make(chan discoverer.VersionedState)
	r.EXPECT().Watch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, f func(s discoverer.VersionedState) error) error {
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				f(v)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	w := NewWatcher(r)
	defer w.Close()

	walReplica := types.ChannelID{Name: "test_pchannel", WALReplicaID: 2}
	assert.Nil(t, w.GetWALReplica(context.Background(), walReplica))

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{
				{
					Addr: "test_addr",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 2,
								Address:  "test_addr",
							},
							WALReplicas: map[types.ChannelID]types.WALReplicaInfo{
								walReplica: {
									ChannelID:         walReplica,
									AccessMode:        types.AccessModeRO,
									PChannelWriteTerm: 3,
									AssignmentEpoch:   5,
								},
							},
						},
					),
				},
			},
		},
	}

	err := w.WatchWALReplica(context.Background(), walReplica, nil)
	assert.NoError(t, err)
	assignment := w.GetWALReplica(context.Background(), walReplica)
	assert.Equal(t, &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       "test_pchannel",
			Term:       3,
			AccessMode: types.AccessModeRO,
		},
		WALReplicaID:    2,
		AssignmentEpoch: 5,
		Node: types.StreamingNodeInfo{
			ServerID: 2,
			Address:  "test_addr",
		},
	}, assignment)

	ch <- discoverer.VersionedState{
		Version: typeutil.VersionInt64(2),
		State: resolver.State{
			Addresses: []resolver.Address{
				{
					Addr: "test_addr_2",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 3,
								Address:  "test_addr_2",
							},
							WALReplicas: map[types.ChannelID]types.WALReplicaInfo{
								walReplica: {
									ChannelID:         walReplica,
									AccessMode:        types.AccessModeRO,
									PChannelWriteTerm: 3,
									AssignmentEpoch:   6,
								},
							},
						},
					),
				},
			},
		},
	}
	err = w.WatchWALReplica(context.Background(), walReplica, assignment)
	assert.NoError(t, err)
	assignment = w.GetWALReplica(context.Background(), walReplica)
	assert.Equal(t, int64(3), assignment.Node.ServerID)
	assert.Equal(t, int64(3), assignment.Channel.Term)
	assert.Equal(t, int64(6), assignment.AssignmentEpoch)
}

func TestWatcherPrefersExplicitWALReplicaAssignmentOverLegacyPrimaryChannel(t *testing.T) {
	w := &watcherImpl{
		cond:                  *syncutil.NewContextCond(&sync.Mutex{}),
		assignments:           make(map[string]types.PChannelInfoAssigned),
		walReplicaAssignments: make(map[types.ChannelID]types.PChannelInfoAssigned),
	}

	pchannel := "test_pchannel"
	replica0 := types.ChannelID{Name: pchannel, WALReplicaID: 0}
	replica1 := types.ChannelID{Name: pchannel, WALReplicaID: 1}
	state := discoverer.VersionedState{
		Version: typeutil.VersionInt64(1),
		State: resolver.State{
			Addresses: []resolver.Address{
				{
					Addr: "secondary_addr",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 10,
								Address:  "secondary_addr",
							},
							WALReplicas: map[types.ChannelID]types.WALReplicaInfo{
								replica0: {
									ChannelID:         replica0,
									AccessMode:        types.AccessModeRO,
									PChannelWriteTerm: 7,
									AssignmentEpoch:   11,
								},
							},
						},
					),
				},
				{
					Addr: "primary_addr",
					BalancerAttributes: attributes.WithChannelAssignmentInfo(
						new(attributes.Attributes),
						&types.StreamingNodeAssignment{
							NodeInfo: types.StreamingNodeInfo{
								ServerID: 20,
								Address:  "primary_addr",
							},
							Channels: map[string]types.PChannelInfo{
								pchannel: {
									Name:       pchannel,
									Term:       8,
									AccessMode: types.AccessModeRW,
								},
							},
							WALReplicas: map[types.ChannelID]types.WALReplicaInfo{
								replica1: {
									ChannelID:         replica1,
									AccessMode:        types.AccessModeRW,
									PChannelWriteTerm: 8,
									AssignmentEpoch:   12,
								},
							},
						},
					),
				},
			},
		},
	}

	for i := 0; i < 64; i++ {
		w.updateAssignment(state)
		primary := w.Get(context.Background(), pchannel)
		assert.Equal(t, &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{
				Name:       pchannel,
				Term:       8,
				AccessMode: types.AccessModeRW,
			},
			WALReplicaID:    1,
			AssignmentEpoch: 12,
			Node: types.StreamingNodeInfo{
				ServerID: 20,
				Address:  "primary_addr",
			},
		}, primary)

		assignment := w.GetWALReplica(context.Background(), replica0)
		assert.Equal(t, &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{
				Name:       pchannel,
				Term:       7,
				AccessMode: types.AccessModeRO,
			},
			WALReplicaID:    0,
			AssignmentEpoch: 11,
			Node: types.StreamingNodeInfo{
				ServerID: 10,
				Address:  "secondary_addr",
			},
		}, assignment)
	}
}
