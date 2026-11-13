package walmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type refreshTestWAL struct{ wal.WAL }

func (*refreshTestWAL) IsAvailable() bool            { panic("mockey") }
func (*refreshTestWAL) Close()                       { panic("mockey") }
func (*refreshTestWAL) ReplicaRefreshRequired() bool { panic("mockey") }

func TestReplicaControllerRebuildKeepsOldPrefix(t *testing.T) {
	for _, sameTerm := range []bool{true, false} {
		t.Run(map[bool]string{true: "eviction", false: "new_primary"}[sameTerm], func(t *testing.T) {
			mockey.PatchConvey("baseline replacement preserves queries across failed and pending preparation", t, func() {
				old, replacement := &refreshTestWAL{}, &refreshTestWAL{}
				var refresh, closed atomic.Bool
				mockey.Mock((*refreshTestWAL).IsAvailable).Return(true).Build()
				mockey.Mock((*refreshTestWAL).ReplicaRefreshRequired).To(func(w *refreshTestWAL) bool {
					return w == old && refresh.Load()
				}).Build()
				mockey.Mock((*refreshTestWAL).Close).To(func(w *refreshTestWAL) {
					if w == old {
						closed.Store(true)
					}
				}).Build()
				opening, failed, ready := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var attempts atomic.Int64
				mockey.Mock((*controllerTestOpener).Open).To(func(_ *controllerTestOpener, ctx context.Context, option *wal.OpenOption) (wal.WAL, error) {
					switch attempts.Add(1) {
					case 1:
						return old, nil
					case 2:
						close(failed)
						return nil, merr.ErrServiceUnavailable
					default:
						if sameTerm {
							require.EqualValues(t, 7, option.Primary.Channel.Term)
						} else {
							require.EqualValues(t, 8, option.Primary.Channel.Term)
						}
						close(opening)
						select {
						case <-ready:
							return replacement, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
				}).Build()
				c := newReplicaController(t.Context(), 2, "p", &controllerTestOpener{})
				defer c.close()
				plan := controllerTestPlan()
				awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
				if sameTerm {
					refresh.Store(true)
				} else {
					plan = proto.CloneOf(plan)
					plan.Revision++
					plan.WalTerm++
					_, err := c.reconcile(plan)
					require.NoError(t, err)
				}
				select {
				case <-failed:
				case <-time.After(5 * time.Second):
					t.Fatal("controller did not prepare a new baseline")
				}
				require.Same(t, old, c.getWAL())
				require.False(t, closed.Load())
				select {
				case <-opening:
				case <-time.After(5 * time.Second):
					t.Fatal("controller did not retry the candidate")
				}
				require.Same(t, old, c.getWAL())
				require.False(t, closed.Load())
				close(ready)
				require.Eventually(t, func() bool { return c.getWAL() == replacement }, 5*time.Second, time.Millisecond)
				awaitReplicaState(t, c, plan, streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY)
				require.True(t, closed.Load())
				require.EqualValues(t, 3, attempts.Load())
			})
		})
	}
}
