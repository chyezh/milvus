package streamingnode

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ metastore.WALReplicaRuntimeCatalog = (*catalog)(nil)

const maxReplicaRuntimeObligations = 128

func replicaRuntimeKey(runtime *viewpb.StreamingWorkNode) string {
	return retentionKey(runtime.Pchannel, "replica-runtimes", strconv.FormatInt(runtime.NodeId, 10)+"/"+strconv.FormatUint(runtime.RuntimeId, 10))
}

func validateRetentionRuntime(runtime *viewpb.StreamingWorkNode) error {
	if runtime.GetPchannel() == "" || runtime.GetNodeId() <= 0 || runtime.GetRuntimeId() == 0 ||
		runtime.GetWalReplicaId() == 0 || runtime.GetWalReplicaRevision() == 0 || runtime.GetWalTerm() <= 0 || !runtime.GetReadOnly() {
		return merr.WrapErrServiceInternalMsg("WAL replica retention requires an exact read-only runtime identity")
	}
	return nil
}

func (c *catalog) loadReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode) (*streamingpb.WALReplicaRuntimeRetention, error) {
	value, err := c.loadRetentionValue(ctx, replicaRuntimeKey(runtime))
	if err != nil || value == "" {
		return nil, err
	}
	stored := &streamingpb.WALReplicaRuntimeRetention{}
	if err := proto.Unmarshal([]byte(value), stored); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL replica runtime obligation")
	}
	if !proto.Equal(runtime, stored.Runtime) || stored.State < streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE ||
		stored.State > streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
		return nil, merr.WrapErrDataIntegrityMsg("WAL replica runtime identity or state is invalid")
	}
	if (stored.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_PREPARED || stored.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_INSTALLED) &&
		(stored.Cursor == nil || stored.Cursor.Term != runtime.WalTerm) {
		return nil, merr.WrapErrDataIntegrityMsg("WAL replica runtime has an invalid bootstrap cursor")
	}
	return stored, nil
}

func (c *catalog) CreateWALReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode) error {
	return c.advanceReplicaRuntime(ctx, runtime, nil, streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE)
}

func (c *catalog) PrepareWALReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode, cursor *streamingpb.WALReplicaCursor) error {
	return c.advanceReplicaRuntime(ctx, runtime, cursor, streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_PREPARED)
}

func (c *catalog) InstallWALReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode, cursor *streamingpb.WALReplicaCursor) error {
	return c.advanceReplicaRuntime(ctx, runtime, cursor, streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_INSTALLED)
}

func (c *catalog) advanceReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode, cursor *streamingpb.WALReplicaCursor, state streamingpb.WALReplicaRuntimeRetentionState) error {
	if err := validateRetentionRuntime(runtime); err != nil {
		return err
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(runtime.Pchannel)+"epoch")
	if err != nil {
		return err
	}
	stored, err := c.loadReplicaRuntime(ctx, runtime)
	if err != nil {
		return err
	}
	if stored == nil {
		if state != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE && state != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			return merr.WrapErrServiceUnavailable("WAL replica candidate was not registered")
		}
		if state == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE {
			active, err := c.ListWALReplicaRuntimes(ctx, runtime.Pchannel)
			if err != nil {
				return err
			}
			if len(active) >= maxReplicaRuntimeObligations {
				return merr.WrapErrServiceUnavailable("WAL replica runtime retention budget exhausted")
			}
		}
		stored = &streamingpb.WALReplicaRuntimeRetention{Runtime: proto.CloneOf(runtime)}
	}
	var guards []predicates.Predicate
	if state == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
		stored.State = state
	} else {
		if stored.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			return merr.WrapErrServiceUnavailable("WAL replica runtime was already released")
		}
		if state != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE {
			if cursor == nil || cursor.Term != runtime.WalTerm {
				return merr.WrapErrServiceInternalMsg("WAL replica bootstrap cursor does not match its runtime")
			}
			if stored.Cursor != nil && !proto.Equal(stored.Cursor, cursor) {
				return merr.WrapErrDataIntegrityMsg("WAL replica candidate was rebound to another bootstrap cut")
			}
			if state == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_INSTALLED && stored.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE {
				return merr.WrapErrServiceUnavailable("WAL replica bootstrap has not completed")
			}
			owner, err := c.metaKV.Load(ctx, buildConsumeCheckpointKey(runtime.Pchannel))
			if err != nil {
				return err
			}
			cp := &streamingpb.WALCheckpoint{}
			if err := proto.Unmarshal([]byte(owner), cp); err != nil {
				return merr.WrapErrDataIntegrity(err, "decode WAL replica bootstrap owner")
			}
			if cp.Term != runtime.WalTerm {
				return merr.WrapErrServiceUnavailable("WAL replica bootstrap source was fenced")
			}
			guards = append(guards, predicates.ValueEqual(buildConsumeCheckpointKey(runtime.Pchannel), owner))
			stored.Cursor = proto.CloneOf(cursor)
			stored.State = max(stored.State, state)
		}
	}
	encoded, err := proto.Marshal(stored)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL replica runtime obligation")
	}
	key := replicaRuntimeKey(runtime)
	activeKey := retentionKey(runtime.Pchannel, "replica-runtime-active", key)
	saves := map[string]string{key: string(encoded)}
	var removes []string
	if state == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
		removes = []string{activeKey}
	} else {
		saves[activeKey] = string(encoded)
	}
	return c.commitRetention(ctx, runtime.Pchannel, epoch, saves, removes, guards...)
}

// The active index bounds recovery work independently of permanent release
// fences. An absent node still owns its roots until explicitly drained/fenced.
func (c *catalog) ListWALReplicaRuntimes(ctx context.Context, channel string) ([]*streamingpb.WALReplicaRuntimeRetention, error) {
	var active []*streamingpb.WALReplicaRuntimeRetention
	err := c.metaKV.WalkWithPrefix(ctx, retentionPrefix(channel)+"replica-runtime-active/", maxReplicaRuntimeObligations, func(_, value []byte) error {
		if len(active) >= maxReplicaRuntimeObligations {
			return merr.WrapErrDataIntegrityMsg("WAL replica runtime index exceeds its budget")
		}
		entry := &streamingpb.WALReplicaRuntimeRetention{}
		if err := proto.Unmarshal(value, entry); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode active WAL replica runtime")
		}
		if err := validateRetentionRuntime(entry.Runtime); err != nil {
			return merr.WrapErrDataIntegrity(err, "invalid active WAL replica runtime")
		}
		if entry.Runtime.Pchannel != channel || entry.State < streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_CANDIDATE || entry.State >= streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			return merr.WrapErrDataIntegrityMsg("WAL replica runtime index has an invalid state or channel")
		}
		active = append(active, entry)
		return nil
	})
	return active, err
}

func (c *catalog) ReleaseWALReplicaRuntime(ctx context.Context, runtime *viewpb.StreamingWorkNode) error {
	if err := c.advanceReplicaRuntime(ctx, runtime, nil, streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED); err != nil {
		return err
	}
	roots, err := c.ListWALObjectRetentionRoots(ctx, runtime.Pchannel)
	if err != nil {
		return err
	}
	for _, root := range roots {
		if proto.Equal(root.Runtime, runtime) {
			if err := c.ReleaseWALObjects(ctx, runtime.Pchannel, root.RootId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *catalog) checkRuntimeRetention(ctx context.Context, channel string, runtime *viewpb.StreamingWorkNode) error {
	if runtime == nil {
		return nil
	}
	if err := validateRetentionRuntime(runtime); err != nil {
		return err
	}
	if runtime.Pchannel != channel {
		return merr.WrapErrServiceInternalMsg("WAL object root belongs to another channel")
	}
	stored, err := c.loadReplicaRuntime(ctx, runtime)
	if err != nil {
		return err
	}
	if stored == nil || stored.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
		return merr.WrapErrServiceUnavailable("WAL object root has no live replica runtime")
	}
	return nil
}

// SweepReleasedWALReplicaRoots resumes cleanup if the receiver exited after
// fencing its runtime but before removing all roots. It never infers release
// from a missing connection, an old source term, or elapsed wall-clock time.
func (c *catalog) SweepReleasedWALReplicaRoots(ctx context.Context, channel string, limit int) error {
	if limit <= 0 || limit > maxRetentionRoots {
		return merr.WrapErrServiceInternalMsg("invalid released WAL replica root sweep budget")
	}
	roots, err := c.ListWALObjectRetentionRoots(ctx, channel)
	if err != nil {
		return err
	}
	removed := 0
	for _, root := range roots {
		if root.Runtime == nil {
			continue
		}
		runtime, err := c.loadReplicaRuntime(ctx, root.Runtime)
		if err != nil {
			return err
		}
		if runtime == nil {
			return merr.WrapErrDataIntegrityMsg("retained WAL object has no runtime obligation")
		}
		if runtime.State != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			continue
		}
		if err := c.ReleaseWALObjects(ctx, channel, root.RootId); err != nil {
			return err
		}
		removed++
		if removed == limit {
			break
		}
	}
	return nil
}
