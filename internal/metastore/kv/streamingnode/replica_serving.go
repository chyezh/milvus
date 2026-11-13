package streamingnode

import (
	"context"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func replicaNodeFenceKey(runtime *viewpb.StreamingWorkNode) string {
	return retentionKey(runtime.Pchannel, "replica-node-fenced", strconv.FormatInt(runtime.NodeId, 10))
}

func replicaNodeSessionKey(nodeID int64) string {
	return path.Join(sessionutil.DefaultServiceRoot, typeutil.StreamingNodeRole+"-"+strconv.FormatInt(nodeID, 10))
}

// ValidateWALReplicaServing is a linearizable publication check after all query
// work, including lazy reads and native result conversion, has completed. A
// lost process may keep executing, but cannot publish a successful result after
// its exact runtime is fenced. Writer term changes alone do not revoke serving.
func (c *catalog) ValidateWALReplicaServing(ctx context.Context, runtime *viewpb.StreamingWorkNode) error {
	if err := validateRetentionRuntime(runtime); err != nil {
		return err
	}
	stored, err := c.loadReplicaRuntime(ctx, runtime)
	if err != nil {
		return err
	}
	if stored == nil || stored.State != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_INSTALLED {
		return merr.WrapErrServiceUnavailable("WAL replica query runtime is not installed or was fenced")
	}
	return nil
}

// FenceLostWALReplicaRuntimes closes serving authority before reclaiming roots.
// Session absence is only the trigger: the durable RELEASED transition rejects
// final query publication, and the node fence prevents delayed new candidates.
// A lost etcd response conservatively leaves objects retained until a later
// sweep. No wall-clock grace period or assumed maximum query duration is used.
func (c *catalog) FenceLostWALReplicaRuntimes(ctx context.Context, channel string, limit int) error {
	if limit <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid WAL replica fencing budget")
	}
	active, err := c.ListWALReplicaRuntimes(ctx, channel)
	if err != nil {
		return err
	}
	for _, entry := range active {
		runtime := entry.Runtime
		sessionKey := replicaNodeSessionKey(runtime.NodeId)
		session, err := c.loadRetentionValue(ctx, sessionKey)
		if err != nil {
			return err
		}
		if session != "" {
			continue
		}
		epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
		if err != nil {
			return err
		}
		entry.State = streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED
		encoded, err := proto.Marshal(entry)
		if err != nil {
			return merr.WrapErrDataIntegrity(err, "encode fenced WAL replica runtime")
		}
		key := replicaRuntimeKey(runtime)
		if err := c.commitRetention(ctx, channel, epoch, map[string]string{
			key: string(encoded), replicaNodeFenceKey(runtime): "fenced",
		}, []string{retentionKey(channel, "replica-runtime-active", key)}, predicates.KeyNotExist(sessionKey)); err != nil {
			return err
		}
		limit--
		if limit == 0 {
			break
		}
	}
	return nil
}
