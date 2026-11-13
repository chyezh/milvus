package channel

import (
	"context"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReplicaPlanTarget contains one placement per discovered SN resource group.
// A nil node retains a member whose RG currently has no healthy candidate.
type ReplicaPlanTarget struct {
	PrimaryResourceGroup string
	Nodes                map[string]*types.StreamingNodeInfo
}

// ReplicaPlan returns an owned copy; the catalog snapshot remains immutable.
func (c *PChannelMeta) ReplicaPlan() *streamingpb.WALReplicaPlan {
	if c.inner.ReplicaPlan == nil {
		return nil
	}
	return proto.Clone(c.inner.ReplicaPlan).(*streamingpb.WALReplicaPlan)
}

// AssignReplicaPlans persists writer fencing and the entire replica intent in
// one catalog update, before any node receives the assignment. Writer discovery
// continues to use the primary projection of this same metadata record.
func (cm *ChannelManager) AssignReplicaPlans(ctx context.Context, targets map[ChannelID]ReplicaPlanTarget) (map[ChannelID]*PChannelMeta, error) {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()

	metas := make([]*streamingpb.PChannelMeta, 0, len(targets))
	for id, target := range targets {
		current, ok := cm.channels[id]
		if !ok {
			return nil, ErrChannelNotExist
		}
		mutable := current.CopyForWrite()
		changed, err := mutable.assignReplicaPlan(target)
		if err != nil {
			return nil, err
		}
		if changed {
			metas = append(metas, mutable.IntoRawMeta())
		}
	}
	if err := cm.updatePChannelMeta(ctx, metas); err != nil {
		return nil, err
	}
	updated := make(map[ChannelID]*PChannelMeta, len(metas))
	for _, raw := range metas {
		meta := cm.channels[types.ChannelID{Name: raw.Channel.Name}]
		updated[meta.ChannelID()] = meta
		cm.metrics.AssignPChannelStatus(meta)
	}
	return updated, nil
}

func (m *mutablePChannel) assignReplicaPlan(target ReplicaPlanTarget) (bool, error) {
	primary := target.Nodes[target.PrimaryResourceGroup]
	if primary == nil {
		return false, merr.WrapErrServiceUnavailableMsg("no primary candidate for WAL %s", m.Name())
	}
	groups := make([]string, 0, len(target.Nodes))
	servers := make(map[int64]struct{}, len(target.Nodes))
	for group, node := range target.Nodes {
		if group == "" {
			return false, merr.WrapErrServiceInternalMsg("WAL replica resource group must be normalized")
		}
		if node != nil {
			if _, duplicate := servers[node.ServerID]; duplicate || node.ServerID <= 0 || node.Address == "" {
				return false, merr.WrapErrServiceInternalMsg("invalid WAL replica node in resource group %s", group)
			}
			servers[node.ServerID] = struct{}{}
		}
		groups = append(groups, group)
	}
	sort.Strings(groups)

	// Only writer replacement/recovery advances WALTerm. A follower joining,
	// moving within its RG, or becoming pending must not fence the writer.
	m.TryAssignToServerID(types.AccessModeRW, *primary)
	previous := m.inner.ReplicaPlan
	plan := &streamingpb.WALReplicaPlan{
		Revision:      previous.GetRevision(),
		WalTerm:       m.CurrentTerm(),
		NextReplicaId: previous.GetNextReplicaId(),
	}
	oldMembers := make(map[string]*streamingpb.WALReplicaAssignment)
	for _, member := range previous.GetReplicas() {
		oldMembers[member.ResourceGroup] = member
	}
	for _, group := range groups {
		member := &streamingpb.WALReplicaAssignment{
			ResourceGroup: group,
			AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
		}
		if old, ok := oldMembers[group]; ok {
			member.ReplicaId = old.ReplicaId
		} else {
			plan.NextReplicaId++
			member.ReplicaId = plan.NextReplicaId
		}
		if node := target.Nodes[group]; node != nil {
			member.Node = types.NewProtoFromStreamingNodeInfo(*node)
		}
		if group == target.PrimaryResourceGroup {
			member.AccessMode = streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
		}
		plan.Replicas = append(plan.Replicas, member)
	}
	if proto.Equal(previous, plan) {
		return false, nil
	}
	plan.Revision++
	m.inner.ReplicaPlan = plan
	return true, nil
}
