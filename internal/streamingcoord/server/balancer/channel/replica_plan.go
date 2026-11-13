package channel

import (
	"context"
	"slices"
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

// CompleteReplicaRetirements removes only exact, drained placements. A delayed
// report from an older plan cannot retire a replacement on the same server or
// an incarnation of the member that has since rejoined its resource group.
func (cm *ChannelManager) CompleteReplicaRetirements(ctx context.Context, id ChannelID, reports []*streamingpb.WALReplicaObservation) error {
	cm.cond.LockAndBroadcast()
	defer cm.cond.L.Unlock()
	current, ok := cm.channels[id]
	if !ok {
		return ErrChannelNotExist
	}
	plan := current.ReplicaPlan()
	if len(plan.GetRetiringReplicas()) == 0 {
		return nil
	}
	before := len(plan.RetiringReplicas)
	plan.RetiringReplicas = slices.DeleteFunc(plan.RetiringReplicas, func(retired *streamingpb.WALReplicaAssignment) bool {
		return slices.ContainsFunc(reports, func(report *streamingpb.WALReplicaObservation) bool {
			return report.GetState() == streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED &&
				report.GetReplicaId() == retired.ReplicaId && report.GetReplicaRevision() == retired.ReplicaRevision &&
				report.GetNodeId() == retired.GetNode().GetServerId()
		})
	})
	if len(plan.RetiringReplicas) == before {
		return nil
	}
	plan.Revision++
	mutable := current.CopyForWrite()
	mutable.inner.ReplicaPlan = plan
	return cm.updatePChannelMeta(ctx, []*streamingpb.PChannelMeta{mutable.IntoRawMeta()})
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
		Revision:         previous.GetRevision(),
		WalTerm:          m.CurrentTerm(),
		NextReplicaId:    previous.GetNextReplicaId(),
		RetiringReplicas: proto.CloneOf(previous).GetRetiringReplicas(),
	}
	oldMembers := make(map[string]*streamingpb.WALReplicaAssignment)
	for _, member := range previous.GetReplicas() {
		oldMembers[member.ResourceGroup] = member
	}
	for _, group := range groups {
		member := &streamingpb.WALReplicaAssignment{
			ResourceGroup:   group,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			ReplicaRevision: 1,
		}
		if old, ok := oldMembers[group]; ok {
			member.ReplicaId = old.ReplicaId
			member.ReplicaRevision = old.ReplicaRevision
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
		if old := oldMembers[group]; old != nil {
			if !proto.Equal(old.Node, member.Node) || old.AccessMode != member.AccessMode ||
				(member.AccessMode == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE && previous.WalTerm != plan.WalTerm) {
				member.ReplicaRevision++
			}
			if old.Node != nil && !proto.Equal(old.Node, member.Node) {
				plan.RetiringReplicas = append(plan.RetiringReplicas, proto.CloneOf(old))
			}
			delete(oldMembers, group)
		}
		plan.Replicas = append(plan.Replicas, member)
	}
	for _, old := range oldMembers {
		if old.Node != nil {
			plan.RetiringReplicas = append(plan.RetiringReplicas, proto.CloneOf(old))
		}
	}
	sort.Slice(plan.RetiringReplicas, func(i, j int) bool {
		a, b := plan.RetiringReplicas[i], plan.RetiringReplicas[j]
		if a.ReplicaId != b.ReplicaId {
			return a.ReplicaId < b.ReplicaId
		}
		return a.ReplicaRevision < b.ReplicaRevision
	})
	if proto.Equal(previous, plan) {
		return false, nil
	}
	plan.Revision++
	m.inner.ReplicaPlan = plan
	return true, nil
}
