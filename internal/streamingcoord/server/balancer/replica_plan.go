package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// planWALReplicas runs the existing load policy independently in every RG.
// Each RG covers all PChannels; the writer is selected from those placements.
func planWALReplicas(view *channel.PChannelView, nodes map[int64]*types.StreamingNodeStatus, preferred string, policy Policy) (map[types.ChannelID]channel.ReplicaPlanTarget, error) {
	groups := make(map[string]map[int64]*types.StreamingNodeStatus)
	for id, node := range nodes {
		group := node.ResourceGroup
		if group == "" {
			group = common.DefaultResourceGroupName
		}
		if groups[group] == nil {
			groups[group] = make(map[int64]*types.StreamingNodeStatus)
		}
		groups[group][id] = node
	}
	groupNames := make([]string, 0, len(groups))
	for group := range groups {
		groupNames = append(groupNames, group)
	}
	sort.Strings(groupNames)
	plans := make(map[types.ChannelID]channel.ReplicaPlanTarget, len(view.Channels))
	for id := range view.Channels {
		plans[id] = channel.ReplicaPlanTarget{Nodes: make(map[string]*types.StreamingNodeInfo, len(groups))}
	}
	for _, group := range groupNames {
		layout := replicaGroupLayout(view, groups[group])
		// The channel's primary projection cannot describe this RG's follower.
		// Keep its own healthy placement even while it is still preparing.
		for id, meta := range view.Channels {
			for _, member := range meta.ReplicaPlan().GetReplicas() {
				if member.ResourceGroup != group {
					continue
				}
				if node := groups[group][member.GetNode().GetServerId()]; node != nil && node.IsHealthy() {
					layout.ChannelsToNodes[id] = node.ServerID
				}
			}
		}
		if layout.TotalNodes() == 0 {
			for _, plan := range plans {
				plan.Nodes[group] = nil
			}
			continue
		}
		expected, err := policy.Balance(layout)
		if err != nil {
			return nil, merr.Wrapf(err, "balance WAL replicas in resource group %s", group)
		}
		for id, plan := range plans {
			assignment, ok := expected.ChannelAssignment[id]
			if !ok || groups[group][assignment.Node.ServerID] == nil || !groups[group][assignment.Node.ServerID].IsHealthy() {
				return nil, merr.WrapErrServiceInternalMsg("balance policy omitted or misplaced WAL %s in resource group %s", id.Name, group)
			}
			node := assignment.Node
			plan.Nodes[group] = &node
		}
	}
	for id, plan := range plans {
		current := view.Channels[id]
		primaryGroup := ""
		if plan.Nodes[preferred] != nil {
			primaryGroup = preferred
		} else {
			// Keep a healthy writer's RG when the preference is absent or
			// unavailable. Adding another RG must not trigger gratuitous failover.
			for _, group := range groupNames {
				if node := groups[group][current.CurrentServerID()]; node != nil && node.IsHealthy() {
					primaryGroup = group
					break
				}
			}
		}
		if primaryGroup == "" {
			primaryGroup = choosePrimaryResourceGroup(plan.Nodes, groups, groupNames)
		}
		if primaryGroup == "" {
			return nil, merr.WrapErrServiceUnavailableMsg("no healthy StreamingNode for WAL %s", id.Name)
		}
		plan.PrimaryResourceGroup = primaryGroup
		plans[id] = plan
	}
	return plans, nil
}

func replicaGroupLayout(view *channel.PChannelView, nodes map[int64]*types.StreamingNodeStatus) CurrentLayout {
	layout := CurrentLayout{
		Config: newCommonBalancePolicyConfig(), Stats: view.Stats,
		Channels:           make(map[channel.ChannelID]types.PChannelInfo, len(view.Channels)),
		AllNodesInfo:       make(map[int64]types.StreamingNodeStatus, len(nodes)),
		ChannelsToNodes:    make(map[types.ChannelID]int64, len(view.Channels)),
		ExpectedAccessMode: make(map[channel.ChannelID]types.AccessMode, len(view.Channels)),
	}
	for id, node := range nodes {
		if node.IsHealthy() {
			layout.AllNodesInfo[id] = *node
		}
	}
	for id, meta := range view.Channels {
		layout.Channels[id] = meta.ChannelInfo()
		layout.ExpectedAccessMode[id] = types.AccessModeRO
		if _, ok := layout.AllNodesInfo[meta.CurrentServerID()]; ok && meta.IsAssigned() {
			layout.ChannelsToNodes[id] = meta.CurrentServerID()
		}
	}
	return layout
}

func choosePrimaryResourceGroup(placements map[string]*types.StreamingNodeInfo, groups map[string]map[int64]*types.StreamingNodeStatus, names []string) string {
	if placements[common.DefaultResourceGroupName] != nil {
		return common.DefaultResourceGroupName
	}
	selected, capacity := "", 0
	for _, group := range names {
		if placements[group] == nil {
			continue
		}
		healthy := 0
		for _, node := range groups[group] {
			if node.IsHealthy() {
				healthy++
			}
		}
		if healthy > capacity {
			selected, capacity = group, healthy
		}
	}
	return selected
}
