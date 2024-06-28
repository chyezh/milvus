package channel

import (
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"go.uber.org/zap"
)

// NewLayout create a new layout.
func NewLayout(channelsInMeta map[string]types.PChannelInfo, nodesStatus map[int64]*NodeStatus) *Layout {
	// build channel to node map.
	channelNodeMap := make(map[string]map[int64]types.PChannelInfo, len(channelsInMeta))
	for serverID, nodeStatus := range nodesStatus {
		for channel, channelInfo := range nodeStatus.Channels {
			if _, ok := channelNodeMap[channel]; !ok {
				channelNodeMap[channel] = make(map[int64]types.PChannelInfo)
			}
			channelNodeMap[channel][serverID] = channelInfo
		}
	}

	return &Layout{
		Version:        0,
		Nodes:          nodesStatus,
		ChannelsInMeta: channelsInMeta,
		channelNodeMap: channelNodeMap,
	}
}

// Layout is the full topology of log node and pChannel.
// Layout is not concurrent safe.
type Layout struct {
	Version        int64
	Nodes          map[int64]*NodeStatus                   // serverID -> node
	ChannelsInMeta map[string]types.PChannelInfo           // channelName -> Channel info
	channelNodeMap map[string]map[int64]types.PChannelInfo // channelName -> serverID -> channel info
}

// InconsistencyChannelInfo is the inconsistency channel info.
type InconsistencyChannelInfo struct {
	WaitForUnassign []types.PChannelInfo
	WaitForAssign   []types.PChannelInfo
}

// FixInconsistency find the inconsistency in current layout.
// Inconsistency may be caused by:
// 1. log node failure.
// 2. log coord failure and recovering.
// 3. some rpc network failure.
// There is a consistency constraints:
// 1. a channel must in meta be assigned
// 2. a channel must be assigned on one and only one log node.
// It return the channel need to be assigned and extra expired relation need to be removed.
// Here's two invariant:
//  1. pchannel's Term in meta is always greater or equal than pchannel's Term in log node.
//  2. if pchannel's Term in meta == pchannel's Term in log node, then pchannel must be assigned to the log node.
//     In other words, pchannel's ServerID in meta == pchannel's ServerID in log node.
func (layout *Layout) FindInconsistency() InconsistencyChannelInfo {
	return InconsistencyChannelInfo{
		WaitForUnassign: layout.findWaitForUnassignedPChannel(),
		WaitForAssign:   layout.findWaitForAssignPChannel(),
	}
}

func (layout *Layout) findWaitForUnassignedPChannel() []types.PChannelInfo {
	// find all expired running channel
	expiredChannel := make([]types.PChannelInfo, 0)
	for pChannelName, nodeMap := range layout.channelNodeMap {
		for serverID, pChannelInfoInNode := range nodeMap {
			// two cases need to expired a running channel:
			// 1. channel not found in meta (should never happens before we support delete pchannel dynamically).
			// 2. channel in meta's term is greater than channel in node (channel should be reassigned to another node)
			if channelInfoInMeta, ok := layout.ChannelsInMeta[pChannelName]; !ok || channelInfoInMeta.Term > pChannelInfoInNode.Term {
				if ok {
					log.Info("channel term in meta is newer than log node", zap.Any("channelInNode", pChannelInfoInNode), zap.Any("channelInMeta", channelInfoInMeta), zap.Int64("serverID", serverID))
				} else {
					log.Info("channel not found in meta", zap.Any("channelInNode", pChannelInfoInNode), zap.Int64("serverID", serverID))
				}
				expiredChannel = append(expiredChannel, pChannelInfoInNode)
			}
		}
	}
	return expiredChannel
}

func (layout *Layout) findWaitForAssignPChannel() []types.PChannelInfo {
	// find all unassigned channel
	unassignedChannel := make([]types.PChannelInfo, 0)
	for pChannelName, pchannelInMeta := range layout.ChannelsInMeta {
		// check if channel is assigned.
		if nodeMap, ok := layout.channelNodeMap[pChannelName]; !ok || len(nodeMap) == 0 {
			// channel not assigned at any log node.
			log.Info("channel is not assigned at any server", zap.Any("channelInMeta", pchannelInMeta))
			unassignedChannel = append(unassignedChannel, pchannelInMeta)
		} else {
			found := false
			for _, info := range nodeMap {
				if info.Term == pchannelInMeta.Term {
					found = true
				}
			}
			if !found {
				// channel is not assigned at right term.
				log.Info("channel is not assigned at right term", zap.Any("channelInMeta", pchannelInMeta))
				unassignedChannel = append(unassignedChannel, pchannelInMeta)
			}
		}
	}
	return unassignedChannel
}
