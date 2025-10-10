// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replicatemanager

import (
	"context"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// replicateManager is the implementation of ReplicateManagerClient.
type replicateManager struct {
	ctx context.Context
	mu  sync.Mutex

	// replicators is a map of replicate pchannel name to ChannelReplicator.
	replicators         map[string]Replicator
	replicatorPChannels map[string]*streamingpb.ReplicatePChannelMeta
}

func NewReplicateManager() *replicateManager {
	return &replicateManager{
		ctx:                 context.Background(),
		replicators:         make(map[string]Replicator),
		replicatorPChannels: make(map[string]*streamingpb.ReplicatePChannelMeta),
	}
}

func (r *replicateManager) CreateReplicator(replicateKey string, repCtx *replication.ReplicateContext) {
	r.mu.Lock()
	defer r.mu.Unlock()

	logger := log.With(zap.String("repKey", replicateKey))
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	if !strings.Contains(repCtx.RepMeta.GetSourceChannelName(), currentClusterID) {
		// should be checked by controller, here is a redundant check
		return
	}
	_, ok := r.replicators[replicateKey]
	if ok {
		logger.Debug("replicator already exists, skip create replicator")
		return
	}
	replicator := NewChannelReplicator(repCtx)
	replicator.StartReplicate()
	r.replicators[replicateKey] = replicator
	r.replicatorPChannels[replicateKey] = repCtx.RepMeta
	logger.Info("created replicator for replicate pchannel")
}

func (r *replicateManager) RemoveReplicator(replicateKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	logger := log.With(zap.String("repKey", replicateKey))
	_, ok := r.replicators[replicateKey]
	if !ok {
		logger.Info("replicator not found, skip remove")
		return
	}
	// replicator will be stopped itself, so here we just
	// need to remove the replicator from the map
	delete(r.replicators, replicateKey)
	delete(r.replicatorPChannels, replicateKey)
	logger.Info("removed replicator for replicate pchannel")
}

func (r *replicateManager) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, replicator := range r.replicators {
		replicator.StopReplicate()
	}
	r.replicators = make(map[string]Replicator)
	r.replicatorPChannels = make(map[string]*streamingpb.ReplicatePChannelMeta)
}
