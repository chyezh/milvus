// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestAllShardsUpRequiresCompleteExpectedSet(t *testing.T) {
	first := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	missing := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}
	secondReplica := qviews.ShardID{ReplicaID: 2, VChannel: "v0"}
	version := qviews.QueryViewVersion{}
	r := &ShardViewRegistry{stats: map[qviews.ShardID]*ShardStats{
		first: {UpVersion: &version, UpLoadInfoVersion: 1},
	}}
	require.False(t, r.AllShardsUp(nil, 1))
	require.True(t, r.AllShardsUp([]qviews.ShardID{first}, 1))
	require.False(t, r.AllShardsUp([]qviews.ShardID{first, missing}, 1))
	require.False(t, r.AllShardsUp([]qviews.ShardID{first, secondReplica}, 1))
	r.stats[missing] = &ShardStats{}
	require.False(t, r.AllShardsUp([]qviews.ShardID{first, missing}, 1))
	r.stats[missing] = &ShardStats{UpVersion: &version}
	require.False(t, r.AllShardsUp([]qviews.ShardID{first, missing}, 1))
	r.stats[missing] = &ShardStats{UpVersion: &version, UpLoadInfoVersion: 1}
	require.True(t, r.AllShardsUp([]qviews.ShardID{first, missing}, 1))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		r.AllShardsUp([]qviews.ShardID{first, missing}, 1)
	}))
}

func TestAllShardsUpRequiresMatchingLoadInfoVersion(t *testing.T) {
	first := qviews.ShardID{ReplicaID: 1, VChannel: "v0"}
	second := qviews.ShardID{ReplicaID: 2, VChannel: "v0"}
	version := qviews.QueryViewVersion{}
	r := &ShardViewRegistry{stats: map[qviews.ShardID]*ShardStats{
		first:  {UpVersion: &version, UpLoadInfoVersion: 1},
		second: {UpVersion: &version, UpLoadInfoVersion: 2},
	}}
	shards := []qviews.ShardID{first, second}
	require.False(t, r.AllShardsUp(shards, 2), "every replica must apply the load config")
	r.stats[first] = &ShardStats{UpVersion: &version, UpLoadInfoVersion: 2}
	require.True(t, r.AllShardsUp(shards, 2))
	r.stats[first] = &ShardStats{UpVersion: &version, UpLoadInfoVersion: 3}
	require.False(t, r.AllShardsUp(shards, 2), "the view must match the observed config exactly")
	require.False(t, r.AllShardsUp(shards, 0))
}
