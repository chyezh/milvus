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

package streamingnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCatalogPendingL0Segments(t *testing.T) {
	catalog := newTestEtcdCatalog(t, "test-pending-l0")
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	// Initially empty.
	pendings, err := catalog.ListPendingL0Segments(ctx, pchannel)
	require.NoError(t, err)
	assert.Empty(t, pendings)

	// Round-trip two pending registrations.
	req1 := &datapb.SaveBinlogPathsRequest{
		SegmentID:    101,
		CollectionID: 1004,
		PartitionID:  1,
		Channel:      "v1",
		SegLevel:     datapb.SegmentLevel_L0,
		CheckPoints:  []*datapb.CheckPoint{{SegmentID: 101, Position: &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 100}}},
		Deltalogs:    []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 11, LogPath: "a/b/101"}}}},
	}
	req2 := &datapb.SaveBinlogPathsRequest{
		SegmentID:    102,
		CollectionID: 1004,
		PartitionID:  1,
		Channel:      "v1",
		SegLevel:     datapb.SegmentLevel_L0,
		CheckPoints:  []*datapb.CheckPoint{{SegmentID: 102, Position: &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 200}}},
	}
	require.NoError(t, catalog.SavePendingL0Segment(ctx, pchannel, req1))
	require.NoError(t, catalog.SavePendingL0Segment(ctx, pchannel, req2))

	pendings, err = catalog.ListPendingL0Segments(ctx, pchannel)
	require.NoError(t, err)
	assert.Len(t, pendings, 2)
	byID := make(map[int64]*datapb.SaveBinlogPathsRequest, len(pendings))
	for _, pending := range pendings {
		byID[pending.GetSegmentID()] = pending
	}
	assert.Equal(t, uint64(100), byID[101].GetCheckPoints()[0].GetPosition().GetTimestamp())
	assert.Equal(t, "a/b/101", byID[101].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	assert.Equal(t, uint64(200), byID[102].GetCheckPoints()[0].GetPosition().GetTimestamp())

	// Re-saving the same segment overwrites.
	req1Overwrite := proto.Clone(req1).(*datapb.SaveBinlogPathsRequest)
	req1Overwrite.CheckPoints[0].Position.Timestamp = 150
	require.NoError(t, catalog.SavePendingL0Segment(ctx, pchannel, req1Overwrite))
	pendings, err = catalog.ListPendingL0Segments(ctx, pchannel)
	require.NoError(t, err)
	assert.Len(t, pendings, 2)
	for _, pending := range pendings {
		if pending.GetSegmentID() == 101 {
			assert.Equal(t, uint64(150), pending.GetCheckPoints()[0].GetPosition().GetTimestamp())
		}
	}

	// Another pchannel is isolated.
	other, err := catalog.ListPendingL0Segments(ctx, pchannel+"-other")
	require.NoError(t, err)
	assert.Empty(t, other)

	// Remove one; removing an absent segment is a no-op.
	require.NoError(t, catalog.RemovePendingL0Segments(ctx, pchannel, []int64{101, 999}))
	pendings, err = catalog.ListPendingL0Segments(ctx, pchannel)
	require.NoError(t, err)
	assert.Len(t, pendings, 1)
	assert.Equal(t, int64(102), pendings[0].GetSegmentID())
}
