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

package recovery

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	mock_metastore "github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newPendingL0Request(segmentID int64, vchannel string, ts uint64) *datapb.SaveBinlogPathsRequest {
	return &datapb.SaveBinlogPathsRequest{
		SegmentID:    segmentID,
		CollectionID: 1004,
		PartitionID:  1,
		Channel:      vchannel,
		SegLevel:     datapb.SegmentLevel_L0,
		CheckPoints: []*datapb.CheckPoint{{
			SegmentID: segmentID,
			Position:  &msgpb.MsgPosition{ChannelName: vchannel, Timestamp: ts},
		}},
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{{LogID: segmentID, LogPath: "a/b/" + vchannel + "/" + strconv.FormatInt(segmentID, 10)}},
		}},
	}
}

func TestRecoverPendingL0Segments_ReRegistersAndRemoves(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	pending := newPendingL0Request(101, "v1", 200)
	catalog.EXPECT().ListPendingL0Segments(mock.Anything, pchannel).Return([]*datapb.SaveBinlogPathsRequest{pending}, nil)
	coord.EXPECT().SaveBinlogPaths(mock.Anything, mock.MatchedBy(func(req *datapb.SaveBinlogPathsRequest) bool {
		return req.GetSegmentID() == 101 &&
			req.GetBase().GetSourceID() == paramtable.GetNodeID() &&
			len(req.GetDeltalogs()) == 1
	}), mock.Anything).Return(merr.Success(), nil)
	catalog.EXPECT().RemovePendingL0Segments(mock.Anything, pchannel, []int64{101}).Return(nil)

	checkpoints, err := recoverPendingL0Segments(ctx, coord, catalog, pchannel)
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"v1": 200}, checkpoints)
}

func TestRecoverPendingL0Segments_TakesMaxPerVChannel(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	p1 := newPendingL0Request(101, "v1", 200)
	p2 := newPendingL0Request(102, "v1", 150)
	p3 := newPendingL0Request(103, "v2", 300)
	catalog.EXPECT().ListPendingL0Segments(mock.Anything, pchannel).Return([]*datapb.SaveBinlogPathsRequest{p1, p2, p3}, nil)
	coord.EXPECT().SaveBinlogPaths(mock.Anything, mock.MatchedBy(func(req *datapb.SaveBinlogPathsRequest) bool {
		return req.GetSegmentID() == 101 || req.GetSegmentID() == 102 || req.GetSegmentID() == 103
	}), mock.Anything).Return(merr.Success(), nil).Times(3)
	catalog.EXPECT().RemovePendingL0Segments(mock.Anything, pchannel, []int64{101, 102, 103}).Return(nil)

	checkpoints, err := recoverPendingL0Segments(ctx, coord, catalog, pchannel)
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"v1": 200, "v2": 300}, checkpoints)
}

func TestRecoverPendingL0Segments_NoPending(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	catalog.EXPECT().ListPendingL0Segments(mock.Anything, pchannel).Return(nil, nil)

	checkpoints, err := recoverPendingL0Segments(ctx, coord, catalog, pchannel)
	require.NoError(t, err)
	assert.Nil(t, checkpoints)
}

func TestRecoverPendingL0Segments_ListFails(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	catalog.EXPECT().ListPendingL0Segments(mock.Anything, pchannel).Return(nil, errors.New("etcd down"))

	_, err := recoverPendingL0Segments(ctx, coord, catalog, pchannel)
	require.Error(t, err)
}

func TestRecoverPendingL0Segments_ReRegisterFailsKeepsRecord(t *testing.T) {
	coord := mocks.NewMockMixCoordClient(t)
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	ctx := context.Background()
	pchannel := "by-dev-rootcoord-dml_0_1004v0"

	pending := newPendingL0Request(101, "v1", 200)
	catalog.EXPECT().ListPendingL0Segments(mock.Anything, pchannel).Return([]*datapb.SaveBinlogPathsRequest{pending}, nil)
	coord.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rpc failed"))
	// No Remove expectation: the record must survive for a retried recovery.

	_, err := recoverPendingL0Segments(ctx, coord, catalog, pchannel)
	require.Error(t, err)
}
