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

package transformlog

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type fakeMaterializeAllocator struct {
	id int64
}

func (a *fakeMaterializeAllocator) Alloc(count uint32) (allocator.UniqueID, allocator.UniqueID, error) {
	begin := a.id
	a.id += allocator.UniqueID(count)
	return begin, a.id, nil
}

func (a *fakeMaterializeAllocator) AllocOne() (allocator.UniqueID, error) {
	a.id++
	return a.id, nil
}

type recordingPendingL0Recorder struct {
	mu      sync.Mutex
	records []*datapb.SaveBinlogPathsRequest
	removed []int64
	recErr  error
}

func (r *recordingPendingL0Recorder) Record(_ context.Context, req *datapb.SaveBinlogPathsRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recErr != nil {
		return r.recErr
	}
	r.records = append(r.records, req)
	return nil
}

func (r *recordingPendingL0Recorder) Remove(_ context.Context, segmentID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, segmentID)
	return nil
}

func (r *recordingPendingL0Recorder) calls() ([]*datapb.SaveBinlogPathsRequest, []int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.records, r.removed
}

func newOutboxTestMaterializer(t *testing.T, recorder *recordingPendingL0Recorder, metaWriter syncmgr.MetaWriter) *SyncMaterializer {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	m := NewSyncMaterializer(cm, &fakeMaterializeAllocator{}, metaWriter, 7)
	if recorder != nil {
		m.WithPendingL0Recorder(recorder)
	}
	return m
}

func outboxTestRequest() MaterializeRequest {
	return MaterializeRequest{
		VChannel:       "by-dev-rootcoord-dml_0_1004v0",
		TargetTimeTick: 200,
		MaxRows:        1000,
		MaxBytes:       1 << 20,
		Entries: []*streamingpb.TransformLogEntry{{
			TimeTick: 100,
			Entry: &streamingpb.TransformLogEntry_Delete{
				Delete: &streamingpb.TransformDeleteEntry{
					Blocks: []*streamingpb.TransformDeleteBlock{{
						PartitionId: 1,
						PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
					}},
				},
			},
		}},
	}
}

func TestSyncMaterializerOutboxRecordsBeforeRegistration(t *testing.T) {
	metaWriter := syncmgr.NewMockMetaWriter(t)
	metaWriter.EXPECT().UpdateSync(mock.Anything, mock.Anything).Return(nil).Once()
	recorder := &recordingPendingL0Recorder{}
	m := newOutboxTestMaterializer(t, recorder, metaWriter)

	require.NoError(t, m.Materialize(context.Background(), outboxTestRequest()))

	records, removed := recorder.calls()
	require.Len(t, records, 1)
	assert.Empty(t, removed)
	req := records[0]
	assert.Equal(t, int64(1), req.GetSegmentID())
	assert.Equal(t, datapb.SegmentLevel_L0, req.GetSegLevel())
	assert.Equal(t, "by-dev-rootcoord-dml_0_1004v0", req.GetChannel())
	assert.Equal(t, int64(7), req.GetBase().GetSourceID())
	assert.Equal(t, int64(1), req.GetPartitionID())
	require.Len(t, req.GetCheckPoints(), 1)
	assert.Equal(t, uint64(200), req.GetCheckPoints()[0].GetPosition().GetTimestamp())
	require.Len(t, req.GetDeltalogs(), 1)
}

func TestSyncMaterializerOutboxRollsBackOnRegistrationFailure(t *testing.T) {
	metaWriter := syncmgr.NewMockMetaWriter(t)
	metaWriter.EXPECT().UpdateSync(mock.Anything, mock.Anything).Return(errors.New("mocked")).Once()
	recorder := &recordingPendingL0Recorder{}
	m := newOutboxTestMaterializer(t, recorder, metaWriter)

	require.Error(t, m.Materialize(context.Background(), outboxTestRequest()))

	records, removed := recorder.calls()
	require.Len(t, records, 1)
	assert.Equal(t, []int64{1}, removed)
}

func TestSyncMaterializerOutboxSkipsRegistrationOnRecordFailure(t *testing.T) {
	metaWriter := syncmgr.NewMockMetaWriter(t)
	// The registration must not run when the outbox record cannot be written.
	metaWriter.AssertNotCalled(t, "UpdateSync", mock.Anything, mock.Anything)
	recorder := &recordingPendingL0Recorder{recErr: errors.New("etcd down")}
	m := newOutboxTestMaterializer(t, recorder, metaWriter)

	require.Error(t, m.Materialize(context.Background(), outboxTestRequest()))
}
