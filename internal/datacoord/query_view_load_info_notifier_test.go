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

package datacoord

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestIndexDDLCallbacksNotifyQueryViewLoadInfo(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	recorder := newQueryViewLoadInfoNotificationRecorder()
	callbacks := &DDLCallbacks{Server: &Server{
		meta:                      meta,
		notifyIndexChan:           make(chan UniqueID, 1),
		queryViewLoadInfoNotifier: recorder,
	}}
	index := &model.Index{
		CollectionID: 100,
		FieldID:      101,
		IndexID:      102,
		IndexName:    "json_path_index",
	}

	create := message.BroadcastResultCreateIndexMessageV2{
		Message: message.MustAsBroadcastCreateIndexMessageV2(message.NewCreateIndexMessageBuilderV2().
			WithHeader(&message.CreateIndexMessageHeader{
				CollectionId: index.CollectionID,
				FieldId:      index.FieldID,
				IndexId:      index.IndexID,
				IndexName:    index.IndexName,
			}).
			WithBody(&message.CreateIndexMessageBody{FieldIndex: model.MarshalIndexModel(index)}).
			WithBroadcast([]string{"control"}).
			MustBuildBroadcast()),
	}
	require.NoError(t, callbacks.createIndexV2AckCallback(context.Background(), create))
	assert.Empty(t, recorder.collections())
	assert.Empty(t, recorder.segments())

	altered := model.CloneIndex(index)
	altered.UserIndexParams = []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}}
	alter := message.BroadcastResultAlterIndexMessageV2{
		Message: message.MustAsBroadcastAlterIndexMessageV2(message.NewAlterIndexMessageBuilderV2().
			WithHeader(&message.AlterIndexMessageHeader{
				CollectionId: index.CollectionID,
				IndexIds:     []int64{index.IndexID},
			}).
			WithBody(&message.AlterIndexMessageBody{FieldIndexes: []*indexpb.FieldIndex{model.MarshalIndexModel(altered)}}).
			WithBroadcast([]string{"control"}).
			MustBuildBroadcast()),
	}
	require.NoError(t, callbacks.alterIndexV2AckCallback(context.Background(), alter))
	assert.Equal(t, []int64{index.CollectionID}, recorder.collections())

	recorder.reset()
	drop := message.BroadcastResultDropIndexMessageV2{
		Message: message.MustAsBroadcastDropIndexMessageV2(message.NewDropIndexMessageBuilderV2().
			WithHeader(&message.DropIndexMessageHeader{
				CollectionId: index.CollectionID,
				IndexIds:     []int64{index.IndexID},
			}).
			WithBody(&message.DropIndexMessageBody{}).
			WithBroadcast([]string{"control"}).
			MustBuildBroadcast()),
	}
	require.NoError(t, callbacks.dropIndexV2Callback(context.Background(), drop))
	assert.Equal(t, []int64{index.CollectionID}, recorder.collections())
}

func TestIndexDDLCallbacksDoNotNotifyQueryViewLoadInfoOnPersistenceFailure(t *testing.T) {
	index := &model.Index{
		CollectionID: 100,
		FieldID:      101,
		IndexID:      102,
		IndexName:    "json_path_index",
	}

	t.Run("alter index", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		require.NoError(t, err)
		recorder := newQueryViewLoadInfoNotificationRecorder()
		callbacks := &DDLCallbacks{Server: &Server{
			meta:                      meta,
			queryViewLoadInfoNotifier: recorder,
		}}
		patch := mockey.Mock((*indexMeta).AlterIndex).Return(errors.New("persist alter index failed")).Build()
		defer patch.UnPatch()

		result := message.BroadcastResultAlterIndexMessageV2{
			Message: message.MustAsBroadcastAlterIndexMessageV2(message.NewAlterIndexMessageBuilderV2().
				WithHeader(&message.AlterIndexMessageHeader{
					CollectionId: index.CollectionID,
					IndexIds:     []int64{index.IndexID},
				}).
				WithBody(&message.AlterIndexMessageBody{FieldIndexes: []*indexpb.FieldIndex{model.MarshalIndexModel(index)}}).
				WithBroadcast([]string{"control"}).
				MustBuildBroadcast()),
		}
		require.Error(t, callbacks.alterIndexV2AckCallback(context.Background(), result))
		assert.Empty(t, recorder.collections())
	})

	t.Run("drop index", func(t *testing.T) {
		meta, err := newMemoryMeta(t)
		require.NoError(t, err)
		recorder := newQueryViewLoadInfoNotificationRecorder()
		callbacks := &DDLCallbacks{Server: &Server{
			meta:                      meta,
			queryViewLoadInfoNotifier: recorder,
		}}
		patch := mockey.Mock((*indexMeta).MarkIndexAsDeleted).Return(errors.New("persist drop index failed")).Build()
		defer patch.UnPatch()

		result := message.BroadcastResultDropIndexMessageV2{
			Message: message.MustAsBroadcastDropIndexMessageV2(message.NewDropIndexMessageBuilderV2().
				WithHeader(&message.DropIndexMessageHeader{
					CollectionId: index.CollectionID,
					IndexIds:     []int64{index.IndexID},
				}).
				WithBody(&message.DropIndexMessageBody{}).
				WithBroadcast([]string{"control"}).
				MustBuildBroadcast()),
		}
		require.Error(t, callbacks.dropIndexV2Callback(context.Background(), result))
		assert.Empty(t, recorder.collections())
	})
}

type queryViewLoadInfoNotification struct {
	collectionID int64
	segmentIDs   []int64
}

type queryViewLoadInfoNotificationRecorder struct {
	mu                      sync.Mutex
	segmentNotifications    []queryViewLoadInfoNotification
	collectionNotifications []int64
}

func newQueryViewLoadInfoNotificationRecorder() *queryViewLoadInfoNotificationRecorder {
	return &queryViewLoadInfoNotificationRecorder{}
}

func (r *queryViewLoadInfoNotificationRecorder) NotifySegments(collectionID int64, segmentIDs ...int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segmentNotifications = append(r.segmentNotifications, queryViewLoadInfoNotification{
		collectionID: collectionID,
		segmentIDs:   append([]int64(nil), segmentIDs...),
	})
}

func (r *queryViewLoadInfoNotificationRecorder) NotifyCollection(collectionID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectionNotifications = append(r.collectionNotifications, collectionID)
}

func (r *queryViewLoadInfoNotificationRecorder) segments() []queryViewLoadInfoNotification {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queryViewLoadInfoNotification(nil), r.segmentNotifications...)
}

func (r *queryViewLoadInfoNotificationRecorder) collections() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.collectionNotifications...)
}

func (r *queryViewLoadInfoNotificationRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segmentNotifications = nil
	r.collectionNotifications = nil
}
