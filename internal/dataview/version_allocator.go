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

package dataview

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type flushVersionCatalog interface {
	GetDataViewVersionState(ctx context.Context, collectionID int64) (*viewpb.CollectionDataVersionState, error)
	SaveDataViewVersionState(ctx context.Context, state *viewpb.CollectionDataVersionState) error
}

type flushVersionSegmentStore interface {
	SaveSealedAtDataVersion(ctx context.Context, segmentID int64, version *viewpb.DataVersion) error
	ListAllSegmentsForVersionAllocation(ctx context.Context, collectionID int64) []*Segment
}

func (m *dataViewManager) AssignFlushVersion(
	ctx context.Context,
	collectionID int64,
	segmentID int64,
) (*viewpb.DataVersion, error) {
	catalog, ok := m.catalog.(flushVersionCatalog)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("data view version catalog is not initialized")
	}
	segments, ok := m.segments.(flushVersionSegmentStore)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("data view segment version store is not initialized")
	}

	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, merr.WrapErrServiceNotReadyMsg("data view collection %d is terminal", collectionID)
	}

	segment := m.segments.GetSegment(ctx, segmentID)
	if segment == nil {
		return nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if segment.GetCollectionID() != collectionID {
		return nil, merr.WrapErrParameterInvalidMsg(
			"flush segment %d belongs to collection %d, requested collection %d",
			segmentID,
			segment.GetCollectionID(),
			collectionID,
		)
	}

	if err := m.recoverFlushVersionStateLocked(ctx, state, catalog, segments); err != nil {
		return nil, err
	}
	if assigned := segment.GetSealedAtDataVersion(); assigned != nil {
		if assigned.GetStreamingVersion() <= 0 || assigned.GetCompactVersion() != 0 {
			return nil, merr.WrapErrDataIntegrityMsg(
				"segment %d has invalid sealed data version %d/%d",
				segmentID,
				assigned.GetStreamingVersion(),
				assigned.GetCompactVersion(),
			)
		}
		if assigned.GetStreamingVersion() > state.persistedAllocated {
			if err := m.saveAllocatedStreamingVersionLocked(ctx, state, catalog, assigned.GetStreamingVersion()); err != nil {
				return nil, err
			}
		}
		return proto.Clone(assigned).(*viewpb.DataVersion), nil
	}

	assigned := &viewpb.DataVersion{
		StreamingVersion: state.versionState.GetAllocatedStreamingVersion() + 1,
	}
	if err := segments.SaveSealedAtDataVersion(ctx, segmentID, assigned); err != nil {
		return nil, flushVersionPersistenceError(
			"persist sealed data version for segment",
			err,
		)
	}
	advanceAllocatedStreamingVersionLocked(state, assigned.GetStreamingVersion())
	if err := m.saveAllocatedStreamingVersionLocked(ctx, state, catalog, assigned.GetStreamingVersion()); err != nil {
		return nil, err
	}
	return proto.Clone(assigned).(*viewpb.DataVersion), nil
}

func (m *dataViewManager) recoverFlushVersionStateLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog flushVersionCatalog,
	segments flushVersionSegmentStore,
) error {
	if state.versionStateRecovered {
		return nil
	}

	durable, err := catalog.GetDataViewVersionState(ctx, state.collectionID)
	if err != nil {
		return flushVersionPersistenceError("load data view version state", err)
	}
	if durable == nil {
		durable = &viewpb.CollectionDataVersionState{CollectionId: state.collectionID}
	} else if durable.GetCollectionId() != state.collectionID {
		return merr.WrapErrDataIntegrityMsg(
			"data view version state collection mismatch: requested=%d, stored=%d",
			state.collectionID,
			durable.GetCollectionId(),
		)
	} else {
		durable = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
	}

	allocated := durable.GetAllocatedStreamingVersion()
	state.persistedAllocated = allocated
	if published := durable.GetPublishedDataVersion(); published.GetStreamingVersion() > allocated {
		allocated = published.GetStreamingVersion()
	}
	if resident := dataVersionFromView(state.latestResident); resident.GetStreamingVersion() > allocated {
		allocated = resident.GetStreamingVersion()
	}
	for _, segment := range segments.ListAllSegmentsForVersionAllocation(ctx, state.collectionID) {
		if assigned := segment.GetSealedAtDataVersion(); assigned.GetStreamingVersion() > allocated {
			allocated = assigned.GetStreamingVersion()
		}
	}
	durable.AllocatedStreamingVersion = allocated
	state.versionState = durable
	state.versionStateRecovered = true
	return nil
}

func advanceAllocatedStreamingVersionLocked(state *collectionDataViewState, streamingVersion int64) {
	if streamingVersion <= state.versionState.GetAllocatedStreamingVersion() {
		return
	}
	next := proto.Clone(state.versionState).(*viewpb.CollectionDataVersionState)
	next.AllocatedStreamingVersion = streamingVersion
	state.versionState = next
}

func (m *dataViewManager) saveAllocatedStreamingVersionLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog flushVersionCatalog,
	streamingVersion int64,
) error {
	next := proto.Clone(state.versionState).(*viewpb.CollectionDataVersionState)
	if streamingVersion > next.GetAllocatedStreamingVersion() {
		next.AllocatedStreamingVersion = streamingVersion
	}
	if err := catalog.SaveDataViewVersionState(ctx, next); err != nil {
		return flushVersionPersistenceError("persist allocated streaming version", err)
	}
	state.versionState = next
	state.persistedAllocated = next.GetAllocatedStreamingVersion()
	return nil
}

func flushVersionPersistenceError(operation string, err error) error {
	if merr.IsMilvusError(err) {
		return merr.Wrap(err, operation)
	}
	return merr.WrapErrServiceUnavailable(operation, err.Error())
}
