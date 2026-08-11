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
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type publishedDataViewCatalog interface {
	GetDataViewVersionState(ctx context.Context, collectionID int64) (*viewpb.CollectionDataVersionState, error)
	SavePublishedDataView(ctx context.Context, state *viewpb.CollectionDataVersionState, view *viewpb.DataViewOfCollection) error
	ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error)
}

// CommitPublishedView completes an explicitly assigned Streaming epoch. A
// later ready epoch is retained but returns a retryable error until every
// earlier assigned epoch has completed.
func (m *dataViewManager) CommitPublishedView(
	ctx context.Context,
	collectionID int64,
	assignedVersion *viewpb.DataVersion,
	mutation PublishedMutation,
) (*viewpb.DataVersion, error) {
	if assignedVersion == nil || assignedVersion.GetStreamingVersion() <= 0 || assignedVersion.GetCompactVersion() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid assigned DataVersion for collection %d", collectionID)
	}
	if err := validatePublishedMutation(collectionID, mutation); err != nil {
		return nil, err
	}

	catalog, ok := m.catalog.(publishedDataViewCatalog)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("published data view catalog is not initialized")
	}
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, merr.WrapErrServiceNotReadyMsg("data view collection %d is terminal", collectionID)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
		return nil, err
	}

	assignedStreaming := assignedVersion.GetStreamingVersion()
	if published := state.versionState.GetPublishedDataVersion(); published.GetStreamingVersion() >= assignedStreaming {
		if err := verifyDurableAssignedPublication(ctx, catalog, collectionID, assignedVersion, mutation); err != nil {
			return nil, err
		}
		return cloneDataVersion(assignedVersion), nil
	}
	if state.pendingAssigned == nil {
		state.pendingAssigned = make(map[int64]struct{})
	}
	if state.readyPublications == nil {
		state.readyPublications = make(map[int64]PublishedMutation)
	}
	state.pendingAssigned[assignedStreaming] = struct{}{}
	state.readyPublications[assignedStreaming] = clonePublishedMutation(mutation)

	requestedPublished, err := m.drainReadyPublicationsLocked(ctx, state, catalog, assignedStreaming)
	if err != nil {
		return nil, err
	}
	if !requestedPublished {
		return nil, merr.WrapErrServiceUnavailableMsg(
			"publication of assigned DataVersion %d/0 for collection %d is waiting for an earlier assigned epoch",
			assignedStreaming,
			collectionID,
		)
	}
	return cloneDataVersion(assignedVersion), nil
}

// CommitStreamingView atomically allocates and publishes a new Streaming
// epoch for an explicit add-only membership mutation. Membership equality is
// the durable idempotency proof for retries after a lost response.
func (m *dataViewManager) CommitStreamingView(
	ctx context.Context,
	collectionID int64,
	mutation PublishedMutation,
) (*viewpb.DataVersion, error) {
	if len(mutation.Remove) != 0 || len(mutation.Add) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("Streaming DataView mutation for collection %d must be add-only", collectionID)
	}
	if err := validatePublishedMutation(collectionID, mutation); err != nil {
		return nil, err
	}
	catalog, ok := m.catalog.(publishedDataViewCatalog)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("published data view catalog is not initialized")
	}
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, merr.WrapErrServiceNotReadyMsg("data view collection %d is terminal", collectionID)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
		return nil, err
	}

	next := publishedMutationBase(collectionID, state.latestResident)
	changed := applyPublishedMutation(next, mutation)
	for _, membership := range mutation.Add {
		if !dataViewContainsMembership(next, membership) {
			return nil, merr.WrapErrDataIntegrityMsg(
				"Streaming DataView mutation for collection %d does not contain segment %d at %s/%d",
				collectionID,
				membership.SegmentID,
				membership.VChannel,
				membership.PartitionID,
			)
		}
	}
	if !changed {
		return dataVersionFromView(state.latestResident), nil
	}
	if pending := minimumPendingStreamingVersion(state.pendingAssigned); pending != 0 {
		return nil, merr.WrapErrServiceUnavailableMsg(
			"Streaming DataView mutation for collection %d is waiting for assigned epoch %d/0",
			collectionID,
			pending,
		)
	}
	current := state.versionState.GetPublishedDataVersion()
	if current == nil || current.GetStreamingVersion() == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no published Streaming epoch", collectionID)
	}
	nextStreaming := state.versionState.GetAllocatedStreamingVersion()
	if current.GetStreamingVersion() > nextStreaming {
		nextStreaming = current.GetStreamingVersion()
	}
	next.DataVersion = &viewpb.DataVersion{StreamingVersion: nextStreaming + 1}
	if err := m.persistPublishedLocked(ctx, state, catalog, next); err != nil {
		return nil, err
	}
	return dataVersionFromView(next), nil
}

// CommitRewrite atomically publishes an independent rewrite of the current
// published membership at the next compact version.
func (m *dataViewManager) CommitRewrite(
	ctx context.Context,
	collectionID int64,
	mutation PublishedMutation,
) (*viewpb.DataVersion, error) {
	if err := validatePublishedMutation(collectionID, mutation); err != nil {
		return nil, err
	}
	catalog, ok := m.catalog.(publishedDataViewCatalog)
	if !ok {
		return nil, merr.WrapErrServiceNotReadyMsg("published data view catalog is not initialized")
	}
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, merr.WrapErrServiceNotReadyMsg("data view collection %d is terminal", collectionID)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
		return nil, err
	}

	next := publishedMutationBase(collectionID, state.latestResident)
	applyPublishedMutation(next, mutation)
	if isDataViewMembershipEqual(state.latestResident, next) {
		return dataVersionFromView(state.latestResident), nil
	}
	current := state.versionState.GetPublishedDataVersion()
	if current == nil || current.GetStreamingVersion() == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no published Streaming epoch to rewrite", collectionID)
	}
	next.DataVersion = &viewpb.DataVersion{
		StreamingVersion: current.GetStreamingVersion(),
		CompactVersion:   current.GetCompactVersion() + 1,
	}
	if err := m.persistPublishedLocked(ctx, state, catalog, next); err != nil {
		return nil, err
	}
	return dataVersionFromView(next), nil
}

func validatePublishedMutation(collectionID int64, mutation PublishedMutation) error {
	for _, membership := range mutation.Add {
		if membership.SegmentID <= 0 || membership.CollectionID != collectionID || membership.VChannel == "" {
			return merr.WrapErrServiceInternalMsg(
				"invalid published membership for segment %d in collection %d",
				membership.SegmentID,
				collectionID,
			)
		}
		if !membership.loadable() {
			return merr.WrapErrServiceInternalMsg(
				"segment %d is not loadable published membership for collection %d",
				membership.SegmentID,
				collectionID,
			)
		}
	}
	return nil
}

func (m *dataViewManager) recoverPublicationStateLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog publishedDataViewCatalog,
) error {
	if state.publicationRecovered {
		return nil
	}
	if versionCatalog, ok := m.catalog.(flushVersionCatalog); ok {
		if segmentStore, ok := m.segments.(flushVersionSegmentStore); ok {
			if err := m.recoverFlushVersionStateLocked(ctx, state, versionCatalog, segmentStore); err != nil {
				return err
			}
		}
	}
	recoveredAllocated := state.versionState.GetAllocatedStreamingVersion()
	durable, published, err := recoverPublishedDataView(ctx, catalog, state.collectionID)
	if err != nil {
		return err
	}
	if durable == nil {
		durable = &viewpb.CollectionDataVersionState{CollectionId: state.collectionID}
	} else {
		durable = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
	}
	if recoveredAllocated > durable.GetAllocatedStreamingVersion() {
		durable.AllocatedStreamingVersion = recoveredAllocated
	}
	state.versionState = durable
	state.persistedAllocated = durable.GetAllocatedStreamingVersion()
	if published != nil {
		state.latestResident = canonicalDataViewClone(published)
		state.latestVisible = canonicalDataViewClone(published)
	}
	if state.pendingAssigned == nil {
		state.pendingAssigned = make(map[int64]struct{})
	}
	if state.readyPublications == nil {
		state.readyPublications = make(map[int64]PublishedMutation)
	}
	state.publicationRecovered = true
	return nil
}

func (m *dataViewManager) drainReadyPublicationsLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog publishedDataViewCatalog,
	requestedStreaming int64,
) (bool, error) {
	requestedPublished := false
	for {
		nextStreaming := minimumPendingStreamingVersion(state.pendingAssigned)
		if nextStreaming == 0 {
			return requestedPublished, nil
		}
		mutation, ready := state.readyPublications[nextStreaming]
		if !ready {
			return requestedPublished, nil
		}

		next := publishedMutationBase(state.collectionID, state.latestResident)
		applyPublishedMutation(next, mutation)
		if isDataViewMembershipEqual(state.latestResident, next) && !isAssignedRemoveOnlyCompletion(mutation) {
			return requestedPublished, nil
		}
		next.DataVersion = &viewpb.DataVersion{StreamingVersion: nextStreaming}
		if err := m.persistPublishedLocked(ctx, state, catalog, next); err != nil {
			return requestedPublished, err
		}
		delete(state.pendingAssigned, nextStreaming)
		delete(state.readyPublications, nextStreaming)
		if nextStreaming == requestedStreaming {
			requestedPublished = true
		}
	}
}

func isAssignedRemoveOnlyCompletion(mutation PublishedMutation) bool {
	return len(mutation.Add) == 0 && len(mutation.Remove) > 0
}

func verifyDurableAssignedPublication(
	ctx context.Context,
	catalog publishedDataViewCatalog,
	collectionID int64,
	assignedVersion *viewpb.DataVersion,
	mutation PublishedMutation,
) error {
	views, err := catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return merr.Wrapf(err, "list DataView snapshots for assigned publication %d/0 of collection %d",
			assignedVersion.GetStreamingVersion(), collectionID)
	}
	var target *viewpb.DataViewOfCollection
	var predecessor *viewpb.DataViewOfCollection
	for _, view := range views {
		if view.GetCollectionId() != collectionID {
			return merr.WrapErrDataIntegrityMsg(
				"assigned publication collection mismatch: requested=%d, stored=%d",
				collectionID,
				view.GetCollectionId(),
			)
		}
		switch comparison := compareDataVersion(view.GetDataVersion(), assignedVersion); {
		case comparison == 0:
			if target != nil && !proto.Equal(canonicalDataViewClone(target), canonicalDataViewClone(view)) {
				return merr.WrapErrDataIntegrityMsg(
					"assigned publication %d/0 for collection %d has conflicting durable snapshots",
					assignedVersion.GetStreamingVersion(),
					collectionID,
				)
			}
			target = view
		case comparison < 0 && (predecessor == nil || compareDataVersion(view.GetDataVersion(), predecessor.GetDataVersion()) > 0):
			predecessor = view
		}
	}
	if target == nil {
		return merr.WrapErrDataIntegrityMsg(
			"assigned publication %d/0 is missing for collection %d",
			assignedVersion.GetStreamingVersion(),
			collectionID,
		)
	}
	if assignedVersion.GetStreamingVersion() > 1 && predecessor == nil {
		return merr.WrapErrDataIntegrityMsg(
			"assigned publication %d/0 for collection %d has no durable predecessor for exact retry verification",
			assignedVersion.GetStreamingVersion(),
			collectionID,
		)
	}

	expected := publishedMutationBase(collectionID, predecessor)
	applyPublishedMutation(expected, mutation)
	expected.DataVersion = cloneDataVersion(assignedVersion)
	expected = cloneDataViewWithoutDeleteTimetick(expected)
	canonicalizeDataView(expected)
	actual := cloneDataViewWithoutDeleteTimetick(target)
	canonicalizeDataView(actual)
	if !proto.Equal(expected, actual) {
		return merr.WrapErrDataIntegrityMsg(
			"assigned publication %d/0 for collection %d does not exactly match the requested mutation",
			assignedVersion.GetStreamingVersion(),
			collectionID,
		)
	}
	return nil
}

func (m *dataViewManager) persistPublishedLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog publishedDataViewCatalog,
	view *viewpb.DataViewOfCollection,
) error {
	toPersist := cloneDataViewWithoutDeleteTimetick(view)
	nextState := proto.Clone(state.versionState).(*viewpb.CollectionDataVersionState)
	nextState.CollectionId = state.collectionID
	if toPersist.GetDataVersion().GetStreamingVersion() > nextState.GetAllocatedStreamingVersion() {
		nextState.AllocatedStreamingVersion = toPersist.GetDataVersion().GetStreamingVersion()
	}
	nextState.PublishedDataVersion = cloneDataVersion(toPersist.GetDataVersion())
	if err := catalog.SavePublishedDataView(ctx, nextState, toPersist); err != nil {
		return merr.Wrapf(err, "publish DataView for collection %d", state.collectionID)
	}
	state.versionState = nextState
	state.persistedAllocated = nextState.GetAllocatedStreamingVersion()
	state.latestResident = canonicalDataViewClone(toPersist)
	state.latestVisible = canonicalDataViewClone(toPersist)
	m.rememberRecoveredDataView(toPersist)
	return nil
}

func publishedMutationBase(collectionID int64, current *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	next := canonicalDataViewClone(current)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: collectionID}
	}
	return next
}

func minimumPendingStreamingVersion(pending map[int64]struct{}) int64 {
	minimum := int64(math.MaxInt64)
	for streamingVersion := range pending {
		if streamingVersion < minimum {
			minimum = streamingVersion
		}
	}
	if minimum == math.MaxInt64 {
		return 0
	}
	return minimum
}

func clonePublishedMutation(mutation PublishedMutation) PublishedMutation {
	return PublishedMutation{
		Add:    append([]SegmentMembership(nil), mutation.Add...),
		Remove: append([]int64(nil), mutation.Remove...),
	}
}
