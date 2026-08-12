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
	"fmt"
	"math"
	"sort"

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

// RetryAssignedFlushPublication resolves a flush retry after the segment has
// already moved to Dropped. A durable published head plus the exact assigned
// snapshot proves a prior successful publication without reconstructing the
// original mutation from the segment's later lifecycle state. If the head is
// still behind, only an explicit remove-only flush completion can publish the
// assigned epoch without adding membership.
func (m *dataViewManager) RetryAssignedFlushPublication(
	ctx context.Context,
	collectionID int64,
	segmentID int64,
	assignedVersion *viewpb.DataVersion,
	removeOnly bool,
) (*viewpb.DataVersion, error) {
	if assignedVersion == nil || assignedVersion.GetStreamingVersion() <= 0 || assignedVersion.GetCompactVersion() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid assigned DataVersion for collection %d", collectionID)
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
	if state.versionState.GetPublishedDataVersion().GetStreamingVersion() >= assignedStreaming {
		if err := verifyDurableAssignedSnapshot(ctx, catalog, collectionID, assignedVersion); err != nil {
			return nil, err
		}
		return cloneDataVersion(assignedVersion), nil
	}
	if !removeOnly {
		return nil, merr.WrapErrServiceUnavailableMsg(
			"publication of assigned DataVersion %d/0 for dropped non-empty segment %d in collection %d is not durable",
			assignedStreaming,
			segmentID,
			collectionID,
		)
	}

	if state.pendingAssigned == nil {
		state.pendingAssigned = make(map[int64]struct{})
	}
	if state.readyPublications == nil {
		state.readyPublications = make(map[int64]PublishedMutation)
	}
	state.pendingAssigned[assignedStreaming] = struct{}{}
	state.readyPublications[assignedStreaming] = PublishedMutation{Remove: []int64{segmentID}}
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

	next := publishedMutationBase(collectionID, state.published)
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
		return dataVersionFromView(state.published), nil
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
	return m.commitRewriteLocked(ctx, state, catalog, mutation)
}

// CommitSegmentTrim persists the scoped SegmentMeta removal fence before it
// completes targeted unpublished assigned epochs and removes published targets.
func (m *dataViewManager) CommitSegmentTrim(
	ctx context.Context,
	collectionID int64,
	resolveTargets SegmentTrimTargetResolver,
	finalize SegmentTrimFinalize,
) (*viewpb.DataVersion, error) {
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
	if resolveTargets == nil {
		return nil, merr.WrapErrServiceInternalMsg("target resolver is nil for DataView trim of collection %d", collectionID)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, catalog); err != nil {
		return nil, err
	}
	if err := m.refreshDurablePublicationLocked(ctx, state, catalog); err != nil {
		return nil, err
	}
	targetIDs, err := resolveSegmentTrimTargetIDs(ctx, resolveTargets, collectionID)
	if err != nil {
		return nil, err
	}
	assignedTargets, err := m.resolveSegmentTrimTargetsLocked(ctx, collectionID, targetIDs)
	if err != nil {
		return nil, err
	}
	if err := validateSegmentTrimOrderingLocked(state, assignedTargets); err != nil {
		return nil, err
	}
	if finalize != nil {
		if err := finalize(ctx); err != nil {
			return nil, err
		}
	}

	targetIDs, err = resolveSegmentTrimTargetIDs(ctx, resolveTargets, collectionID)
	if err != nil {
		return nil, err
	}
	assignedTargets, err = m.resolveSegmentTrimTargetsLocked(ctx, collectionID, targetIDs)
	if err != nil {
		return nil, err
	}
	if err := validateSegmentTrimOrderingLocked(state, assignedTargets); err != nil {
		return nil, err
	}

	for _, target := range assignedTargets {
		streamingVersion := target.assignedVersion.GetStreamingVersion()
		if streamingVersion <= state.versionState.GetPublishedDataVersion().GetStreamingVersion() {
			continue
		}
		state.pendingAssigned[streamingVersion] = struct{}{}
		state.readyPublications[streamingVersion] = PublishedMutation{Remove: []int64{target.segmentID}}
		if pending := minimumPendingStreamingVersion(state.pendingAssigned); pending != streamingVersion {
			return nil, merr.WrapErrServiceUnavailableMsg(
				"trim of assigned DataVersion %d/0 for collection %d is waiting for earlier assigned epoch %d/0",
				streamingVersion,
				collectionID,
				pending,
			)
		}
		next := publishedMutationBase(collectionID, state.published)
		applyPublishedMutation(next, state.readyPublications[streamingVersion])
		next.DataVersion = &viewpb.DataVersion{StreamingVersion: streamingVersion}
		if err := m.persistPublishedLocked(ctx, state, catalog, next); err != nil {
			return nil, err
		}
		delete(state.pendingAssigned, streamingVersion)
		delete(state.readyPublications, streamingVersion)
	}

	publishedTargets := make([]int64, 0, len(targetIDs))
	for _, segmentID := range targetIDs {
		if dataViewContainsSegment(state.published, segmentID) {
			publishedTargets = append(publishedTargets, segmentID)
		}
	}
	if len(publishedTargets) == 0 {
		return dataVersionFromView(state.published), nil
	}
	version, err := m.commitRewriteLocked(ctx, state, catalog, PublishedMutation{Remove: publishedTargets})
	if err != nil {
		return nil, err
	}
	return version, nil
}

func resolveSegmentTrimTargetIDs(
	ctx context.Context,
	resolveTargets SegmentTrimTargetResolver,
	collectionID int64,
) ([]int64, error) {
	targetIDs := append([]int64(nil), resolveTargets(ctx)...)
	for _, segmentID := range targetIDs {
		if segmentID <= 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid segment %d for DataView trim of collection %d", segmentID, collectionID)
		}
	}
	return targetIDs, nil
}

func validateSegmentTrimOrderingLocked(
	state *collectionDataViewState,
	assignedTargets []resolvedSegmentTrimTarget,
) error {
	pending := make(map[int64]struct{}, len(state.pendingAssigned)+len(assignedTargets))
	for streamingVersion := range state.pendingAssigned {
		pending[streamingVersion] = struct{}{}
	}
	publishedStreaming := state.versionState.GetPublishedDataVersion().GetStreamingVersion()
	for _, target := range assignedTargets {
		streamingVersion := target.assignedVersion.GetStreamingVersion()
		if streamingVersion > publishedStreaming {
			pending[streamingVersion] = struct{}{}
		}
	}
	for _, target := range assignedTargets {
		streamingVersion := target.assignedVersion.GetStreamingVersion()
		if streamingVersion <= publishedStreaming {
			continue
		}
		if earlier := minimumPendingStreamingVersion(pending); earlier != streamingVersion {
			return merr.WrapErrServiceUnavailableMsg(
				"trim of assigned DataVersion %d/0 for collection %d is waiting for earlier assigned epoch %d/0",
				streamingVersion,
				state.collectionID,
				earlier,
			)
		}
		delete(pending, streamingVersion)
		publishedStreaming = streamingVersion
	}
	return nil
}

func (m *dataViewManager) resolveSegmentTrimTargetsLocked(
	ctx context.Context,
	collectionID int64,
	targetIDs []int64,
) ([]resolvedSegmentTrimTarget, error) {
	assignedTargets := make([]resolvedSegmentTrimTarget, 0, len(targetIDs))
	for _, segmentID := range targetIDs {
		segment := m.segments.GetSegment(ctx, segmentID)
		if segment == nil {
			return nil, merr.WrapErrServiceUnavailableMsg(
				"segment %d disappeared before DataView trim of collection %d",
				segmentID,
				collectionID,
			)
		}
		if segment.GetCollectionID() != collectionID {
			return nil, merr.WrapErrDataIntegrityMsg(
				"trimmed segment %d belongs to collection %d, requested collection %d",
				segmentID,
				segment.GetCollectionID(),
				collectionID,
			)
		}
		assigned := segment.GetSealedAtDataVersion()
		if assigned == nil {
			continue
		}
		if assigned.GetStreamingVersion() <= 0 || assigned.GetCompactVersion() != 0 {
			return nil, merr.WrapErrDataIntegrityMsg(
				"trimmed segment %d has invalid assigned DataVersion %d/%d in collection %d",
				segmentID,
				assigned.GetStreamingVersion(),
				assigned.GetCompactVersion(),
				collectionID,
			)
		}
		assignedTargets = append(assignedTargets, resolvedSegmentTrimTarget{
			segmentID:       segmentID,
			assignedVersion: cloneDataVersion(assigned),
		})
	}
	sort.Slice(assignedTargets, func(i, j int) bool {
		return assignedTargets[i].assignedVersion.GetStreamingVersion() < assignedTargets[j].assignedVersion.GetStreamingVersion()
	})
	return assignedTargets, nil
}

type resolvedSegmentTrimTarget struct {
	segmentID       int64
	assignedVersion *viewpb.DataVersion
}

func (m *dataViewManager) refreshDurablePublicationLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog publishedDataViewCatalog,
) error {
	durable, published, err := recoverPublishedDataView(ctx, catalog, state.collectionID)
	if err != nil {
		return err
	}
	if durable == nil || durable.GetPublishedDataVersion() == nil || published == nil {
		return merr.WrapErrServiceNotReadyMsg("collection %d has no durable published DataView", state.collectionID)
	}
	current := state.versionState.GetPublishedDataVersion()
	if compareDataVersion(durable.GetPublishedDataVersion(), current) < 0 {
		return merr.WrapErrDataIntegrityMsg(
			"durable published DataView for collection %d regressed from %d/%d to %d/%d",
			state.collectionID,
			current.GetStreamingVersion(),
			current.GetCompactVersion(),
			durable.GetPublishedDataVersion().GetStreamingVersion(),
			durable.GetPublishedDataVersion().GetCompactVersion(),
		)
	}
	persistedAllocated := durable.GetAllocatedStreamingVersion()
	durable = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
	if state.versionState.GetAllocatedStreamingVersion() > durable.GetAllocatedStreamingVersion() {
		durable.AllocatedStreamingVersion = state.versionState.GetAllocatedStreamingVersion()
	}
	state.versionState = durable
	state.persistedAllocated = persistedAllocated
	state.published = canonicalDataViewClone(published)
	m.rememberRecoveredDataView(published)
	publishedStreaming := durable.GetPublishedDataVersion().GetStreamingVersion()
	for streamingVersion := range state.pendingAssigned {
		if streamingVersion <= publishedStreaming {
			delete(state.pendingAssigned, streamingVersion)
			delete(state.readyPublications, streamingVersion)
		}
	}
	return nil
}

func (m *dataViewManager) commitRewriteLocked(
	ctx context.Context,
	state *collectionDataViewState,
	catalog publishedDataViewCatalog,
	mutation PublishedMutation,
) (*viewpb.DataVersion, error) {
	next := publishedMutationBase(state.collectionID, state.published)
	applyPublishedMutation(next, mutation)
	if isDataViewMembershipEqual(state.published, next) {
		return dataVersionFromView(state.published), nil
	}
	current := state.versionState.GetPublishedDataVersion()
	if current == nil || current.GetStreamingVersion() == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no published Streaming epoch to rewrite", state.collectionID)
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
		state.published = canonicalDataViewClone(published)
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

		next := publishedMutationBase(state.collectionID, state.published)
		applyPublishedMutation(next, mutation)
		if isDataViewMembershipEqual(state.published, next) && !isAssignedRemoveOnlyCompletion(mutation) {
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

func verifyDurableAssignedSnapshot(
	ctx context.Context,
	catalog publishedDataViewCatalog,
	collectionID int64,
	assignedVersion *viewpb.DataVersion,
) error {
	views, err := catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return merr.Wrapf(err, "list DataView snapshots for assigned publication %d/0 of collection %d",
			assignedVersion.GetStreamingVersion(), collectionID)
	}
	var target *viewpb.DataViewOfCollection
	for _, view := range views {
		if view.GetCollectionId() != collectionID {
			return merr.WrapErrDataIntegrityMsg(
				"assigned publication collection mismatch: requested=%d, stored=%d",
				collectionID,
				view.GetCollectionId(),
			)
		}
		if compareDataVersion(view.GetDataVersion(), assignedVersion) != 0 {
			continue
		}
		if target != nil && !proto.Equal(canonicalDataViewClone(target), canonicalDataViewClone(view)) {
			return merr.WrapErrDataIntegrityMsg(
				"assigned publication %d/0 for collection %d has conflicting durable snapshots",
				assignedVersion.GetStreamingVersion(),
				collectionID,
			)
		}
		target = view
	}
	if target == nil {
		return merr.WrapErrDataIntegrityMsg(
			"assigned publication %d/0 is missing for collection %d",
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
		return publishedDataViewPersistenceError(state.collectionID, err)
	}
	state.versionState = nextState
	state.persistedAllocated = nextState.GetAllocatedStreamingVersion()
	state.published = canonicalDataViewClone(toPersist)
	m.invalidateRetainedMembership(state.collectionID)
	m.rememberRecoveredDataView(toPersist)
	return nil
}

func publishedDataViewPersistenceError(collectionID int64, err error) error {
	if merr.IsMilvusError(err) {
		return merr.Wrapf(err, "publish DataView for collection %d", collectionID)
	}
	return merr.WrapErrServiceUnavailable(
		fmt.Sprintf("publish DataView for collection %d", collectionID),
		err.Error(),
	)
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
