package querycoordv2

import (
	"bytes"
	"cmp"
	"context"
	"hash/fnv"
	"io"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type queryViewSegmentLoadInfoSubscription struct {
	collectionID int64
	segmentID    int64
	revision     *querypb.QueryViewSegmentLoadInfoRevision
}

type queryViewSegmentLoadInfoWatchSession struct {
	server        *Server
	stream        querypb.QueryCoord_WatchQueryViewSegmentLoadInfoServer
	subscriptions map[int64]queryViewSegmentLoadInfoSubscription
	watcher       *queryViewSegmentLoadInfoWatcher
	notifyCh      chan struct{}
	dirty         map[int64]struct{}
	mu            sync.Mutex
}

func (s *Server) WatchQueryViewSegmentLoadInfo(stream querypb.QueryCoord_WatchQueryViewSegmentLoadInfoServer) error {
	if s.segmentLoadInfoWatcher == nil {
		s.segmentLoadInfoWatcher = newQueryViewSegmentLoadInfoWatcher()
	}
	session := &queryViewSegmentLoadInfoWatchSession{
		server:        s,
		stream:        stream,
		subscriptions: make(map[int64]queryViewSegmentLoadInfoSubscription),
		watcher:       s.segmentLoadInfoWatcher,
		notifyCh:      make(chan struct{}, 1),
		dirty:         make(map[int64]struct{}),
	}
	return session.run()
}

func (s *Server) NotifyQueryViewSegmentLoadInfoChanged(collectionID int64, segmentIDs ...int64) {
	s.NotifySegments(collectionID, segmentIDs...)
}

func (s *Server) NotifySegments(collectionID int64, segmentIDs ...int64) {
	if s.segmentLoadInfoWatcher == nil {
		return
	}
	s.segmentLoadInfoWatcher.notify(collectionID, segmentIDs...)
}

func (s *Server) NotifyQueryViewCollectionLoadInfoChanged(collectionID int64) {
	s.NotifyCollection(collectionID)
}

func (s *Server) NotifyCollection(collectionID int64) {
	if s.segmentLoadInfoWatcher == nil {
		return
	}
	s.segmentLoadInfoWatcher.notifyCollection(collectionID)
}

func (s *queryViewSegmentLoadInfoWatchSession) run() error {
	if s.watcher != nil {
		s.watcher.register(s)
		defer s.watcher.unregister(s)
	}
	defer s.clear()

	reqCh := make(chan *querypb.WatchQueryViewSegmentLoadInfoRequest, 1)
	errCh := make(chan error, 1)
	go s.recvLoop(reqCh, errCh)

	for {
		select {
		case <-s.stream.Context().Done():
			return nil
		case err := <-errCh:
			if err != nil && err != io.EOF {
				mlog.Warn(s.stream.Context(), "query view segment load info watch recv failed", mlog.Err(err))
			}
			return nil
		case req := <-reqCh:
			resp := s.handle(req)
			if !s.sendResponse(resp) {
				return nil
			}
		case <-s.notifyCh:
			resp := s.handleDirtySegments()
			if !s.sendResponse(resp) {
				return nil
			}
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) recvLoop(reqCh chan<- *querypb.WatchQueryViewSegmentLoadInfoRequest, errCh chan<- error) {
	for {
		req, err := s.stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case reqCh <- req:
		case <-s.stream.Context().Done():
			return
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) sendResponse(resp *querypb.WatchQueryViewSegmentLoadInfoResponse) bool {
	if resp == nil || len(resp.GetSnapshots()) == 0 && merr.Ok(resp.GetStatus()) {
		return true
	}
	if err := s.stream.Send(resp); err != nil {
		mlog.Warn(s.stream.Context(), "query view segment load info watch send failed", mlog.Err(err))
		return false
	}
	if !merr.Ok(resp.GetStatus()) {
		return false
	}
	s.updateSnapshotRevisions(resp.GetSnapshots())
	return true
}

func (s *queryViewSegmentLoadInfoWatchSession) handle(req *querypb.WatchQueryViewSegmentLoadInfoRequest) *querypb.WatchQueryViewSegmentLoadInfoResponse {
	resp := &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Status: merr.Success(),
	}
	ctx := s.stream.Context()
	if err := merr.CheckHealthy(s.server.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	if s.server.mixCoord == nil {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("mixcoord is not initialized"))
		return resp
	}

	for _, unsubscribe := range req.GetUnsubscribe() {
		s.unsubscribe(unsubscribe.GetSegmentID())
	}
	subscriptions := make([]queryViewSegmentLoadInfoSubscription, 0, len(req.GetSubscribe()))
	for _, subscribe := range req.GetSubscribe() {
		if subscribe.GetCollectionID() == 0 || subscribe.GetSegmentID() == 0 {
			continue
		}
		subscription := queryViewSegmentLoadInfoSubscription{
			collectionID: subscribe.GetCollectionID(),
			segmentID:    subscribe.GetSegmentID(),
			revision:     subscribe.GetRevision(),
		}
		s.subscribe(subscription)
		subscriptions = append(subscriptions, subscription)
	}

	snapshots, err := s.buildSnapshots(ctx, subscriptions)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	resp.Snapshots = snapshots
	return resp
}

func (s *queryViewSegmentLoadInfoWatchSession) handleDirtySegments() *querypb.WatchQueryViewSegmentLoadInfoResponse {
	resp := &querypb.WatchQueryViewSegmentLoadInfoResponse{
		Status: merr.Success(),
	}
	ctx := s.stream.Context()
	if err := merr.CheckHealthy(s.server.State()); err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	if s.server.mixCoord == nil {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("mixcoord is not initialized"))
		return resp
	}

	snapshots, err := s.buildSnapshots(ctx, s.drainDirtySubscriptions())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp
	}
	resp.Snapshots = snapshots
	return resp
}

func (s *queryViewSegmentLoadInfoWatchSession) buildSnapshots(ctx context.Context, subscriptions []queryViewSegmentLoadInfoSubscription) ([]*querypb.QueryViewSegmentLoadInfoSnapshot, error) {
	byCollection := make(map[int64][]queryViewSegmentLoadInfoSubscription)
	for _, subscription := range subscriptions {
		if subscription.collectionID == 0 || subscription.segmentID == 0 {
			continue
		}
		byCollection[subscription.collectionID] = append(byCollection[subscription.collectionID], subscription)
	}
	snapshots := make([]*querypb.QueryViewSegmentLoadInfoSnapshot, 0, len(subscriptions))
	for collectionID, collectionSubscriptions := range byCollection {
		segmentIDs := make([]int64, 0, len(collectionSubscriptions))
		expected := make(map[int64]*querypb.QueryViewSegmentLoadInfoRevision, len(collectionSubscriptions))
		for _, subscription := range collectionSubscriptions {
			segmentIDs = append(segmentIDs, subscription.segmentID)
			expected[subscription.segmentID] = subscription.revision
		}
		infos, indexInfos, err := s.server.mixCoord.GetQueryViewSegmentLoadInfos(ctx, collectionID, segmentIDs)
		if err != nil {
			return nil, err
		}
		collection, err := s.server.mixCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			),
			CollectionID: collectionID,
		})
		if err := merr.CheckRPCCall(collection, err); err != nil {
			return nil, err
		}
		if collection.GetSchema() == nil {
			return nil, merr.WrapErrServiceInternalMsg("collection schema is missing for query view segment load info, collectionID=%d", collectionID)
		}
		for _, loadInfo := range infos {
			revision := calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexInfos)
			if sameQueryViewSegmentLoadInfoRevision(expected[loadInfo.GetSegmentID()], revision) {
				continue
			}
			snapshots = append(snapshots, &querypb.QueryViewSegmentLoadInfoSnapshot{
				CollectionID:     collectionID,
				SegmentID:        loadInfo.GetSegmentID(),
				Revision:         revision,
				LoadInfo:         loadInfo,
				IndexInfoList:    indexInfos,
				CollectionSchema: collection.GetSchema(),
				SchemaBarrierTs:  collection.GetUpdateTimestamp(),
			})
		}
	}
	return snapshots, nil
}

func (s *queryViewSegmentLoadInfoWatchSession) subscribe(subscription queryViewSegmentLoadInfoSubscription) {
	s.mu.Lock()
	s.subscriptions[subscription.segmentID] = subscription
	delete(s.dirty, subscription.segmentID)
	s.mu.Unlock()
	if s.watcher != nil {
		s.watcher.subscribe(s, subscription.collectionID, subscription.segmentID)
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) unsubscribe(segmentID int64) {
	s.mu.Lock()
	subscription, ok := s.subscriptions[segmentID]
	delete(s.subscriptions, segmentID)
	delete(s.dirty, segmentID)
	s.mu.Unlock()
	if ok && s.watcher != nil {
		s.watcher.unsubscribe(s, subscription.collectionID, segmentID)
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) markDirty(collectionID int64, segmentIDs []int64) {
	s.mu.Lock()
	dirty := false
	for _, segmentID := range segmentIDs {
		subscription, ok := s.subscriptions[segmentID]
		if !ok || subscription.collectionID != collectionID {
			continue
		}
		s.dirty[segmentID] = struct{}{}
		dirty = true
	}
	s.mu.Unlock()
	if dirty {
		select {
		case s.notifyCh <- struct{}{}:
		default:
		}
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) drainDirtySubscriptions() []queryViewSegmentLoadInfoSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscriptions := make([]queryViewSegmentLoadInfoSubscription, 0, len(s.dirty))
	for segmentID := range s.dirty {
		subscription, ok := s.subscriptions[segmentID]
		if ok {
			subscriptions = append(subscriptions, subscription)
		}
		delete(s.dirty, segmentID)
	}
	return subscriptions
}

func (s *queryViewSegmentLoadInfoWatchSession) updateSnapshotRevisions(snapshots []*querypb.QueryViewSegmentLoadInfoSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snapshot := range snapshots {
		subscription, ok := s.subscriptions[snapshot.GetSegmentID()]
		if !ok {
			continue
		}
		subscription.revision = snapshot.GetRevision()
		s.subscriptions[snapshot.GetSegmentID()] = subscription
	}
}

func (s *queryViewSegmentLoadInfoWatchSession) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.subscriptions)
	clear(s.dirty)
}

type queryViewSegmentLoadInfoWatcher struct {
	mu           sync.RWMutex
	sessions     map[*queryViewSegmentLoadInfoWatchSession]struct{}
	bySession    map[*queryViewSegmentLoadInfoWatchSession]map[int64]int64
	bySegment    map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}
	byCollection map[int64]map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}
}

func newQueryViewSegmentLoadInfoWatcher() *queryViewSegmentLoadInfoWatcher {
	return &queryViewSegmentLoadInfoWatcher{
		sessions:     make(map[*queryViewSegmentLoadInfoWatchSession]struct{}),
		bySession:    make(map[*queryViewSegmentLoadInfoWatchSession]map[int64]int64),
		bySegment:    make(map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}),
		byCollection: make(map[int64]map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{}),
	}
}

func (w *queryViewSegmentLoadInfoWatcher) register(session *queryViewSegmentLoadInfoWatchSession) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sessions[session] = struct{}{}
	w.bySession[session] = make(map[int64]int64)
	for _, subscription := range session.subscriptions {
		w.addSubscriptionLocked(session, subscription.collectionID, subscription.segmentID)
	}
}

func (w *queryViewSegmentLoadInfoWatcher) unregister(session *queryViewSegmentLoadInfoWatchSession) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.sessions, session)
	for segmentID, collectionID := range w.bySession[session] {
		w.removeSubscriptionLocked(session, collectionID, segmentID)
	}
	delete(w.bySession, session)
}

func (w *queryViewSegmentLoadInfoWatcher) subscribe(session *queryViewSegmentLoadInfoWatchSession, collectionID, segmentID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.sessions[session]; !ok {
		return
	}
	w.addSubscriptionLocked(session, collectionID, segmentID)
}

func (w *queryViewSegmentLoadInfoWatcher) addSubscriptionLocked(session *queryViewSegmentLoadInfoWatchSession, collectionID, segmentID int64) {
	if previousCollectionID, ok := w.bySession[session][segmentID]; ok {
		w.removeSubscriptionLocked(session, previousCollectionID, segmentID)
	}
	w.bySession[session][segmentID] = collectionID

	sessions, ok := w.bySegment[segmentID]
	if !ok {
		sessions = make(map[*queryViewSegmentLoadInfoWatchSession]struct{})
		w.bySegment[segmentID] = sessions
	}
	sessions[session] = struct{}{}

	segments, ok := w.byCollection[collectionID]
	if !ok {
		segments = make(map[int64]map[*queryViewSegmentLoadInfoWatchSession]struct{})
		w.byCollection[collectionID] = segments
	}
	collectionSessions, ok := segments[segmentID]
	if !ok {
		collectionSessions = make(map[*queryViewSegmentLoadInfoWatchSession]struct{})
		segments[segmentID] = collectionSessions
	}
	collectionSessions[session] = struct{}{}
}

func (w *queryViewSegmentLoadInfoWatcher) unsubscribe(session *queryViewSegmentLoadInfoWatchSession, collectionID, segmentID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.removeSubscriptionLocked(session, collectionID, segmentID)
}

func (w *queryViewSegmentLoadInfoWatcher) removeSubscriptionLocked(session *queryViewSegmentLoadInfoWatchSession, collectionID, segmentID int64) {
	delete(w.bySession[session], segmentID)

	sessions, ok := w.bySegment[segmentID]
	if ok {
		delete(sessions, session)
		if len(sessions) == 0 {
			delete(w.bySegment, segmentID)
		}
	}

	segments, ok := w.byCollection[collectionID]
	if !ok {
		return
	}
	collectionSessions, ok := segments[segmentID]
	if ok {
		delete(collectionSessions, session)
		if len(collectionSessions) == 0 {
			delete(segments, segmentID)
		}
	}
	if len(segments) == 0 {
		delete(w.byCollection, collectionID)
	}
}

func (w *queryViewSegmentLoadInfoWatcher) notify(collectionID int64, segmentIDs ...int64) {
	if collectionID == 0 || len(segmentIDs) == 0 {
		return
	}
	targets := make(map[*queryViewSegmentLoadInfoWatchSession][]int64)
	w.mu.RLock()
	for _, segmentID := range segmentIDs {
		for session := range w.bySegment[segmentID] {
			targets[session] = append(targets[session], segmentID)
		}
	}
	w.mu.RUnlock()
	for session, sessionSegmentIDs := range targets {
		session.markDirty(collectionID, sessionSegmentIDs)
	}
}

func (w *queryViewSegmentLoadInfoWatcher) notifyCollection(collectionID int64) {
	if collectionID == 0 {
		return
	}
	targets := make(map[*queryViewSegmentLoadInfoWatchSession][]int64)
	w.mu.RLock()
	for segmentID, sessions := range w.byCollection[collectionID] {
		for session := range sessions {
			targets[session] = append(targets[session], segmentID)
		}
	}
	w.mu.RUnlock()
	for session, segmentIDs := range targets {
		session.markDirty(collectionID, segmentIDs)
	}
}

func calculateQueryViewSegmentLoadInfoRevision(loadInfo *querypb.SegmentLoadInfo, indexInfos []*indexpb.IndexInfo) *querypb.QueryViewSegmentLoadInfoRevision {
	snapshot := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{
		LoadInfo:      loadInfo,
		IndexInfoList: indexInfos,
	}).(*querypb.QueryViewSegmentLoadInfoSnapshot)
	canonicalizeQueryViewSegmentLoadInfoSnapshot(snapshot)
	return &querypb.QueryViewSegmentLoadInfoRevision{
		LoadInfoRevision: hashProto(snapshot),
	}
}

func canonicalizeQueryViewSegmentLoadInfoSnapshot(snapshot *querypb.QueryViewSegmentLoadInfoSnapshot) {
	for _, index := range snapshot.GetIndexInfoList() {
		sortKeyValuePairs(index.TypeParams)
		sortKeyValuePairs(index.IndexParams)
		sortKeyValuePairs(index.UserIndexParams)
	}
	slices.SortFunc(snapshot.IndexInfoList, compareIndexInfo)

	loadInfo := snapshot.GetLoadInfo()
	if loadInfo == nil {
		return
	}
	canonicalizeFieldBinlogs(loadInfo.BinlogPaths)
	canonicalizeFieldBinlogs(loadInfo.Statslogs)
	canonicalizeFieldBinlogs(loadInfo.Deltalogs)
	canonicalizeFieldBinlogs(loadInfo.Bm25Logs)
	for _, index := range loadInfo.GetIndexInfos() {
		sortKeyValuePairs(index.IndexParams)
		slices.Sort(index.IndexFilePaths)
	}
	slices.SortFunc(loadInfo.IndexInfos, compareFieldIndexInfo)
	for _, stats := range loadInfo.GetTextStatsLogs() {
		slices.Sort(stats.Files)
	}
	for _, stats := range loadInfo.GetJsonKeyStatsLogs() {
		slices.Sort(stats.Files)
	}
	slices.Sort(loadInfo.CompactionFrom)
	slices.Sort(loadInfo.ChildManifestPaths)
}

func canonicalizeFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) {
	for _, fieldBinlog := range fieldBinlogs {
		slices.Sort(fieldBinlog.ChildFields)
		slices.SortFunc(fieldBinlog.Binlogs, compareBinlog)
	}
	slices.SortFunc(fieldBinlogs, compareFieldBinlog)
}

func compareFieldBinlog(left, right *datapb.FieldBinlog) int {
	if result := cmp.Compare(left.GetFieldID(), right.GetFieldID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetFormat(), right.GetFormat()); result != 0 {
		return result
	}
	if result := slices.Compare(left.GetChildFields(), right.GetChildFields()); result != 0 {
		return result
	}
	if result := slices.CompareFunc(left.GetBinlogs(), right.GetBinlogs(), compareBinlog); result != 0 {
		return result
	}
	return compareProto(left, right)
}

func compareBinlog(left, right *datapb.Binlog) int {
	if result := cmp.Compare(left.GetLogID(), right.GetLogID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetLogPath(), right.GetLogPath()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetTimestampFrom(), right.GetTimestampFrom()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetTimestampTo(), right.GetTimestampTo()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetEntriesNum(), right.GetEntriesNum()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetLogSize(), right.GetLogSize()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetMemorySize(), right.GetMemorySize()); result != 0 {
		return result
	}
	return compareProto(left, right)
}

func compareProto(left, right proto.Message) int {
	options := proto.MarshalOptions{Deterministic: true}
	leftBytes, _ := options.Marshal(left)
	rightBytes, _ := options.Marshal(right)
	return bytes.Compare(leftBytes, rightBytes)
}

func sortKeyValuePairs(pairs []*commonpb.KeyValuePair) {
	slices.SortFunc(pairs, func(left, right *commonpb.KeyValuePair) int {
		if result := cmp.Compare(left.GetKey(), right.GetKey()); result != 0 {
			return result
		}
		if result := cmp.Compare(left.GetValue(), right.GetValue()); result != 0 {
			return result
		}
		return compareProto(left, right)
	})
}

func compareIndexInfo(left, right *indexpb.IndexInfo) int {
	if result := cmp.Compare(left.GetCollectionID(), right.GetCollectionID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetFieldID(), right.GetFieldID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexID(), right.GetIndexID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexName(), right.GetIndexName()); result != 0 {
		return result
	}
	return compareProto(left, right)
}

func compareFieldIndexInfo(left, right *querypb.FieldIndexInfo) int {
	if result := cmp.Compare(left.GetFieldID(), right.GetFieldID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexID(), right.GetIndexID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetBuildID(), right.GetBuildID()); result != 0 {
		return result
	}
	if result := cmp.Compare(left.GetIndexName(), right.GetIndexName()); result != 0 {
		return result
	}
	return compareProto(left, right)
}

func hashProto(message proto.Message) uint64 {
	if message == nil {
		return 0
	}
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return 0
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write(bytes)
	return hasher.Sum64()
}

func sameQueryViewSegmentLoadInfoRevision(left, right *querypb.QueryViewSegmentLoadInfoRevision) bool {
	if left == nil || right == nil {
		return false
	}
	return left.GetLoadInfoRevision() == right.GetLoadInfoRevision()
}
