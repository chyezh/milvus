package idf

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	testBM25InputFieldID  = int64(101)
	testBM25OutputFieldID = int64(102)
)

type testBytesFileReader struct {
	*bytes.Reader
}

func (r *testBytesFileReader) Close() error         { return nil }
func (r *testBytesFileReader) Size() (int64, error) { return int64(r.Len()), nil }

type blockingNodeSchedulerTask struct {
	started chan struct{}
	release chan struct{}
}

type observedDoneContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneObserved) })
	return c.Context.Done()
}

func (t *blockingNodeSchedulerTask) Execute(ctx context.Context) error {
	close(t.started)
	select {
	case <-t.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newScheduledOracleRuntime(scheduler nodescheduler.Scheduler, current qviews.DataVersion) *oracleRuntime {
	return &oracleRuntime{
		provider:       &Provider{},
		scheduler:      scheduler,
		collectionID:   1,
		vchannel:       "v1",
		currentVersion: current,
		currentStats:   make(bm25Stats),
		currentSealed:  make(map[int64]*sealedBm25Stats),
		currentGrowing: make(map[int64]struct{}),
		growingStore:   newGrowingStatsStore(nil),
	}
}

func TestOracleRuntimeSchedulesCoalescedAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	first := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	latest := qviews.DataVersion{StreamingVersion: 12, CompactVersion: 1}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	blocker := &blockingNodeSchedulerTask{started: make(chan struct{}), release: make(chan struct{})}
	scheduler.Submit(blocker)
	<-blocker.started

	var callsMu sync.Mutex
	calls := make([]qviews.DataVersion, 0, 1)
	mock := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
		_ *Provider,
		_ context.Context,
		_ int64,
		_ string,
		version qviews.DataVersion,
		_ []int64,
		_ uint64,
	) ([]*datapb.StreamingNodeBM25Resource, error) {
		callsMu.Lock()
		calls = append(calls, version)
		callsMu.Unlock()
		return nil, nil
	}).Build()
	defer mock.UnPatch()

	runtime := newScheduledOracleRuntime(scheduler, current)
	runtime.MaybeAdvance(first)
	runtime.MaybeAdvance(latest)

	runtime.mu.RLock()
	require.True(t, runtime.advanceScheduled)
	require.True(t, runtime.pending.EQ(latest))
	runtime.mu.RUnlock()

	close(blocker.release)
	require.Eventually(t, func() bool {
		runtime.mu.RLock()
		defer runtime.mu.RUnlock()
		return runtime.currentVersion.EQ(latest) && !runtime.advanceScheduled
	}, time.Second, 10*time.Millisecond)
	runtime.Close()

	callsMu.Lock()
	require.Equal(t, []qviews.DataVersion{latest}, calls)
	callsMu.Unlock()
}

func TestOracleRuntimeRetriesFailedAdvance(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{name: "typed unavailable", err: merr.WrapErrServiceUnavailableMsg("temporary resource lookup failure")},
		{name: "grpc unavailable", err: status.Error(codes.Unavailable, "temporary resource lookup failure")},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(failure.name, func(t *testing.T) {
			current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
			target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()

			var calls atomic.Int32
			mock := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
				_ *Provider,
				_ context.Context,
				_ int64,
				_ string,
				_ qviews.DataVersion,
				_ []int64,
				_ uint64,
			) ([]*datapb.StreamingNodeBM25Resource, error) {
				if calls.Add(1) == 1 {
					return nil, failure.err
				}
				return nil, nil
			}).Build()
			defer mock.UnPatch()

			runtime := newScheduledOracleRuntime(scheduler, current)
			runtime.MaybeAdvance(target)
			require.Eventually(t, func() bool {
				runtime.mu.RLock()
				defer runtime.mu.RUnlock()
				return runtime.currentVersion.EQ(target) && !runtime.advanceScheduled
			}, time.Second, time.Millisecond)
			runtime.Close()
			require.Equal(t, int32(2), calls.Load())
		})
	}
}

func TestOracleRuntimeStopsAdvanceAfterCorruptPreparedStats(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	localDir := filepath.Join(t.TempDir(), "prepared")
	fieldDir := filepath.Join(localDir, fmt.Sprint(testBM25OutputFieldID))
	require.NoError(t, os.MkdirAll(fieldDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(fieldDir, "0.data"), []byte{1, 2, 3}, 0o600))
	sealed := &sealedBm25Stats{
		key:       sealedCacheKey("prepared"),
		segmentID: 7,
		localDir:  localDir,
		fieldList: []int64{testBM25OutputFieldID},
		refs:      1,
	}
	cache := newSegmentCacheAt(t.TempDir())
	cache.entries[sealed.key] = sealed

	var resourceLookups atomic.Int32
	lookup := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
		_ *Provider,
		_ context.Context,
		_ int64,
		_ string,
		_ qviews.DataVersion,
		_ []int64,
		_ uint64,
	) ([]*datapb.StreamingNodeBM25Resource, error) {
		resourceLookups.Add(1)
		return nil, nil
	}).Build()
	defer lookup.UnPatch()

	runtime := newScheduledOracleRuntime(scheduler, current)
	defer runtime.Close()
	runtime.provider.sealedCache = cache
	runtime.prepared = map[qviews.DataVersion]map[int64]*sealedBm25Stats{
		target: {sealed.segmentID: sealed},
	}
	runtime.MaybeAdvance(target)

	runtime.mu.RLock()
	handle := runtime.advanceHandle
	runtime.mu.RUnlock()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, handle.Wait(waitCtx))
	runtime.mu.RLock()
	versionUnchanged := runtime.currentVersion.EQ(current)
	hasPending := runtime.hasPending
	advanceScheduled := runtime.advanceScheduled
	runtime.mu.RUnlock()
	require.True(t, versionUnchanged)
	require.False(t, hasPending)
	require.False(t, advanceScheduled)
	require.Equal(t, int32(0), resourceLookups.Load())
	require.NoDirExists(t, localDir)
}

func TestAcquireSealedContributionsUsesSharedLimit(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 1})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)

	chunkManager := mocks.NewChunkManager(t)
	var active atomic.Int32
	var maxActive atomic.Int32
	var started atomic.Int32
	firstBatchStarted := make(chan struct{})
	release := make(chan struct{})
	chunkManager.EXPECT().Reader(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			if started.Add(1) == 2 {
				close(firstBatchStarted)
			}
			select {
			case <-release:
				return &testBytesFileReader{bytes.NewReader(statsBytes)}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Times(4)

	limiter := syncutil.NewSemaphore(2)
	providers := []*Provider{
		{chunkManager: chunkManager, sealedCache: newSegmentCacheAt(t.TempDir()), sealedStatsLoadLimiter: limiter},
		{chunkManager: chunkManager, sealedCache: newSegmentCacheAt(t.TempDir()), sealedStatsLoadLimiter: limiter},
	}
	type result struct {
		contributions map[int64]*sealedBm25Stats
		cache         *segmentCache
		err           error
	}
	results := make(chan result, len(providers))
	for i, provider := range providers {
		resources := testSealedBM25Resources(int64(i*2+1), 2)
		go func(provider *Provider, resources []*datapb.StreamingNodeBM25Resource) {
			contributions, err := provider.acquireSealedContributions(context.Background(), resources, make(bm25Stats))
			results <- result{contributions: contributions, cache: provider.sealedCache, err: err}
		}(provider, resources)
	}

	select {
	case <-firstBatchStarted:
	case <-time.After(time.Second):
		t.Fatal("sealed BM25 stats loads did not start")
	}
	close(release)
	for range providers {
		result := <-results
		require.NoError(t, result.err)
		require.Len(t, result.contributions, 2)
		for _, contribution := range result.contributions {
			result.cache.release(contribution)
		}
	}
	require.Equal(t, int32(2), maxActive.Load())
}

func TestAcquireSealedContributionsMergesCompletedLoadsImmediately(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 1})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)

	secondReadStarted := make(chan struct{})
	releaseSecondRead := make(chan struct{})
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, path string) (storage.FileReader, error) {
			if path == "stats-2" {
				close(secondReadStarted)
				<-releaseSecondRead
			}
			return &testBytesFileReader{bytes.NewReader(statsBytes)}, nil
		}).Times(2)

	provider := &Provider{
		chunkManager:           chunkManager,
		sealedCache:            newSegmentCacheAt(t.TempDir()),
		sealedStatsLoadLimiter: syncutil.NewSemaphore(2),
	}
	merged := make(chan struct{}, 2)
	var mergeOrigin func(*storage.BM25Stats, *storage.BM25Stats)
	mergeMock := mockey.Mock((*storage.BM25Stats).Merge).To(func(target, source *storage.BM25Stats) {
		mergeOrigin(target, source)
		merged <- struct{}{}
	}).Origin(&mergeOrigin).Build()
	defer mergeMock.UnPatch()

	aggregate := newBM25StatsFromSchema(testBM25WALView(qviews.DataVersion{}).Schema)
	type result struct {
		contributions map[int64]*sealedBm25Stats
		err           error
	}
	resultCh := make(chan result, 1)
	go func() {
		contributions, err := provider.acquireSealedContributions(
			context.Background(),
			testSealedBM25Resources(1, 2),
			aggregate,
		)
		resultCh <- result{contributions: contributions, err: err}
	}()

	<-secondReadStarted
	select {
	case <-merged:
	case <-time.After(time.Second):
		t.Fatal("completed sealed stats were not merged while another load was pending")
	}
	close(releaseSecondRead)
	acquired := <-resultCh
	require.NoError(t, acquired.err)
	require.Len(t, acquired.contributions, 2)
	require.Equal(t, int64(2), aggregate[testBM25OutputFieldID].NumRow())
	for _, contribution := range acquired.contributions {
		provider.sealedCache.release(contribution)
	}
}

func TestAcquireSealedContributionsCancellationWhileWaitingForLimit(t *testing.T) {
	readStarted := make(chan struct{})
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
			close(readStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()

	provider := &Provider{
		chunkManager:           chunkManager,
		sealedCache:            newSegmentCacheAt(t.TempDir()),
		sealedStatsLoadLimiter: syncutil.NewSemaphore(1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := provider.acquireSealedContributions(ctx, testSealedBM25Resources(1, 2), make(bm25Stats))
		resultCh <- err
	}()

	<-readStarted
	cancel()
	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("sealed stats acquisition did not stop after cancellation")
	}
	require.Empty(t, provider.sealedCache.entries)
}

func TestAcquireSealedContributionsReleasesReferencesAfterError(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 1})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)

	goodRead := make(chan struct{})
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, path string) (storage.FileReader, error) {
			if path == "stats-1" {
				close(goodRead)
				return &testBytesFileReader{bytes.NewReader(statsBytes)}, nil
			}
			<-goodRead
			return nil, merr.WrapErrDataIntegrityMsg("test sealed stats read failure")
		}).Times(2)

	provider := &Provider{
		chunkManager:           chunkManager,
		sealedCache:            newSegmentCacheAt(t.TempDir()),
		sealedStatsLoadLimiter: syncutil.NewSemaphore(2),
	}
	_, err = provider.acquireSealedContributions(context.Background(), testSealedBM25Resources(1, 2), make(bm25Stats))
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Empty(t, provider.sealedCache.entries)
}

func TestAcquireSealedContributionsRejectsDuplicateSegment(t *testing.T) {
	provider := &Provider{
		chunkManager:           storage.NewLocalChunkManager(),
		sealedCache:            newSegmentCacheAt(t.TempDir()),
		sealedStatsLoadLimiter: syncutil.NewSemaphore(2),
	}
	resources := testSealedBM25Resources(1, 2)
	resources[1].SegmentId = resources[0].SegmentId

	_, err := provider.acquireSealedContributions(context.Background(), resources, nil)
	require.Error(t, err)
	require.Empty(t, provider.sealedCache.entries)
}

func TestAcquireSealedContributionsCanceledBeforeLoad(t *testing.T) {
	for _, held := range []int{0, 1} {
		t.Run(fmt.Sprint(held), func(t *testing.T) {
			chunkManager := mocks.NewChunkManager(t)
			limiter := syncutil.NewSemaphore(1)
			for range held {
				require.NoError(t, limiter.Acquire(context.Background()))
				defer limiter.Release()
			}
			provider := &Provider{
				chunkManager:           chunkManager,
				sealedCache:            newSegmentCacheAt(t.TempDir()),
				sealedStatsLoadLimiter: limiter,
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(merr.WrapErrServiceUnavailableMsg("test cancellation cause"))

			_, err := provider.acquireSealedContributions(ctx, testSealedBM25Resources(1, 1), nil)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, held, limiter.Current())
			require.Empty(t, provider.sealedCache.entries)
			chunkManager.AssertNotCalled(t, "Reader", mock.Anything, mock.Anything)
		})
	}
}

func TestOracleRuntimeCloseCancelsScheduledAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	started := make(chan struct{})
	canceled := make(chan struct{})
	mock := mockey.Mock((*Provider).getSealedBM25Resources).To(func(
		_ *Provider,
		ctx context.Context,
		_ int64,
		_ string,
		_ qviews.DataVersion,
		_ []int64,
		_ uint64,
	) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}).Build()
	defer mock.UnPatch()

	runtime := newScheduledOracleRuntime(scheduler, current)
	runtime.MaybeAdvance(target)
	<-started
	runtime.Close()

	select {
	case <-canceled:
	default:
		t.Fatal("scheduled IDF advance was not canceled")
	}
}

func TestOracleRuntimeCommitDiffUsesLatestGrowingState(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	target := qviews.DataVersion{StreamingVersion: 11, CompactVersion: 1}
	schema := testBM25WALView(current).Schema
	growingStore := newGrowingStatsStore(schema)
	initial := storage.NewBM25Stats()
	initial.Append(map[uint32]float32{7: 1})
	growingStore.appendStats(20, 10, bm25Stats{testBM25OutputFieldID: initial})
	global := newBM25StatsFromSchema(schema)
	global.merge(bm25Stats{testBM25OutputFieldID: initial})
	runtime := &oracleRuntime{
		currentVersion: current,
		currentStats:   global,
		currentSealed:  make(map[int64]*sealedBm25Stats),
		currentGrowing: map[int64]struct{}{20: {}},
		growingStore:   growingStore,
	}

	additional := storage.NewBM25Stats()
	additional.Append(map[uint32]float32{8: 2})
	runtime.mu.Lock()
	runtime.growingStore.appendStats(20, 10, bm25Stats{testBM25OutputFieldID: additional})
	runtime.currentStats.merge(bm25Stats{testBM25OutputFieldID: additional})
	runtime.growingStore.markSealed(20, target)
	runtime.mu.Unlock()

	runtime.commitDiff(context.Background(), &idfDiff{
		target:     target,
		positive:   make(bm25Stats),
		negative:   make(bm25Stats),
		nextSealed: make(map[int64]*sealedBm25Stats),
	})

	require.True(t, runtime.currentVersion.EQ(target))
	require.Empty(t, runtime.currentGrowing)
	require.Zero(t, runtime.currentStats[testBM25OutputFieldID].NumRow())
	require.Zero(t, runtime.currentStats[testBM25OutputFieldID].NumToken())
}

func TestOracleRuntimeSharesSingleGlobalAcrossDataVersions(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	fieldID := int64(102)
	schema := &schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		OutputFieldIds: []int64{fieldID},
	}}}
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	global := newBM25StatsFromSchema(schema)
	global.merge(bm25Stats{fieldID: stats})

	mock := mockey.Mock((*Provider).getSealedBM25Resources).Return(nil, nil).Build()
	defer mock.UnPatch()
	runtime := &oracleRuntime{
		provider:       &Provider{sealedCache: newSegmentCacheAt(t.TempDir())},
		collectionID:   1,
		vchannel:       "v1",
		schema:         schema,
		currentVersion: current,
		currentStats:   global,
		currentSealed:  make(map[int64]*sealedBm25Stats),
		currentGrowing: make(map[int64]struct{}),
		prepared:       make(map[qviews.DataVersion]map[int64]*sealedBm25Stats),
		growingStore:   newGrowingStatsStore(schema),
	}

	require.NoError(t, runtime.PrepareDataVersion(context.Background(), target))
	require.True(t, oraclePreparedReady(runtime, target))
	require.Same(t, global[fieldID], runtime.currentStats[fieldID])
	query := &schemapb.SparseFloatArray{Contents: [][]byte{
		typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1}),
	}}
	_, currentAvgdl, err := runtime.BuildIDF(context.Background(), current, fieldID, query)
	require.NoError(t, err)
	_, targetAvgdl, err := runtime.BuildIDF(context.Background(), target, fieldID, query)
	require.NoError(t, err)
	require.Equal(t, float64(2), currentAvgdl)
	require.Equal(t, currentAvgdl, targetAvgdl)

	runtime.ReleaseDataVersion(target)
	_, targetAvgdl, err = runtime.BuildIDF(context.Background(), target, fieldID, query)
	require.NoError(t, err)
	require.Equal(t, currentAvgdl, targetAvgdl)
}

func TestOracleRuntimeReusesSealedFileDuringPreparation(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)
	resource := testSealedBM25Resources(3, 1)[0]

	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, resource.GetBm25Binlogs()[0].GetBinlogs()[0].GetLogPath()).
		Return(&testBytesFileReader{Reader: bytes.NewReader(statsBytes)}, nil).
		Once()
	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			response := testBM25ResourceResponse(req)
			response.Bm25Resources = []*datapb.StreamingNodeBM25Resource{resource}
			return response, nil
		}).Once()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(client, WithChunkManager(chunkManager), WithNodeScheduler(scheduler))
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	oracle, err := newOracleRuntime(
		context.Background(),
		provider,
		testBM25WALView(current),
		[]*datapb.StreamingNodeBM25Resource{resource},
		false,
	)
	require.NoError(t, err)

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	oracle.Advance(target)
	require.Eventually(t, func() bool {
		return oracleCurrentVersion(oracle).EQ(target)
	}, time.Second, time.Millisecond)

	oracle.Close()
	require.Empty(t, provider.sealedCache.entries)
}

func TestOracleRuntimeUpdatesSealedStatsWhenResourceChangesForSameSegment(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	chunkManager := storage.NewLocalChunkManager()
	firstPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{7: 1})
	secondPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{7: 4})
	firstResource := testSealedBM25Resources(3, 1)[0]
	firstResource.Bm25Binlogs[0].Binlogs[0].LogPath = firstPath
	secondResource := testSealedBM25Resources(3, 1)[0]
	secondResource.Bm25Binlogs[0].Binlogs[0].LogPath = secondPath

	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(target))
			response := testBM25ResourceResponse(req)
			response.Bm25Resources = []*datapb.StreamingNodeBM25Resource{secondResource}
			return response, nil
		}).Once()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(client, WithChunkManager(chunkManager), WithNodeScheduler(scheduler))
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	oracle, err := newOracleRuntime(context.Background(), provider, testBM25WALView(current), []*datapb.StreamingNodeBM25Resource{firstResource}, false)
	require.NoError(t, err)
	defer oracle.Close()

	_, avgdl, err := oracle.BuildIDF(context.Background(), current, testBM25OutputFieldID, nil)
	require.NoError(t, err)
	require.Equal(t, float64(1), avgdl)
	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))

	oracle.Advance(target)
	require.Eventually(t, func() bool {
		oracle.mu.RLock()
		defer oracle.mu.RUnlock()
		return oracle.currentVersion.EQ(target) && oracle.currentStats[testBM25OutputFieldID].GetAvgdl() == 4
	}, time.Second, time.Millisecond)
	_, avgdl, err = oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, nil)
	require.NoError(t, err)
	require.Equal(t, float64(4), avgdl)
}

func TestOracleRuntimeHandsGrowingContributionToSealedFromDisk(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	chunkManager := storage.NewLocalChunkManager()
	sealedStats := storage.NewBM25Stats()
	sealedStats.Append(map[uint32]float32{7: 2}, map[uint32]float32{8: 4})
	sealedBytes, err := sealedStats.Serialize()
	require.NoError(t, err)
	sealedPath := t.TempDir() + "/sealed-stats"
	require.NoError(t, chunkManager.Write(context.Background(), sealedPath, sealedBytes))
	sealedResource := &datapb.StreamingNodeBM25Resource{
		SegmentId:      20,
		PartitionId:    10,
		StorageVersion: storage.StorageV2,
		Bm25Binlogs: []*datapb.FieldBinlog{{
			FieldID: testBM25OutputFieldID,
			Binlogs: []*datapb.Binlog{{LogPath: sealedPath}},
		}},
	}

	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			response := testBM25ResourceResponse(req)
			response.Bm25Resources = []*datapb.StreamingNodeBM25Resource{sealedResource}
			return response, nil
		}).Once()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(client, WithChunkManager(chunkManager), WithNodeScheduler(scheduler))
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	schema := testBM25WALView(current).Schema
	growingStore := newGrowingStatsStore(schema)
	growingStats := storage.NewBM25Stats()
	growingStats.Append(map[uint32]float32{7: 2})
	growingStore.appendStats(20, 10, bm25Stats{testBM25OutputFieldID: growingStats})
	growingStore.markSealed(20, target)
	global := newBM25StatsFromSchema(schema)
	global.merge(bm25Stats{testBM25OutputFieldID: growingStats})
	oracle := &oracleRuntime{
		provider:        provider,
		scheduler:       scheduler,
		collectionID:    1,
		vchannel:        "test-vchannel",
		schema:          schema,
		currentVersion:  current,
		currentStats:    global,
		currentSealed:   make(map[int64]*sealedBm25Stats),
		currentGrowing:  map[int64]struct{}{20: {}},
		prepared:        make(map[qviews.DataVersion]map[int64]*sealedBm25Stats),
		growingStore:    growingStore,
		loadInfoVersion: 0,
	}
	defer oracle.Close()

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	oracle.Advance(target)
	require.Eventually(t, func() bool {
		if !oracleCurrentVersion(oracle).EQ(target) {
			return false
		}
		growingStore.mu.RLock()
		defer growingStore.mu.RUnlock()
		_, growingExists := growingStore.segments[20]
		return !growingExists
	}, time.Second, time.Millisecond)
	oracle.mu.RLock()
	currentStats := oracle.currentStats[testBM25OutputFieldID]
	require.Equal(t, int64(2), currentStats.NumRow())
	require.Equal(t, int64(6), currentStats.NumToken())
	require.Empty(t, oracle.currentGrowing)
	oracle.mu.RUnlock()
}

func TestOracleRuntimeLazyPreparationUsesSharedCurrentGlobal(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			version := qviews.FromProtoDataVersion(req.GetDataVersion())
			require.True(t, version.EQ(current))
			return testBM25ResourceResponse(req), nil
		}).Once()
	oracle := newTestLazyOracleRuntime(t, client, current)
	defer oracle.Close()

	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	oracle.growingStore.appendStats(20, 10, bm25Stats{testBM25OutputFieldID: stats})

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	require.Empty(t, client.Calls)
	require.False(t, oraclePreparedReady(oracle, target))
	_, avgdl, err := oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Equal(t, float64(2), avgdl)
	require.Len(t, client.Calls, 1)
	require.True(t, oracleStatsReady(oracle))

	oracle.ReleaseDataVersion(target)
	_, _, err = oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Len(t, client.Calls, 1)
}

func TestOracleRuntimeEagerPreparationCancellationIsIsolated(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	client := mocks.NewMockDataCoordClient(t)
	firstStarted := make(chan struct{})
	var calls atomic.Int32
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return testBM25ResourceResponse(req), nil
		}).Twice()
	oracle := newTestEagerOracleRuntime(t, client, current)
	defer oracle.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- oracle.PrepareDataVersion(firstCtx, target)
	}()
	<-firstStarted

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- oracle.PrepareDataVersion(context.Background(), target)
	}()
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second eager preparation waited for the canceled caller")
	}
	require.True(t, oraclePreparedReady(oracle, target))

	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	require.Len(t, client.Calls, 2)
}

func TestOracleRuntimePreparationPrefetchesWithoutParsing(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	resource := testSealedBM25Resources(30, 1)[0]
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, resource.GetBm25Binlogs()[0].GetBinlogs()[0].GetLogPath()).
		Return(&testBytesFileReader{Reader: bytes.NewReader([]byte{1, 2, 3})}, nil).Once()
	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			response := testBM25ResourceResponse(req)
			response.Bm25Resources = []*datapb.StreamingNodeBM25Resource{resource}
			return response, nil
		}).Once()
	oracle := newTestEagerOracleRuntime(t, client, current, WithChunkManager(chunkManager))
	defer oracle.Close()

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	require.True(t, oraclePreparedReady(oracle, target))
	require.Len(t, oracle.provider.sealedCache.entries, 1)
}

func TestOracleRuntimeSharesCanceledVersionMaterialization(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	client := mocks.NewMockDataCoordClient(t)
	rpcStarted := make(chan struct{})
	var calls atomic.Int32
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			if calls.Add(1) == 1 {
				close(rpcStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return testBM25ResourceResponse(req), nil
		}).Twice()
	oracle := newTestLazyOracleRuntime(t, client, version)
	defer oracle.Close()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(firstCtx, version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		firstResult <- err
	}()
	<-rpcStarted

	waiterCtx := &observedDoneContext{Context: context.Background(), doneObserved: make(chan struct{})}
	secondResult := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(waiterCtx, version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		secondResult <- err
	}()
	<-waiterCtx.doneObserved

	cancelFirst()
	require.ErrorIs(t, <-firstResult, context.Canceled)
	require.ErrorIs(t, <-secondResult, context.Canceled)
	require.False(t, oracleStatsReady(oracle))
	require.Len(t, client.Calls, 1)

	_, _, err := oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Len(t, client.Calls, 2)
	require.True(t, oracleStatsReady(oracle))
}

func TestOracleRuntimeRetriesCurrentMaterializationAfterLazyAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 12}
	client := mocks.NewMockDataCoordClient(t)
	rpcStarted := make(chan struct{})
	releaseFirstRPC := make(chan struct{})
	var calls atomic.Int32
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			if calls.Add(1) == 1 {
				require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(current))
				close(rpcStarted)
				<-releaseFirstRPC
				return nil, ctx.Err()
			}
			require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(target))
			return testBM25ResourceResponse(req), nil
		}).Twice()
	oracle := newTestLazyOracleRuntime(t, client, current)
	defer oracle.Close()

	firstResult := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		firstResult <- err
	}()
	<-rpcStarted
	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	oracle.Advance(target)

	waiterCtx := &observedDoneContext{Context: context.Background(), doneObserved: make(chan struct{})}
	secondResult := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(waiterCtx, target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		secondResult <- err
	}()
	<-waiterCtx.doneObserved
	close(releaseFirstRPC)

	require.NoError(t, <-firstResult)
	require.NoError(t, <-secondResult)
	require.Equal(t, int32(2), calls.Load())
	require.True(t, oracleCurrentVersion(oracle).EQ(target))
	require.True(t, oracleStatsReady(oracle))
}

func TestOracleRuntimeCancellationBeforeCommitDoesNotPublish(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	client := mocks.NewMockDataCoordClient(t)
	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	rpcReturned := make(chan struct{})
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			close(rpcStarted)
			<-releaseRPC
			close(rpcReturned)
			return testBM25ResourceResponse(req), nil
		}).Once()
	oracle := newTestLazyOracleRuntime(t, client, version)
	defer oracle.Close()

	oracle.growingStore.mu.Lock()
	storeLocked := true
	defer func() {
		if storeLocked {
			oracle.growingStore.mu.Unlock()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(ctx, version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		result <- err
	}()
	<-rpcStarted
	close(releaseRPC)
	<-rpcReturned

	require.Eventually(t, func() bool {
		if oracle.mu.TryLock() {
			oracle.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	cancel()
	oracle.growingStore.mu.Unlock()
	storeLocked = false

	require.ErrorIs(t, <-result, context.Canceled)
	require.False(t, oracleStatsReady(oracle))
}

func TestOracleRuntimeAllowsNextQueryAfterMaterializationFailure(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	client := mocks.NewMockDataCoordClient(t)
	var calls atomic.Int32
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			if calls.Add(1) == 1 {
				return nil, merr.WrapErrServiceUnavailableMsg("temporary resource lookup failure")
			}
			return testBM25ResourceResponse(req), nil
		}).Twice()
	oracle := newTestLazyOracleRuntime(t, client, version)
	defer oracle.Close()

	_, _, err := oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.Error(t, err)
	require.False(t, oracleStatsReady(oracle))
	require.Len(t, client.Calls, 1)

	_, _, err = oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Len(t, client.Calls, 2)
	require.True(t, oracleStatsReady(oracle))
}

func TestOracleRuntimeIncludesGrowingStatsAddedDuringMaterialization(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	client := mocks.NewMockDataCoordClient(t)
	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			close(rpcStarted)
			select {
			case <-releaseRPC:
				return testBM25ResourceResponse(req), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).Once()
	oracle := newTestLazyOracleRuntime(t, client, version)
	defer oracle.Close()

	type result struct {
		avgdl float64
		err   error
	}
	buildResult := make(chan result, 1)
	go func() {
		_, avgdl, err := oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		buildResult <- result{avgdl: avgdl, err: err}
	}()
	<-rpcStarted

	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	oracle.mu.Lock()
	oracle.growingStore.appendStats(20, 10, bm25Stats{testBM25OutputFieldID: stats})
	oracle.mu.Unlock()
	close(releaseRPC)

	resultValue := <-buildResult
	require.NoError(t, resultValue.err)
	require.Equal(t, float64(2), resultValue.avgdl)
}

func TestOracleRuntimeLazyTargetFilesLoadOnFirstQueryAfterAdvance(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 12}
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{7: 2})
	serialized, err := stats.Serialize()
	require.NoError(t, err)
	resource := testSealedBM25Resources(30, 1)[0]

	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, resource.GetBm25Binlogs()[0].GetBinlogs()[0].GetLogPath()).
		Return(&testBytesFileReader{Reader: bytes.NewReader(serialized)}, nil).Once()
	client := mocks.NewMockDataCoordClient(t)
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(target))
			response := testBM25ResourceResponse(req)
			response.Bm25Resources = []*datapb.StreamingNodeBM25Resource{resource}
			return response, nil
		}).Once()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	provider := NewProvider(client, WithChunkManager(chunkManager), WithNodeScheduler(scheduler))
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	oracle, err := newOracleRuntime(context.Background(), provider, testBM25WALView(current), nil, true)
	require.NoError(t, err)
	defer oracle.Close()

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	require.False(t, oraclePreparedReady(oracle, target))
	require.False(t, oracleStatsReady(oracle))
	provider.sealedCache.mu.Lock()
	entryCount := len(provider.sealedCache.entries)
	provider.sealedCache.mu.Unlock()
	require.Zero(t, entryCount)
	require.Empty(t, client.Calls)

	oracle.Advance(target)
	require.True(t, oracleCurrentVersion(oracle).EQ(target))
	require.Empty(t, client.Calls)
	_, avgdl, err := oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Equal(t, float64(2), avgdl)
	require.Len(t, client.Calls, 1)
}

func TestOracleRuntimeLazyOnlyDefersInitialMaterialization(t *testing.T) {
	current := qviews.DataVersion{StreamingVersion: 10}
	target := qviews.DataVersion{StreamingVersion: 11}
	client := mocks.NewMockDataCoordClient(t)
	var calls atomic.Int32
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			if calls.Add(1) == 1 {
				require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(current))
			} else {
				require.True(t, qviews.FromProtoDataVersion(req.GetDataVersion()).EQ(target))
			}
			return testBM25ResourceResponse(req), nil
		}).Twice()
	oracle := newTestLazyOracleRuntime(t, client, current)
	defer oracle.Close()

	_, _, err := oracle.BuildIDF(context.Background(), current, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.True(t, oracleStatsReady(oracle))

	require.NoError(t, oracle.PrepareDataVersion(context.Background(), target))
	require.True(t, oraclePreparedReady(oracle, target))
	require.Len(t, client.Calls, 2)

	oracle.Advance(target)
	require.Eventually(t, func() bool {
		return oracleCurrentVersion(oracle).EQ(target)
	}, time.Second, time.Millisecond)
	require.True(t, oracleStatsReady(oracle))
	_, _, err = oracle.BuildIDF(context.Background(), target, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.NoError(t, err)
	require.Len(t, client.Calls, 2)
}

func TestOracleRuntimeCloseCancelsVersionMaterialization(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 10}
	client := mocks.NewMockDataCoordClient(t)
	rpcStarted := make(chan struct{})
	client.EXPECT().GetStreamingNodeQueryViewResources(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
			close(rpcStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
	oracle := newTestLazyOracleRuntime(t, client, version)

	result := make(chan error, 1)
	go func() {
		_, _, err := oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
		result <- err
	}()
	<-rpcStarted
	oracle.Close()

	require.ErrorIs(t, <-result, context.Canceled)
	_, _, err := oracle.BuildIDF(context.Background(), version, testBM25OutputFieldID, &schemapb.SparseFloatArray{})
	require.ErrorIs(t, err, context.Canceled)
}

func newTestLazyOracleRuntime(t *testing.T, client *mocks.MockDataCoordClient, version qviews.DataVersion, opts ...ProviderOption) *oracleRuntime {
	t.Helper()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	provider := NewProvider(client, append([]ProviderOption{WithNodeScheduler(scheduler)}, opts...)...)
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	oracle, err := newOracleRuntime(
		context.Background(),
		provider,
		testBM25WALView(version),
		nil,
		true,
	)
	require.NoError(t, err)
	return oracle
}

func newTestEagerOracleRuntime(t *testing.T, client *mocks.MockDataCoordClient, version qviews.DataVersion, opts ...ProviderOption) *oracleRuntime {
	t.Helper()
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	provider := NewProvider(client, append([]ProviderOption{WithNodeScheduler(scheduler)}, opts...)...)
	provider.sealedCache = newSegmentCacheAt(t.TempDir())
	oracle, err := newOracleRuntime(
		context.Background(),
		provider,
		testBM25WALView(version),
		nil,
		false,
	)
	require.NoError(t, err)
	return oracle
}

func testBM25WALView(version qviews.DataVersion) walview.VChannelWALView {
	return walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "test-vchannel",
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: testBM25InputFieldID, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: testBM25OutputFieldID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			},
			Functions: []*schemapb.FunctionSchema{{
				Name:             "bm25",
				Type:             schemapb.FunctionType_BM25,
				InputFieldIds:    []int64{testBM25InputFieldID},
				OutputFieldIds:   []int64{testBM25OutputFieldID},
				InputFieldNames:  []string{"text"},
				OutputFieldNames: []string{"sparse"},
			}},
		},
		LoadFields: []*messagespb.LoadFieldConfig{{FieldId: testBM25OutputFieldID}},
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: 1,
			VChannel:     "test-vchannel",
			DataVersion:  version,
		},
	}
}

func testBM25ResourceResponse(req *datapb.GetStreamingNodeQueryViewResourcesRequest) *datapb.GetStreamingNodeQueryViewResourcesResponse {
	return &datapb.GetStreamingNodeQueryViewResourcesResponse{
		Status:       merr.Success(),
		CollectionId: req.GetCollectionId(),
		Vchannel:     req.GetVchannel(),
		DataVersion:  req.GetDataVersion(),
	}
}

func oracleStatsReady(oracle *oracleRuntime) bool {
	oracle.mu.RLock()
	defer oracle.mu.RUnlock()
	return oracle.currentStats != nil
}

func oraclePreparedReady(oracle *oracleRuntime, version qviews.DataVersion) bool {
	oracle.mu.RLock()
	defer oracle.mu.RUnlock()
	_, ok := oracle.prepared[version]
	return ok
}

func oracleCurrentVersion(oracle *oracleRuntime) qviews.DataVersion {
	oracle.mu.RLock()
	defer oracle.mu.RUnlock()
	return oracle.currentVersion
}

func testSealedBM25Resources(firstSegmentID int64, count int) []*datapb.StreamingNodeBM25Resource {
	resources := make([]*datapb.StreamingNodeBM25Resource, 0, count)
	for i := range count {
		segmentID := firstSegmentID + int64(i)
		resources = append(resources, &datapb.StreamingNodeBM25Resource{
			SegmentId:      segmentID,
			PartitionId:    segmentID,
			StorageVersion: storage.StorageV2,
			Bm25Binlogs: []*datapb.FieldBinlog{{
				FieldID: testBM25OutputFieldID,
				Binlogs: []*datapb.Binlog{{
					LogPath: fmt.Sprintf("stats-%d", segmentID),
				}},
			}},
		})
	}
	return resources
}
