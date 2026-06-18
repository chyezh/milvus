package idf

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var _ viewresource.IDFOracleRuntimeBuilder = (*Provider)(nil)

// Provider loads sealed BM25 resources for a DataVersion and aggregates the
// WALView growing BM25 stats into a runtime oracle.
type Provider struct {
	client       querypb.QueryCoordClient
	chunkManager storage.ChunkManager
	sealedCache  *segmentCache
}

type ProviderOption func(*Provider)

func WithChunkManager(chunkManager storage.ChunkManager) ProviderOption {
	return func(p *Provider) {
		p.chunkManager = chunkManager
	}
}

func NewProvider(client querypb.QueryCoordClient, opts ...ProviderOption) *Provider {
	provider := &Provider{client: client, sealedCache: newSegmentCache()}
	for _, opt := range opts {
		opt(provider)
	}
	return provider
}

type FutureProvider struct {
	client       *syncutil.Future[types.MixCoordClient]
	chunkManager storage.ChunkManager
	sealedCache  *segmentCache
}

func NewFutureProvider(client *syncutil.Future[types.MixCoordClient], opts ...ProviderOption) *FutureProvider {
	provider := &Provider{}
	for _, opt := range opts {
		opt(provider)
	}
	return &FutureProvider{
		client:       client,
		chunkManager: provider.chunkManager,
		sealedCache:  newSegmentCache(),
	}
}

func (p *FutureProvider) NewRuntime(desc viewresource.LoadResourceDescriptor) (viewresource.IDFOracleRuntime, error) {
	return &Runtime{future: p, desc: desc}, nil
}

func (p *Provider) NewRuntime(desc viewresource.LoadResourceDescriptor) (viewresource.IDFOracleRuntime, error) {
	return &Runtime{provider: p, desc: desc}, nil
}

type Runtime struct {
	mu       sync.RWMutex
	provider *Provider
	future   *FutureProvider
	desc     viewresource.LoadResourceDescriptor
	oracle   *oracleRuntime
	closed   bool
}

func (r *Runtime) Prepare(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return context.Canceled
	}
	if r.oracle != nil {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	provider, err := r.resolveProvider(ctx)
	if err != nil {
		return err
	}
	oracle, err := provider.buildOracle(ctx, r.desc)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if oracle != nil {
			oracle.Close()
		}
		return context.Canceled
	}
	r.oracle = oracle
	return nil
}

func (r *Runtime) resolveProvider(ctx context.Context) (*Provider, error) {
	if r.provider != nil {
		return r.provider, nil
	}
	if r.future == nil {
		return nil, errors.New("IDF oracle provider is nil")
	}
	schema := r.desc.Schema()
	settings := r.desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return &Provider{}, nil
	}
	if r.future.client == nil {
		return nil, errors.New("mixcoord client future is nil")
	}
	client, err := r.future.client.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Provider{
		client:       client,
		chunkManager: r.future.chunkManager,
		sealedCache:  r.future.sealedCache,
	}, nil
}

func (p *Provider) buildOracle(ctx context.Context, desc viewresource.LoadResourceDescriptor) (*oracleRuntime, error) {
	schema := desc.Schema()
	settings := desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return nil, nil
	}
	if p.client == nil {
		return nil, errors.New("querycoord client is nil")
	}

	resources, err := p.getSealedBM25Resources(ctx, desc.CollectionID(), desc.VChannel(), desc.DataVersion(), settings)
	if err != nil {
		return nil, err
	}
	return newOracleRuntime(ctx, p, desc, resources)
}

func (r *Runtime) BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	oracle := r.currentOracle()
	if oracle == nil {
		return nil, 0, errors.New("IDF oracle is not initialized")
	}
	return oracle.BuildIDF(fieldID, tfs)
}

func (r *Runtime) ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if oracle := r.currentOracle(); oracle != nil {
		oracle.ApplyLiveEvent(ctx, event)
	}
}

func (r *Runtime) Advance(oldestDataVersion qviews.DataVersion) {
	if oracle := r.currentOracle(); oracle != nil {
		oracle.Advance(oldestDataVersion)
	}
}

func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	oracle := r.oracle
	r.oracle = nil
	r.mu.Unlock()
	if oracle != nil {
		oracle.Close()
	}
}

func (r *Runtime) currentOracle() *oracleRuntime {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.oracle
}

func collectGrowingInsertStats(stats bm25Stats, schema *schemapb.CollectionSchema, insert walview.SegmentInsertMessage) error {
	body := insert.Message.MustBody()
	if body == nil {
		return errors.New("bm25 growing insert message has nil request")
	}
	request := proto.Clone(body).(*msgpb.InsertRequest)
	request.PartitionID = insert.Assignment.GetPartitionId()
	request.SegmentID = insert.Assignment.GetSegmentAssignment().GetSegmentId()
	insertData, err := storage.ColumnBasedInsertMsgToInsertData(&msgstream.InsertMsg{InsertRequest: request}, schema)
	if err != nil {
		return err
	}
	for fieldID, fieldStats := range stats {
		fieldData, ok := insertData.Data[fieldID].(*storage.SparseFloatVectorFieldData)
		if !ok {
			continue
		}
		fieldStats.AppendFieldData(fieldData)
	}
	return nil
}

func validateResourceResponseFor(collectionID int64, vchannel string, dataVersion qviews.DataVersion, resp *querypb.GetStreamingNodeQueryViewResourcesResponse) error {
	if resp.GetCollectionId() != collectionID {
		return errors.Errorf(
			"bm25 resource response mismatch: request collection %d, response collection %d",
			collectionID,
			resp.GetCollectionId(),
		)
	}
	if resp.GetVchannel() != vchannel {
		return errors.Errorf(
			"bm25 resource response mismatch: request vchannel %s, response vchannel %s",
			vchannel,
			resp.GetVchannel(),
		)
	}
	if resp.GetDataVersion() == nil {
		return errors.New("bm25 resource response mismatch: response data version is nil")
	}
	responseVersion := qviews.FromProtoDataVersion(resp.GetDataVersion())
	if !responseVersion.EQ(dataVersion) {
		return errors.Errorf(
			"bm25 resource response mismatch: request data version %s, response data version %s",
			dataVersion.String(),
			responseVersion.String(),
		)
	}
	return nil
}

func hasLoadedBM25Function(schema *schemapb.CollectionSchema, loadedFields []int64) bool {
	if schema == nil {
		return false
	}
	loaded := lo.SliceToMap(loadedFields, func(fieldID int64) (int64, struct{}) {
		return fieldID, struct{}{}
	})
	loadsAllFields := len(loadedFields) == 0
	for _, function := range schema.GetFunctions() {
		if function.GetType() != schemapb.FunctionType_BM25 || len(function.GetOutputFieldIds()) == 0 {
			continue
		}
		if loadsAllFields {
			return true
		}
		if _, ok := loaded[function.GetOutputFieldIds()[0]]; ok {
			return true
		}
	}
	return false
}
