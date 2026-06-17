package idf

import (
	"context"

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

func (p *FutureProvider) BuildInitial(ctx context.Context, desc viewresource.LoadResourceDescriptor) (*viewresource.BM25Runtime, error) {
	schema := desc.Schema()
	settings := desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return newReadyBM25Runtime(), nil
	}
	if p.client == nil {
		return nil, errors.New("mixcoord client future is nil")
	}
	client, err := p.client.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return (&Provider{
		client:       client,
		chunkManager: p.chunkManager,
		sealedCache:  p.sealedCache,
	}).BuildInitial(ctx, desc)
}

func (p *Provider) BuildInitial(ctx context.Context, desc viewresource.LoadResourceDescriptor) (*viewresource.BM25Runtime, error) {
	schema := desc.Schema()
	settings := desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return newReadyBM25Runtime(), nil
	}
	if p.client == nil {
		return nil, errors.New("querycoord client is nil")
	}

	resources, err := p.getSealedBM25Resources(ctx, desc.CollectionID(), desc.VChannel(), desc.DataVersion(), settings)
	if err != nil {
		return nil, err
	}
	bm25Resources := make([]*viewresource.BM25SegmentResource, 0, len(resources))
	for _, resource := range resources {
		bm25Resources = append(bm25Resources, &viewresource.BM25SegmentResource{
			SegmentID:      resource.GetSegmentId(),
			PartitionID:    resource.GetPartitionId(),
			BM25Binlogs:    resource.GetBm25Binlogs(),
			StorageVersion: resource.GetStorageVersion(),
			ManifestPath:   resource.GetManifestPath(),
		})
	}
	growingSegmentIDs := make([]int64, 0, len(desc.WALView.SegmentSnapshot.Segments))
	for _, segment := range desc.WALView.SegmentSnapshot.Segments {
		growingSegmentIDs = append(growingSegmentIDs, segment.SegmentID)
	}
	runtime, err := newOracleRuntime(ctx, p, desc, resources)
	if err != nil {
		return nil, err
	}
	bm25 := &viewresource.BM25Runtime{
		Resources:         bm25Resources,
		GrowingSegmentIDs: growingSegmentIDs,
		Oracle:            runtime,
		LiveUpdater:       runtime,
		Advancer:          runtime,
		OnClose:           runtime.Close,
	}
	return bm25, nil
}

func newReadyBM25Runtime() *viewresource.BM25Runtime {
	runtime := &viewresource.BM25Runtime{}
	runtime.MarkCatchupDone()
	return runtime
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
