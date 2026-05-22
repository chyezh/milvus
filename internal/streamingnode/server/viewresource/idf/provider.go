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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var _ viewresource.BM25Provider = (*Provider)(nil)

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

func (p *FutureProvider) PrepareLatestFromAlterLoadConfig(ctx context.Context, desc viewresource.LoadResourceDescriptor) (*viewresource.BM25Runtime, error) {
	schema := desc.Schema()
	settings := desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return &viewresource.BM25Runtime{}, nil
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
	}).PrepareLatestFromAlterLoadConfig(ctx, desc)
}

func (p *Provider) PrepareLatestFromAlterLoadConfig(ctx context.Context, desc viewresource.LoadResourceDescriptor) (*viewresource.BM25Runtime, error) {
	schema := desc.Schema()
	settings := desc.Settings()
	if !hasLoadedBM25Function(schema, settings.GetRequiredFields()) {
		return &viewresource.BM25Runtime{}, nil
	}
	if p.client == nil {
		return nil, errors.New("querycoord client is nil")
	}

	resp, err := p.client.GetStreamingNodeQueryViewResources(ctx, &querypb.GetStreamingNodeQueryViewResourcesRequest{
		CollectionId: desc.CollectionID(),
		Vchannel:     desc.VChannel(),
		DataVersion:  desc.DataVersion().IntoProto(),
		Settings:     settings,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	if err := validateResourceResponse(desc, resp); err != nil {
		return nil, err
	}

	resources := make([]*viewresource.BM25SegmentResource, 0, len(resp.GetBm25Resources()))
	for _, resource := range resp.GetBm25Resources() {
		resources = append(resources, &viewresource.BM25SegmentResource{
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
	stats, lease, err := p.prepareStats(ctx, desc, resp.GetBm25Resources())
	if err != nil {
		return nil, err
	}
	var onClose func()
	if lease != nil {
		onClose = lease.Close
	}
	oracle := newOracle(stats, schema, growingSegmentIDs)
	return &viewresource.BM25Runtime{
		Resources:         resources,
		GrowingSegmentIDs: growingSegmentIDs,
		Oracle:            oracle,
		LiveUpdater:       oracle,
		OnClose:           onClose,
	}, nil
}

func (p *Provider) prepareStats(
	ctx context.Context,
	desc viewresource.LoadResourceDescriptor,
	resources []*querypb.StreamingNodeBM25Resource,
) (bm25Stats, *segmentCacheLease, error) {
	stats := newBM25StatsFromSchema(desc.Schema())
	var lease *segmentCacheLease
	if p.chunkManager != nil {
		if p.sealedCache == nil {
			p.sealedCache = newSegmentCache()
		}
		sealedStats, sealedLease, err := p.sealedCache.loadSealedStats(ctx, p.chunkManager, resources)
		if err != nil {
			return nil, nil, err
		}
		lease = sealedLease
		mergeBM25Stats(stats, sealedStats)
	}
	if err := collectGrowingStats(stats, desc); err != nil {
		if lease != nil {
			lease.Close()
		}
		return nil, nil, err
	}
	return stats, lease, nil
}

func collectGrowingStats(stats bm25Stats, desc viewresource.LoadResourceDescriptor) error {
	for _, segment := range desc.WALView.SegmentSnapshot.Segments {
		for _, raw := range segment.Data.InsertMessages {
			if err := walview.ForEachSegmentInsertMessage(raw, segment.SegmentID, func(insert walview.SegmentInsertMessage) error {
				return collectGrowingInsertStats(stats, desc.Schema(), insert)
			}); err != nil {
				return err
			}
		}
	}
	return nil
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

func validateResourceResponse(desc viewresource.LoadResourceDescriptor, resp *querypb.GetStreamingNodeQueryViewResourcesResponse) error {
	if resp.GetCollectionId() != desc.CollectionID() {
		return errors.Errorf(
			"bm25 resource response mismatch: request collection %d, response collection %d",
			desc.CollectionID(),
			resp.GetCollectionId(),
		)
	}
	if resp.GetVchannel() != desc.VChannel() {
		return errors.Errorf(
			"bm25 resource response mismatch: request vchannel %s, response vchannel %s",
			desc.VChannel(),
			resp.GetVchannel(),
		)
	}
	if resp.GetDataVersion() == nil {
		return errors.New("bm25 resource response mismatch: response data version is nil")
	}
	responseVersion := qviews.FromProtoDataVersion(resp.GetDataVersion())
	if !responseVersion.EQ(desc.DataVersion()) {
		return errors.Errorf(
			"bm25 resource response mismatch: request data version %s, response data version %s",
			desc.DataVersion().String(),
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
