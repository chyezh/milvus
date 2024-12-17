package flusherimpl

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

func newWALFlusher() *walFlusherImpl {
	broker := broker.NewCoordBroker(resource.Resource().DataCoordClient(), paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()
	return &walFlusherImpl{
		notifier:     syncutil.NewAsyncTaskNotifier[struct{}](),
		broker:       broker,
		fgMgr:        pipeline.NewFlowgraphManager(),
		syncMgr:      syncmgr.NewSyncManager(chunkManager),
		wbMgr:        writebuffer.NewManager(syncmgr.NewSyncManager(chunkManager)),
		cpUpdater:    util.NewChannelCheckpointUpdater(broker),
		chunkManager: chunkManager,
		dataServices: make(map[string]*dataService),
	}
}

type dataService struct {
	input          chan<- *msgstream.MsgPack
	handler        func(message.ImmutableMessage) []*msgstream.MsgPack
	ds             *pipeline.DataSyncService
	startMessageID message.MessageID
}

type walFlusherImpl struct {
	notifier     *syncutil.AsyncTaskNotifier[struct{}]
	wal          wal.WAL
	scanner      wal.Scanner
	broker       broker.Broker
	fgMgr        pipeline.FlowgraphManager
	syncMgr      syncmgr.SyncManager
	wbMgr        writebuffer.BufferManager
	cpUpdater    *util.ChannelCheckpointUpdater
	chunkManager storage.ChunkManager

	dataServices map[string]*dataService
}

func (impl *walFlusherImpl) recover() (wal.Scanner, error) {
	ctx := impl.notifier.Context()
	// Get the vchannels of the pchannel.
	vchannels, err := impl.getVchannels(ctx, impl.wal.Channel().Name)
	if err != nil {
		return nil, err
	}
	// Get the recovery info of the vchannels.
	impl.dataServices = impl.buildDataSyncServices(ctx, vchannels)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// TODO: we need more wal level checkpoint here.
	// Because vchannel level checkpoint can not promise the message not lost.
	for _, ds := range impl.dataServices {
	}

	return nil
}

// getVchannels gets the vchannels of current pchannel.
func (impl *walFlusherImpl) getVchannels(ctx context.Context, pchannel string) ([]string, error) {
	var vchannels []string
	if err := retry.Do(ctx, func() error {
		resp, err := resource.Resource().RootCoordClient().GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
			Pchannel: impl.wal.Channel().Name,
		})
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		for _, collection := range resp.GetCollections() {
			vchannels = append(vchannels, collection.Vchannel)
		}
		return nil
	}, retry.AttemptAlways()); err != nil {
		return nil, errors.Wrapf(err, "when get pchannel: %s at recovery", impl.wal.Channel().Name)
	}
	return vchannels, nil
}

// buildDataSyncServices builds the data sync services for the vchannels.
func (impl *walFlusherImpl) buildDataSyncServices(ctx context.Context, vchannels []string) map[string]*dataService {
	futures := make(map[string]*conc.Future[*dataService], len(vchannels))
	for _, vchannel := range vchannels {
		future := GetExecPool().Submit(func() (*dataService, error) {
			return impl.buildDataSyncService(ctx, vchannel)
		})
		futures[vchannel] = future
	}
	dataServices := make(map[string]*dataService, len(futures))

	for vchannel, future := range futures {
		ds, err := future.Await()
		if err == nil {
			dataServices[vchannel] = ds
		}
	}
	return dataServices
}

// buildDataSyncService builds the data sync service for the vchannel.
func (impl *walFlusherImpl) buildDataSyncService(ctx context.Context, vchannel string) (*dataService, error) {
	resp, err := impl.getRecoveryInfo(ctx, vchannel)
	if err != nil {
		return nil, err
	}

	// Build and add pipeline.
	input := make(chan *msgstream.MsgPack, 10)
	ds, err := pipeline.NewStreamingNodeDataSyncService(ctx,
		&util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             impl.broker,
			SyncMgr:            impl.syncMgr,
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: impl.wbMgr,
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(impl.wbMgr),
		},
		&datapb.ChannelWatchInfo{Vchan: resp.GetInfo(), Schema: resp.GetSchema()},
		input,
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _ := tt.Binlogs()
				resource.Resource().SegmentAssignStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		nil,
	)
	return &dataService{
		input:          input,
		handler:        nil,
		ds:             ds,
		startMessageID: adaptor.MustGetMessageIDFromMQWrapperIDBytes(impl.wal.WALName(), resp.GetInfo().GetSeekPosition().GetMsgID()),
	}, nil
}

func (impl *walFlusherImpl) getRecoveryInfo(ctx context.Context, vchannel string) (*datapb.GetChannelRecoveryInfoResponse, error) {
	var resp *datapb.GetChannelRecoveryInfoResponse
	err := retry.Do(ctx, func() error {
		var err error
		resp, err = resource.Resource().DataCoordClient().
			GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		// The channel has been dropped, skip to recover it.
		if len(resp.GetInfo().GetSeekPosition().GetMsgID()) == 0 && resp.GetInfo().GetSeekPosition().GetTimestamp() == math.MaxUint64 {
			log.Info("channel has been dropped, skip to create flusher for vchannel", zap.String("vchannel", vchannel))
			return retry.Unrecoverable(errChannelLifetimeUnrecoverable)
		}
		return nil
	}, retry.AttemptAlways())
	return resp, err
}

// Execute starts the wal flusher.
func (impl *walFlusherImpl) Execute() {
	defer func() {
		impl.notifier.Finish(struct{}{})
		if impl.scanner != nil {
			impl.scanner.Close()
		}
		impl.fgMgr.ClearFlowgraphs()
		impl.wbMgr.Stop()
		impl.cpUpdater.Close()
	}()
	go impl.cpUpdater.Start()
	impl.wbMgr.Start()

	if err := impl.recover(); err != nil {
		return
	}

	for msg := range impl.scanner.Chan() {
		if err := impl.dispatch(msg); err != nil {
			return
		}
	}
	panic("the channel should never be closed")
}

// handleMessage handles the message from wal.
func (impl *walFlusherImpl) dispatch(msg message.ImmutableMessage) error {
	// Do the data sync service management here.
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		if _, ok := impl.dataServices[msg.VChannel()]; ok {
			// There may be repeated CreateCollection operation. so we need to ignore the message.
			return nil
		}
		createCollectionMsg, err := message.AsImmutableCreateCollectionMessageV1(msg)
		if err != nil {
			// TODO: DPanic here.
			panic("the message type is not CreateCollectionMessage")
		}
		impl.whenCreateCollection(createCollectionMsg)
	case message.MessageTypeDropCollection:
		// flowgraph is removed by data sync service it self.
		defer func() {
			impl.dataServices[msg.VChannel()] = nil
			impl.msgPackHandler[msg.VChannel()] = nil
		}()
	}
	vchannel := msg.VChannel()
	if _, ok := impl.msgPackHandler[vchannel]; !ok {
		// There may be repeated DropCollection operation. so we need to ignore the message.
		return nil
	}

	msgPacks := impl.msgPackHandler[vchannel](msg)
	for _, msgPack := range msgPacks {
		select {
		case <-impl.notifier.Context().Done():
			return impl.notifier.Context().Err()
		case impl.dataServices[vchannel] <- msgPack:
		}
	}
	return nil
}

// whenCreateCollection, create a new datasync service for the collection.
func (impl *walFlusherImpl) whenCreateCollection(createCollectionMsg message.ImmutableCreateCollectionMessageV1) {
	createCollectionRequest, err := createCollectionMsg.Body()
	if err != nil {
		panic("the message body is not CreateCollectionRequest")
	}
	msgChan := make(chan *msgstream.MsgPack, 10)

	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(createCollectionRequest.GetSchema(), schema); err != nil {
		panic("failed to unmarshal collection schema")
	}
	ds := pipeline.NewEmptyStreamingNodeDataSyncService(
		context.Background(), // There's no any rpc in this function, so the context is not used here.
		&util.PipelineParams{
			Ctx:                context.Background(),
			Broker:             impl.broker,
			SyncMgr:            impl.syncMgr,
			ChunkManager:       impl.chunkManager,
			WriteBufferManager: impl.wbMgr,
			CheckpointUpdater:  impl.cpUpdater,
			Allocator:          idalloc.NewMAllocator(resource.Resource().IDAllocator()),
			MsgHandler:         newMsgHandler(impl.wbMgr),
		},
		msgChan,
		&datapb.VchannelInfo{
			CollectionID: createCollectionMsg.Header().GetCollectionId(),
			ChannelName:  createCollectionMsg.VChannel(),
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: createCollectionMsg.VChannel(),
				// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
				MsgID:     adaptor.MustGetMQWrapperIDFromMessage(createCollectionMsg.LastConfirmedMessageID()).Serialize(),
				MsgGroup:  "", // Not important any more.
				Timestamp: createCollectionMsg.TimeTick(),
			},
		},
		schema,
		func(t syncmgr.Task, err error) {
			if err != nil || t == nil {
				return
			}
			if tt, ok := t.(*syncmgr.SyncTask); ok {
				insertLogs, _, _ := tt.Binlogs()
				resource.Resource().SegmentAssignStatsManager().UpdateOnSync(tt.SegmentID(), stats.SyncOperationMetrics{
					BinLogCounterIncr:     1,
					BinLogFileCounterIncr: uint64(len(insertLogs)),
				})
			}
		},
		nil,
	)
	impl.addNewDataSyncService(createCollectionMsg.VChannel(), msgChan, ds, nil)
}

func (impl *walFlusherImpl) addNewDataSyncService(
	vchannel string,
	input chan<- *msgstream.MsgPack,
	ds *pipeline.DataSyncService,
	handler func(message.ImmutableMessage) []*msgstream.MsgPack,
) {
	if impl.dataServices[vchannel] != nil {
		panic("the vchannel is already registered")
	}
	impl.dataServices[vchannel] = input
	impl.fgMgr.AddFlowgraph(ds)
	impl.msgPackHandler[vchannel] = handler
}

func (impl *walFlusherImpl) Close() {
	impl.notifier.Cancel()
	impl.notifier.BlockUntilFinish()
}
