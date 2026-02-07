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
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	globalIDAllocator "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	connMetaMaxRetryTime = 100
	allPartitionID       = 0 // partitionID means no filtering
)

var (
	// TODO: sunby put to config
	enableTtChecker           = true
	ttCheckerName             = "dataTtChecker"
	ttMaxInterval             = 2 * time.Minute
	ttCheckerWarnMsg          = fmt.Sprintf("Datacoord haven't received tt for %f minutes", ttMaxInterval.Minutes())
	segmentTimedFlushDuration = 10.0
)

type (
	// UniqueID shortcut for typeutil.UniqueID
	UniqueID = typeutil.UniqueID
	// Timestamp shortcurt for typeutil.Timestamp
	Timestamp = typeutil.Timestamp
)

type mixCoordCreatorFunc func(ctx context.Context) (types.MixCoord, error)

// makes sure Server implements `DataCoord`
var _ types.DataCoord = (*Server)(nil)

var Params = paramtable.Get()

// Server implements `types.DataCoord`
// handles Data Coordinator related jobs
type Server struct {
	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel context.CancelFunc
	serverLoopWg     sync.WaitGroup
	quitCh           chan struct{}
	stateCode        atomic.Value

	etcdCli        *clientv3.Client
	tikvCli        *txnkv.Client
	address        string
	watchClient    kv.WatchKV
	kv             kv.MetaKv
	metaRootPath   string
	meta           *meta
	segmentManager Manager
	allocator      allocator.Allocator
	// self host id allocator, to avoid get unique id from rootcoord
	idAllocator      *globalIDAllocator.GlobalIDAllocator
	nodeManager      session.NodeManager
	cluster2         session.Cluster
	mixCoord         types.MixCoord
	garbageCollector *garbageCollector
	gcOpt            GcOption
	handler          Handler
	importMeta       ImportMeta
	importInspector  ImportInspector
	importChecker    ImportChecker

	copySegmentMeta      CopySegmentMeta
	copySegmentInspector CopySegmentInspector
	copySegmentChecker   CopySegmentChecker

	snapshotManager SnapshotManager

	compactionTrigger        trigger
	compactionInspector      CompactionInspector
	compactionTriggerManager TriggerManager

	metricsCacheManager *metricsinfo.MetricsCacheManager

	flushCh         chan UniqueID
	notifyIndexChan chan UniqueID
	factory         dependency.Factory

	session          sessionutil.SessionInterface
	icSession        sessionutil.SessionInterface
	dnSessionWatcher sessionutil.SessionWatcher
	qnSessionWatcher sessionutil.SessionWatcher

	enableActiveStandBy bool
	activateFunc        func() error

	dataNodeCreator session.DataNodeCreatorFunc
	mixCoordCreator mixCoordCreatorFunc
	// indexCoord             types.IndexCoord

	// segReferManager  *SegmentReferenceManager
	indexEngineVersionManager IndexEngineVersionManager

	statsInspector              *statsInspector
	indexInspector              *indexInspector
	analyzeInspector            *analyzeInspector
	externalCollectionInspector *externalCollectionInspector
	globalScheduler             task.GlobalScheduler

	// manage ways that data coord access other coord
	broker broker.Broker

	metricsRequest *metricsinfo.MetricsRequest

	// file resource
	fileResourceObserver FileResourceObserver
}

type FileResourceObserver interface {
	InitDataCoord(manager session.NodeManager)
	Notify()
}

type CollectionNameInfo struct {
	CollectionName string
	DBName         string
}

// Option utility function signature to set DataCoord server attributes
type Option func(svr *Server)

func WithMixCoordCreator(creator mixCoordCreatorFunc) Option {
	return func(svr *Server) {
		svr.mixCoordCreator = creator
	}
}

// WithDataNodeCreator returns an `Option` setting DataNode create function
func WithDataNodeCreator(creator session.DataNodeCreatorFunc) Option {
	return func(svr *Server) {
		svr.dataNodeCreator = creator
	}
}

// WithSegmentManager returns an Option to set SegmentManager
func WithSegmentManager(manager Manager) Option {
	return func(svr *Server) {
		svr.segmentManager = manager
	}
}

// CreateServer creates a `Server` instance
func CreateServer(ctx context.Context, factory dependency.Factory, opts ...Option) *Server {
	rand.Seed(time.Now().UnixNano())
	s := &Server{
		ctx:                 ctx,
		quitCh:              make(chan struct{}),
		factory:             factory,
		flushCh:             make(chan UniqueID, 1024),
		notifyIndexChan:     make(chan UniqueID, 1024),
		dataNodeCreator:     defaultDataNodeCreatorFunc,
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		metricsRequest:      metricsinfo.NewMetricsRequest(),
	}

	for _, opt := range opts {
		opt(s)
	}
	expr.Register("datacoord", s)
	return s
}

func defaultDataNodeCreatorFunc(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
	return datanodeclient.NewClient(ctx, addr, nodeID, Params.DataCoordCfg.WithCredential.GetAsBool())
}

func (s *Server) SetFileResourceObserver(observer FileResourceObserver) {
	s.fileResourceObserver = observer
}

// QuitSignal returns signal when server quits
func (s *Server) QuitSignal() <-chan struct{} {
	return s.quitCh
}

// Register registers data service at etcd
func (s *Server) Register() error {
	return nil
}

func (s *Server) ServerExist(serverID int64) bool {
	sessions, _, err := s.session.GetSessions(s.ctx, typeutil.DataNodeRole)
	if err != nil {
		mlog.Warn(s.ctx, "failed to get sessions", mlog.Err(err))
		return false
	}
	sessionMap := lo.MapKeys(sessions, func(s *sessionutil.Session, _ string) int64 {
		return s.ServerID
	})
	_, exists := sessionMap[serverID]
	return exists
}

// Init change server state to Initializing
func (s *Server) Init() error {
	s.registerMetricsRequest()
	s.factory.Init(Params)
	if err := s.initSession(); err != nil {
		return err
	}
	if err := s.initKV(); err != nil {
		return err
	}

	return s.initDataCoord()
}

func (s *Server) initDataCoord() error {

	mlog.Info(context.TODO(), "DataCoord try to wait for MixCoord ready")
	if err := s.initMixCoord(); err != nil {
		return err
	}

	s.UpdateStateCode(commonpb.StateCode_Initializing)

	s.broker = broker.NewCoordinatorBroker(s.mixCoord)
	s.allocator = allocator.NewRootCoordAllocator(s.mixCoord)

	storageCli, err := s.newChunkManagerFactory()
	if err != nil {
		return err
	}
	mlog.Info(context.TODO(), "init chunk manager factory done")

	if err = s.initMeta(storageCli); err != nil {
		return err
	}

	// init id allocator after init meta
	s.idAllocator = globalIDAllocator.NewGlobalIDAllocator("idTimestamp", s.kv)
	err = s.idAllocator.Initialize()
	if err != nil {
		mlog.Error(context.TODO(), "data coordinator id allocator initialize failed", mlog.Err(err))
		return err
	}

	s.handler = newServerHandler(s)

	// check whether old node exist, if yes suspend auto balance until all old nodes down
	s.updateBalanceConfigLoop(s.ctx)

	if err = s.initCluster(); err != nil {
		return err
	}
	mlog.Info(context.TODO(), "init datanode cluster done")

	if err = s.initServiceDiscovery(); err != nil {
		return err
	}
	mlog.Info(context.TODO(), "init service discovery done")

	s.globalScheduler = task.NewGlobalTaskScheduler(s.ctx, s.cluster2)

	s.importMeta, err = NewImportMeta(s.ctx, s.meta.catalog, s.allocator, s.meta)
	if err != nil {
		return err
	}
	s.initCompaction()
	mlog.Info(context.TODO(), "init compaction done")

	s.initAnalyzeInspector()
	mlog.Info(context.TODO(), "init analyze inspector done")

	s.initIndexInspector(storageCli)
	mlog.Info(context.TODO(), "init task scheduler done")

	s.initStatsInspector()
	mlog.Info(context.TODO(), "init statsJobManager done")

	// TODO: enable external collection inspector
	// s.initExternalCollectionInspector()
	// mlog.Info(context.TODO(), "init external collection inspector done")

	if err = s.initSegmentManager(); err != nil {
		return err
	}
	mlog.Info(context.TODO(), "init segment manager done")

	s.initGarbageCollection(storageCli)

	s.importInspector = NewImportInspector(s.ctx, s.meta, s.importMeta, s.globalScheduler)

	s.importChecker = NewImportChecker(s.ctx, s.meta, s.broker, s.allocator, s.importMeta, s.compactionInspector, s.handler, s.compactionTriggerManager)

	// init file resource observer
	if s.fileResourceObserver != nil {
		s.fileResourceObserver.InitDataCoord(s.nodeManager)
	}

	// Initialize copy segment meta and components
	s.copySegmentMeta, err = NewCopySegmentMeta(s.ctx, s.meta.catalog, s.meta, s.meta.snapshotMeta)
	if err != nil {
		return err
	}
	s.copySegmentInspector = NewCopySegmentInspector(
		s.ctx,
		s.meta,
		s.copySegmentMeta,
		s.globalScheduler,
	)

	s.copySegmentChecker = NewCopySegmentChecker(
		s.ctx,
		s.meta,
		s.broker,
		s.allocator,
		s.copySegmentMeta,
	)
	mlog.Info(context.TODO(), "init copy segment inspector and checker done")

	// Initialize snapshot manager
	s.snapshotManager = NewSnapshotManager(
		s.meta,
		s.meta.snapshotMeta,
		s.copySegmentMeta,
		s.allocator,
		s.handler,
		s.broker,
		s.getChannelsByCollectionID,
	)
	mlog.Info(context.TODO(), "init snapshot manager done")

	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)

	RegisterDDLCallbacks(s)
	mlog.Info(context.TODO(), "init datacoord done", mlog.Int64("nodeID", paramtable.GetNodeID()), mlog.String("Address", s.address))

	s.initMessageCallback()
	return nil
}

// initMessageCallback initializes the message callback.
// TODO: we should build a ddl framework to handle the message ack callback for ddl messages
func (s *Server) initMessageCallback() {
	registry.RegisterImportV1AckCallback(func(ctx context.Context, result message.BroadcastResultImportMessageV1) error {
		body := result.Message.MustBody()
		if body.Schema != nil {
			body.Schema.DbName = body.DbName
		}
		vchannels := result.GetVChannelsWithoutControlChannel()
		importResp, err := s.ImportV2(ctx, &internalpb.ImportRequestInternal{
			CollectionID:   body.GetCollectionID(),
			CollectionName: body.GetCollectionName(),
			PartitionIDs:   body.GetPartitionIDs(),
			ChannelNames:   vchannels,
			Schema:         body.GetSchema(),
			Files: lo.Map(body.GetFiles(), func(file *msgpb.ImportFile, _ int) *internalpb.ImportFile {
				return &internalpb.ImportFile{
					Id:    file.GetId(),
					Paths: file.GetPaths(),
				}
			}),
			Options:       funcutil.Map2KeyValuePair(body.GetOptions()),
			DataTimestamp: body.GetBase().GetTimestamp(),
			JobID:         body.GetJobID(),
		})
		err = merr.CheckRPCCall(importResp, err)
		if errors.Is(err, merr.ErrCollectionNotFound) {
			mlog.Warn(ctx, "import message failed because of collection not found, skip it", mlog.String("job_id", importResp.GetJobID()), mlog.Err(err))
			return nil
		}
		if err != nil {
			mlog.Warn(ctx, "import message failed", mlog.String("job_id", importResp.GetJobID()), mlog.Err(err))
			return err
		}
		mlog.Info(ctx, "import message handled", mlog.String("job_id", importResp.GetJobID()))
		return nil
	})

	registry.RegisterImportV1CheckCallback(func(ctx context.Context, msg message.BroadcastImportMessageV1) error {
		b := msg.MustBody()
		options := funcutil.Map2KeyValuePair(b.GetOptions())
		_, err := importutilv2.GetTimeoutTs(options)
		if err != nil {
			return err
		}
		err = ValidateBinlogImportRequest(ctx, s.meta.chunkManager, b.GetFiles(), options)
		if err != nil {
			return err
		}
		err = ValidateMaxImportJobExceed(ctx, s.importMeta)
		if err != nil {
			return err
		}
		balancer, err := balance.GetWithContext(ctx)
		if err != nil {
			return err
		}
		channelAssignment, err := balancer.GetLatestChannelAssignment()
		if err != nil {
			return err
		}
		replicateConfig := channelAssignment.ReplicateConfiguration
		if replicateConfig != nil && len(replicateConfig.GetClusters()) > 1 {
			return status.NewReplicateViolation("import in replicating cluster is not supported yet")
		}
		return nil
	})
}

// Start initialize `Server` members and start loops, follow steps are taken:
//  1. initialize message factory parameters
//  2. initialize root coord client, meta, datanode cluster, segment info channel,
//     allocator, segment manager
//  3. start service discovery and server loops, which includes message stream handler (segment statistics,datanode tt)
//     datanodes etcd watch, etcd alive check and flush completed status check
//  4. set server state to Healthy
func (s *Server) Start() error {
	s.startDataCoord()
	mlog.Info(context.TODO(), "DataCoord startup successfully")

	return nil
}

func (s *Server) startDataCoord() {
	s.startTaskScheduler()
	s.startServerLoop()
	s.afterStart()
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	sessionutil.SaveServerInfo(typeutil.MixCoordRole, s.session.GetServerID())
}

func (s *Server) GetServerID() int64 {
	if s.session != nil {
		return s.session.GetServerID()
	}
	return paramtable.GetNodeID()
}

func (s *Server) afterStart() {}

func (s *Server) initCluster() error {
	if s.nodeManager == nil {
		s.nodeManager = session.NewNodeManager(s.dataNodeCreator)
	}
	if s.cluster2 == nil {
		s.cluster2 = session.NewCluster(s.nodeManager)
	}
	return nil
}

func (s *Server) SetAddress(address string) {
	s.address = address
}

// SetEtcdClient sets etcd client for datacoord.
func (s *Server) SetEtcdClient(client *clientv3.Client) {
	s.etcdCli = client
}

func (s *Server) SetTiKVClient(client *txnkv.Client) {
	s.tikvCli = client
}

func (s *Server) SetMixCoord(mixCoord types.MixCoord) {
	s.mixCoord = mixCoord
}

func (s *Server) SetDataNodeCreator(f func(context.Context, string, int64) (types.DataNodeClient, error)) {
	s.dataNodeCreator = f
}

func (s *Server) SetSession(session sessionutil.SessionInterface) error {
	s.session = session
	s.icSession = session
	if s.session == nil {
		return errors.New("session is nil, the etcd client connection may have failed")
	}
	return nil
}

func (s *Server) newChunkManagerFactory() (storage.ChunkManager, error) {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(s.ctx)
	if err != nil {
		mlog.Error(context.TODO(), "chunk manager init failed", mlog.Err(err))
		return nil, err
	}
	return cli, err
}

func (s *Server) initGarbageCollection(cli storage.ChunkManager) {
	s.garbageCollector = newGarbageCollector(s.meta, s.handler, GcOption{
		cli:              cli,
		broker:           s.broker,
		enabled:          Params.DataCoordCfg.EnableGarbageCollection.GetAsBool(),
		checkInterval:    Params.DataCoordCfg.GCInterval.GetAsDuration(time.Second),
		scanInterval:     Params.DataCoordCfg.GCScanIntervalInHour.GetAsDuration(time.Hour),
		missingTolerance: Params.DataCoordCfg.GCMissingTolerance.GetAsDuration(time.Second),
		dropTolerance:    Params.DataCoordCfg.GCDropTolerance.GetAsDuration(time.Second),
	})
}

func (s *Server) initServiceDiscovery() error {
	r := semver.MustParseRange(">=2.2.3")
	sessions, rev, err := s.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		mlog.Warn(context.TODO(), "DataCoord failed to init service discovery", mlog.Err(err))
		return err
	}
	mlog.Info(context.TODO(), "DataCoord success to get DataNode sessions", mlog.Any("sessions", sessions))

	if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
		mlog.Info(context.TODO(), "initServiceDiscovery adding datanode with bind mode",
			mlog.Int64("nodeID", Params.DataCoordCfg.IndexNodeID.GetAsInt64()),
			mlog.String("address", Params.DataCoordCfg.IndexNodeAddress.GetValue()))
		if err := s.nodeManager.AddNode(Params.DataCoordCfg.IndexNodeID.GetAsInt64(),
			Params.DataCoordCfg.IndexNodeAddress.GetValue()); err != nil {
			mlog.Warn(context.TODO(), "DataCoord failed to add datanode", mlog.Err(err))
			return err
		}
		s.dnSessionWatcher = sessionutil.EmptySessionWatcher()
	} else {
		err := s.rewatchDataNodes(sessions)
		if err != nil {
			mlog.Warn(context.TODO(), "DataCoord failed to rewatch datanode", mlog.Err(err))
			return err
		}
		mlog.Info(context.TODO(), "DataCoord Cluster Manager start up successfully")

		s.dnSessionWatcher = s.session.WatchServicesWithVersionRange(typeutil.DataNodeRole, r, rev+1, s.rewatchDataNodes)
	}

	s.indexEngineVersionManager = newIndexEngineVersionManager()
	qnSessions, qnRevision, err := s.session.GetSessions(s.ctx, typeutil.QueryNodeRole)
	if err != nil {
		mlog.Warn(context.TODO(), "DataCoord get QueryNode sessions failed", mlog.Err(err))
		return err
	}
	s.rewatchQueryNodes(qnSessions)
	s.qnSessionWatcher = s.session.WatchServicesWithVersionRange(typeutil.QueryNodeRole, r, qnRevision+1, s.rewatchQueryNodes)

	return nil
}

// rewatchQueryNodes is used to rewatch query nodes when datacoord is started or reconnected to etcd
// Note: may apply same node multiple times, so rewatchQueryNodes must be idempotent
func (s *Server) rewatchQueryNodes(sessions map[string]*sessionutil.Session) error {
	s.indexEngineVersionManager.Startup(sessions)
	return nil
}

// rewatchDataNodes is used to rewatch data nodes when datacoord is started or reconnected to etcd
// Note: may apply same node multiple times, so rewatchDataNodes must be idempotent
func (s *Server) rewatchDataNodes(sessions map[string]*sessionutil.Session) error {
	legacyVersion, err := semver.Parse(paramtable.Get().DataCoordCfg.LegacyVersionWithoutRPCWatch.GetValue())
	if err != nil {
		mlog.Warn(context.TODO(), "DataCoord failed to init service discovery", mlog.Err(err))
		return err
	}

	datanodes := make([]*session.NodeInfo, 0, len(sessions))
	for _, ss := range sessions {
		info := &session.NodeInfo{
			NodeID:  ss.ServerID,
			Address: ss.Address,
		}

		if ss.Version.LTE(legacyVersion) {
			info.IsLegacy = true
		}

		datanodes = append(datanodes, info)
	}

	if err := s.nodeManager.Startup(s.ctx, datanodes); err != nil {
		mlog.Warn(context.TODO(), "DataCoord failed to add datanode", mlog.Err(err))
		return err
	}
	return nil
}

func (s *Server) initSegmentManager() error {
	if s.segmentManager == nil {
		manager, err := newSegmentManager(s.meta, s.allocator)
		if err != nil {
			return err
		}
		s.segmentManager = manager
	}
	return nil
}

func (s *Server) initSession() error {
	if s.icSession == nil {
		s.icSession = sessionutil.NewSession(s.ctx)
		s.icSession.Init(typeutil.IndexCoordRole, s.address, true, true)
		s.icSession.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	if s.session == nil {
		s.session = sessionutil.NewSession(s.ctx)

		s.session.Init(typeutil.DataCoordRole, s.address, true, true)
		s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	}
	return nil
}

func (s *Server) initKV() error {
	if s.kv != nil {
		return nil
	}
	s.watchClient = etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
		etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	metaType := Params.MetaStoreCfg.MetaStoreType.GetValue()
	mlog.Info(context.TODO(), "data coordinator connecting to metadata store", mlog.String("metaType", metaType))
	if metaType == util.MetaStoreTypeTiKV {
		s.metaRootPath = Params.TiKVCfg.MetaRootPath.GetValue()
		s.kv = tikv.NewTiKV(s.tikvCli, s.metaRootPath,
			tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else if metaType == util.MetaStoreTypeEtcd {
		s.metaRootPath = Params.EtcdCfg.MetaRootPath.GetValue()
		s.kv = etcdkv.NewEtcdKV(s.etcdCli, s.metaRootPath,
			etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
	} else {
		return retry.Unrecoverable(fmt.Errorf("not supported meta store: %s", metaType))
	}
	mlog.Info(context.TODO(), "data coordinator successfully connected to metadata store", mlog.String("metaType", metaType))
	return nil
}

func (s *Server) initMeta(chunkManager storage.ChunkManager) error {
	if s.meta != nil {
		return nil
	}
	reloadEtcdFn := func() error {
		var err error
		catalog := datacoord.NewCatalog(s.kv, chunkManager.RootPath(), s.metaRootPath)
		s.meta, err = newMeta(s.ctx, catalog, chunkManager, s.broker)
		if err != nil {
			return err
		}

		// Load collection information asynchronously
		// HINT: please make sure this is the last step in the `reloadEtcdFn` function !!!
		go func() {
			_ = retry.Do(s.ctx, func() error {
				return s.meta.reloadCollectionsFromRootcoord(s.ctx, s.broker)
			}, retry.Sleep(time.Second), retry.Attempts(connMetaMaxRetryTime))
		}()
		return nil
	}
	return retry.Do(s.ctx, reloadEtcdFn, retry.Attempts(connMetaMaxRetryTime))
}

func (s *Server) initAnalyzeInspector() {
	if s.analyzeInspector == nil {
		s.analyzeInspector = newAnalyzeInspector(s.ctx, s.meta, s.globalScheduler)
	}
}

func (s *Server) initIndexInspector(storageCli storage.ChunkManager) {
	if s.indexInspector == nil {
		s.indexInspector = newIndexInspector(s.ctx, s.notifyIndexChan, s.meta, s.globalScheduler, s.allocator, s.handler, storageCli, s.indexEngineVersionManager)
	}
}

func (s *Server) initStatsInspector() {
	if s.statsInspector == nil {
		s.statsInspector = newStatsInspector(s.ctx, s.meta, s.globalScheduler, s.allocator, s.handler, s.compactionInspector, s.indexEngineVersionManager)
	}
}

func (s *Server) initExternalCollectionInspector() {
	if s.externalCollectionInspector == nil {
		s.externalCollectionInspector = newExternalCollectionInspector(s.ctx, s.meta, s.globalScheduler, s.allocator)
	}
}

func (s *Server) initCompaction() {
	cph := newCompactionInspector(s.meta, s.allocator, s.handler, s.globalScheduler, s.indexEngineVersionManager)
	cph.loadMeta()
	s.compactionInspector = cph
	s.compactionTriggerManager = NewCompactionTriggerManager(s.allocator, s.handler, s.compactionInspector, s.meta, s.importMeta, s.indexEngineVersionManager)
	s.compactionTriggerManager.InitForceMergeMemoryQuerier(s.nodeManager, s.mixCoord, s.session)
	s.compactionTrigger = newCompactionTrigger(s.meta, s.compactionInspector, s.allocator, s.handler, s.indexEngineVersionManager)
}

func (s *Server) stopCompaction() {
	if s.compactionTrigger != nil {
		s.compactionTrigger.stop()
	}
	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Stop()
	}

	if s.compactionInspector != nil {
		s.compactionInspector.stop()
	}
}

func (s *Server) startCompaction() {
	if s.compactionInspector != nil {
		s.compactionInspector.start()
	}

	if s.compactionTrigger != nil {
		s.compactionTrigger.start()
	}

	if s.compactionTriggerManager != nil {
		s.compactionTriggerManager.Start()
	}
}

func (s *Server) startServerLoop() {
	if Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		s.startCompaction()
	}

	s.serverLoopWg.Add(2)
	s.startWatchService(s.serverLoopCtx)
	s.startFlushLoop(s.serverLoopCtx)
	s.globalScheduler.Start()
	go s.importInspector.Start()
	go s.importChecker.Start()

	// Start copy segment inspector and checker
	go s.copySegmentInspector.Start()
	go s.copySegmentChecker.Start()

	s.garbageCollector.start()
}

func (s *Server) startCollectMetaMetrics(ctx context.Context) {
	s.serverLoopWg.Add(1)
	go s.collectMetaMetrics(ctx)
}

func (s *Server) collectMetaMetrics(ctx context.Context) {
	defer s.serverLoopWg.Done()

	ticker := time.NewTicker(time.Second * 120)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mlog.Warn(s.ctx, "collectMetaMetrics ctx done")
			return
		case <-ticker.C:
			s.meta.statsTaskMeta.updateMetrics()
			s.meta.indexMeta.updateIndexTasksMetrics()
		}
	}
}

func (s *Server) startTaskScheduler() {
	s.statsInspector.Start()
	s.indexInspector.Start()
	s.analyzeInspector.Start()
	// TODO: enable external collection inspector
	// s.externalCollectionInspector.Start()
	s.startCollectMetaMetrics(s.serverLoopCtx)
}

func (s *Server) getFlushableSegmentsInfo(ctx context.Context, flushableIDs []int64) []*SegmentInfo {
	res := make([]*SegmentInfo, 0, len(flushableIDs))
	for _, id := range flushableIDs {
		sinfo := s.meta.GetHealthySegment(ctx, id)
		if sinfo == nil {
			mlog.Error(ctx, "get segment from meta error", mlog.Int64("id", id))
			continue
		}
		res = append(res, sinfo)
	}
	return res
}

func (s *Server) setLastFlushTime(segments []*SegmentInfo) {
	for _, sinfo := range segments {
		s.meta.SetLastFlushTime(sinfo.GetID(), time.Now())
	}
}

// start a goroutine wto watch services
func (s *Server) startWatchService(ctx context.Context) {
	go s.watchService(ctx)
}

func (s *Server) stopServiceWatch() {
	// ErrCompacted is handled inside SessionWatcher, which means there is some other error occurred, closing server.
	mlog.Error(s.ctx, "watch service channel closed", mlog.Int64("serverID", paramtable.GetNodeID()))
	go s.Stop()
	if s.session.IsTriggerKill() {
		if p, err := os.FindProcess(os.Getpid()); err == nil {
			p.Signal(syscall.SIGINT)
		}
	}
}

// watchService watches services.
func (s *Server) watchService(ctx context.Context) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()
	for {
		select {
		case <-ctx.Done():
			mlog.Info(ctx, "watch service shutdown")
			return
		case event, ok := <-s.dnSessionWatcher.EventChannel():
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.DataNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						mlog.Warn(ctx, "DataCoord server stop error", mlog.Err(err))
					}
				}()
				return
			}
		case event, ok := <-s.qnSessionWatcher.EventChannel():
			if !ok {
				s.stopServiceWatch()
				return
			}
			if err := s.handleSessionEvent(ctx, typeutil.QueryNodeRole, event); err != nil {
				go func() {
					if err := s.Stop(); err != nil {
						mlog.Warn(ctx, "DataCoord server stop error", mlog.Err(err))
					}
				}()
				return
			}
		}
	}
}

// handles session events - DataNodes Add/Del
func (s *Server) handleSessionEvent(ctx context.Context, role string, event *sessionutil.SessionEvent) error {
	if event == nil {
		return nil
	}
	switch role {
	case typeutil.DataNodeRole:
		info := &datapb.DataNodeInfo{
			Address:  event.Session.Address,
			Version:  event.Session.ServerID,
			Channels: []*datapb.ChannelStatus{},
		}
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			mlog.Info(ctx, "received datanode register",
				mlog.String("address", info.Address),
				mlog.Int64("serverID", info.Version))
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				mlog.Info(ctx, "receive datanode session event, but adding datanode by bind mode, skip it",
					mlog.String("address", event.Session.Address),
					mlog.Int64("serverID", event.Session.ServerID),
					mlog.String("event type", event.EventType.String()))
				return nil
			}
			err := s.nodeManager.AddNode(event.Session.ServerID, event.Session.Address)
			if err != nil {
				return err
			}

			// notify file manager sync file resource to new node
			if s.fileResourceObserver != nil {
				s.fileResourceObserver.Notify()
			}
			return nil
		case sessionutil.SessionDelEvent:
			mlog.Info(ctx, "received datanode unregister",
				mlog.String("address", info.Address),
				mlog.Int64("serverID", info.Version))
			s.metricsCacheManager.InvalidateSystemInfoMetrics()
			if Params.DataCoordCfg.BindIndexNodeMode.GetAsBool() {
				mlog.Info(ctx, "receive datanode session event, but adding datanode by bind mode, skip it",
					mlog.String("address", event.Session.Address),
					mlog.Int64("serverID", event.Session.ServerID),
					mlog.String("event type", event.EventType.String()))
				return nil
			}
			s.nodeManager.RemoveNode(event.Session.ServerID)
		default:
			mlog.Warn(ctx, "receive unknown service event type",
				mlog.Any("type", event.EventType))
		}
	case typeutil.QueryNodeRole:
		switch event.EventType {
		case sessionutil.SessionAddEvent:
			mlog.Info(context.TODO(), "received querynode register",
				mlog.String("address", event.Session.Address),
				mlog.Int64("serverID", event.Session.ServerID),
				mlog.Bool("indexNonEncoding", event.Session.IndexNonEncoding))
			s.indexEngineVersionManager.AddNode(event.Session)
		case sessionutil.SessionDelEvent:
			mlog.Info(context.TODO(), "received querynode unregister",
				mlog.String("address", event.Session.Address),
				mlog.Int64("serverID", event.Session.ServerID))
			s.indexEngineVersionManager.RemoveNode(event.Session)
		case sessionutil.SessionUpdateEvent:
			serverID := event.Session.ServerID
			mlog.Info(context.TODO(), "received querynode SessionUpdateEvent", mlog.Int64("serverID", serverID))
			s.indexEngineVersionManager.Update(event.Session)
		default:
			mlog.Warn(context.TODO(), "receive unknown service event type",
				mlog.Any("type", event.EventType))
		}
	}

	return nil
}

// startFlushLoop starts a goroutine to handle post func process
// which is to notify `RootCoord` that this segment is flushed
func (s *Server) startFlushLoop(ctx context.Context) {
	go func() {
		defer logutil.LogPanic()
		defer s.serverLoopWg.Done()
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()
		// send `Flushing` segments
		go s.handleFlushingSegments(ctx2)
		for {
			select {
			case <-ctx.Done():
				mlog.Info(s.ctx, "flush loop shutdown")
				return
			case segmentID := <-s.flushCh:
				// Ignore return error
				mlog.Info(ctx, "flush successfully", mlog.Any("segmentID", segmentID))
				err := s.postFlush(ctx, segmentID)
				if err != nil {
					mlog.Warn(ctx, "failed to do post flush", mlog.Int64("segmentID", segmentID), mlog.Err(err))
				}
			}
		}
	}()
}

// post function after flush is done
// 1. check segment id is valid
// 2. notify RootCoord segment is flushed
// 3. change segment state to `Flushed` in meta
func (s *Server) postFlush(ctx context.Context, segmentID UniqueID) error {
	segment := s.meta.GetHealthySegment(ctx, segmentID)
	if segment == nil {
		return merr.WrapErrSegmentNotFound(segmentID, "segment not found, might be a faked segment, ignore post flush")
	}

	if enableSortCompaction() {
		select {
		case getStatsTaskChSingleton() <- segmentID:
		default:
		}
	} else {
		select {
		case getBuildIndexChSingleton() <- segmentID:
		default:
		}
	}

	insertFileNum := 0
	for _, fieldBinlog := range segment.GetBinlogs() {
		insertFileNum += len(fieldBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

	statFileNum := 0
	for _, fieldBinlog := range segment.GetStatslogs() {
		statFileNum += len(fieldBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

	deleteFileNum := 0
	for _, filedBinlog := range segment.GetDeltalogs() {
		deleteFileNum += len(filedBinlog.GetBinlogs())
	}
	metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))

	mlog.Info(ctx, "flush segment complete", mlog.Int64("id", segmentID))
	return nil
}

// recovery logic, fetch all Segment in `Flushing` state and do Flush notification logic
func (s *Server) handleFlushingSegments(ctx context.Context) {
	segments := s.meta.GetFlushingSegments()
	for _, segment := range segments {
		// The old flushing segment may not be flushed, so we need to flush it again.
		// It should be retry until success
		if err := s.flushFlushingSegment(ctx, segment.ID); err != nil {
			mlog.Warn(ctx, "flush flushing segment failed", mlog.Int64("segmentID", segment.ID), mlog.Err(err))
			return
		}
		mlog.Info(ctx, "flush flushing segment success", mlog.Int64("segmentID", segment.ID))
		select {
		case <-ctx.Done():
			return
		case s.flushCh <- segment.ID:
		}
	}
}

// flushFlushingSegment flushes a segment to `Flushed` state
func (s *Server) flushFlushingSegment(ctx context.Context, segmentID UniqueID) error {
	return retry.Do(ctx, func() error {
		// set segment to SegmentState_Flushed
		var operators []UpdateOperator
		if enableSortCompaction() {
			operators = append(operators, SetSegmentIsInvisible(segmentID, true))
		}
		operators = append(operators, UpdateStatusOperator(segmentID, commonpb.SegmentState_Flushed))
		if err := s.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			mlog.Warn(ctx, "flush segment complete failed", mlog.Int64("segmentID", segmentID), mlog.Err(err))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// underlying etcd may return context canceled, so we need to return a error to retry.
			return errors.New("flush segment complete failed")
		}
		return nil
	}, retry.AttemptAlways())
}

func (s *Server) initMixCoord() error {
	var err error
	if s.mixCoord == nil {
		if s.mixCoord, err = s.mixCoordCreator(s.ctx); err != nil {
			return err
		}
	}
	return nil
}

// Stop do the Server finalize processes
// it checks the server status is healthy, if not, just quit
// if Server is healthy, set server state to stopped, release etcd session,
//
//	stop message stream client and stop server loops
func (s *Server) Stop() error {
	if !s.stateCode.CompareAndSwap(commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal) {
		return nil
	}
	mlog.Info(context.TODO(), "datacoord server shutdown")
	s.garbageCollector.close()
	mlog.Info(context.TODO(), "datacoord garbage collector stopped")

	if s.meta != nil {
		s.meta.GetSnapshotMeta().Close()
		mlog.Info(context.TODO(), "datacoord snapshot meta closed")
	}

	s.stopServerLoop()
	mlog.Info(context.TODO(), "datacoord stopServerLoop stopped")

	s.globalScheduler.Stop()
	s.importInspector.Close()
	s.importChecker.Close()

	// Stop copy segment components
	s.copySegmentInspector.Close()
	s.copySegmentChecker.Close()
	mlog.Info(context.TODO(), "datacoord copy segment inspector and checker stopped")

	s.stopCompaction()
	mlog.Info(context.TODO(), "datacoord compaction stopped")

	s.statsInspector.Stop()
	mlog.Info(context.TODO(), "datacoord stats inspector stopped")

	s.indexInspector.Stop()
	mlog.Info(context.TODO(), "datacoord index inspector stopped")

	s.analyzeInspector.Stop()
	mlog.Info(context.TODO(), "datacoord analyze inspector stopped")

	if s.dnSessionWatcher != nil {
		s.dnSessionWatcher.Stop()
	}

	if s.qnSessionWatcher != nil {
		s.qnSessionWatcher.Stop()
	}
	// TODO: enable external collection inspector
	// s.externalCollectionInspector.Stop()
	// mlog.Info(context.TODO(), "datacoord external collection inspector stopped")

	if s.session != nil {
		s.session.Stop()
	}

	if s.icSession != nil {
		s.icSession.Stop()
	}

	s.stopServerLoop()
	mlog.Info(context.TODO(), "datacoord serverloop stopped")
	mlog.Warn(context.TODO(), "datacoord stop successful")
	return nil
}

// CleanMeta only for test
func (s *Server) CleanMeta() error {
	mlog.Debug(s.ctx, "clean meta", mlog.Any("kv", s.kv))
	err := s.kv.RemoveWithPrefix(s.ctx, "")
	err2 := s.watchClient.RemoveWithPrefix(s.ctx, "")
	if err2 != nil {
		if err != nil {
			err = fmt.Errorf("Failed to CleanMeta[metadata cleanup error: %w][watchdata cleanup error: %v]", err, err2)
		} else {
			err = err2
		}
	}
	return err
}

func (s *Server) stopServerLoop() {
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Server) registerMetricsRequest() {
	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSystemInfoMetrics(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.DistKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getDistJSON(ctx, req), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ImportTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.importMeta.TaskStatsJSON(ctx), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.CompactionTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.meta.compactionTaskMeta.TaskStatsJSON(), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.BuildIndexTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.meta.indexMeta.TaskStatsJSON(), nil
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SyncTaskKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSyncTaskJSON(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.SegmentKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getSegmentsJSON(ctx, req, jsonReq)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.ChannelKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return s.getChannelsJSON(ctx, req)
		})

	s.metricsRequest.RegisterMetricsRequest(metricsinfo.IndexKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
			return s.meta.indexMeta.GetIndexJSON(collectionID), nil
		})
	mlog.Info(s.ctx, "register metrics actions finished")
}

// loadCollectionFromRootCoord communicates with RootCoord and asks for collection information.
// collection information will be added to server meta info.
func (s *Server) loadCollectionFromRootCoord(ctx context.Context, collectionID int64) error {
	has, err := s.broker.HasCollection(ctx, collectionID)
	if err != nil {
		return err
	}
	if !has {
		return merr.WrapErrCollectionNotFound(collectionID)
	}

	resp, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return err
	}
	partitionIDs, err := s.broker.ShowPartitionsInternal(ctx, collectionID)
	if err != nil {
		return err
	}

	properties := make(map[string]string)
	for _, pair := range resp.Properties {
		properties[pair.GetKey()] = pair.GetValue()
	}

	collInfo := &collectionInfo{
		ID:             resp.CollectionID,
		Schema:         resp.Schema,
		Partitions:     partitionIDs,
		StartPositions: resp.GetStartPositions(),
		Properties:     properties,
		CreatedAt:      resp.GetCreatedTimestamp(),
		DatabaseName:   resp.GetDbName(),
		DatabaseID:     resp.GetDbId(),
		VChannelNames:  resp.GetVirtualChannelNames(),
	}
	s.meta.AddCollection(collInfo)
	return nil
}

func (s *Server) updateBalanceConfigLoop(ctx context.Context) {
	success := s.updateBalanceConfig()
	if success {
		return
	}

	s.serverLoopWg.Add(1)
	go func() {
		defer s.serverLoopWg.Done()
		ticker := time.NewTicker(Params.DataCoordCfg.CheckAutoBalanceConfigInterval.GetAsDuration(time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				mlog.Info(ctx, "update balance config loop exit!")
				return

			case <-ticker.C:
				success := s.updateBalanceConfig()
				if success {
					return
				}
			}
		}
	}()
}

func (s *Server) updateBalanceConfig() bool {
	r := semver.MustParseRange("<2.3.0")
	sessions, _, err := s.session.GetSessionsWithVersionRange(typeutil.DataNodeRole, r)
	if err != nil {
		mlog.Warn(context.TODO(), "check data node version occur error on etcd", mlog.Err(err))
		return false
	}

	if len(sessions) == 0 {
		// only balance channel when all data node's version > 2.3.0
		Params.Reset(Params.DataCoordCfg.AutoBalance.Key)
		mlog.Info(context.TODO(), "all old data node down, enable auto balance!")
		return true
	}

	Params.Save(Params.DataCoordCfg.AutoBalance.Key, "false")
	mlog.RatedDebug(context.TODO(), 1.0/10, "old data node exist", mlog.Strings("sessions", lo.Keys(sessions)))
	return false
}

func (s *Server) listLoadedSegments(ctx context.Context) ([]int64, error) {
	req := &querypb.ListLoadedSegmentsRequest{}
	resp, err := s.mixCoord.ListLoadedSegments(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}

	return resp.SegmentIDs, nil
}
