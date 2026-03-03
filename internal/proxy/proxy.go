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

package proxy

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/expr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// UniqueID is alias of typeutil.UniqueID
type UniqueID = typeutil.UniqueID

// Timestamp is alias of typeutil.Timestamp
type Timestamp = typeutil.Timestamp

// const sendTimeTickMsgInterval = 200 * time.Millisecond
// const channelMgrTickerInterval = 100 * time.Millisecond

// make sure Proxy implements types.Proxy
var _ types.Proxy = (*Proxy)(nil)

var (
	Params  = paramtable.Get()
	rateCol *ratelimitutil.RateCollector
)

// Proxy of milvus
type Proxy struct {
	milvuspb.UnimplementedMilvusServiceServer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Int32

	address  string
	mixCoord types.MixCoordClient

	simpleLimiter *SimpleLimiter

	chMgr channelsMgr

	sched *taskScheduler

	rowIDAllocator *allocator.IDAllocator
	tsoAllocator   *timestampAllocator

	metricsCacheManager *metricsinfo.MetricsCacheManager

	session  *sessionutil.Session
	shardMgr shardclient.ShardClientMgr

	searchResultCh chan *internalpb.SearchResults

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	// for load balance in replicas
	lbPolicy shardclient.LBPolicy

	// resource manager
	resourceManager resource.Manager

	// materialized view
	enableMaterializedView bool

	// delete rate limiter
	enableComplexDeleteLimit bool

	slowQueries *expirable.LRU[Timestamp, *metricsinfo.SlowQuery]
}

// NewProxy returns a Proxy struct.
func NewProxy(ctx context.Context, _ dependency.Factory) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	n := 1024 // better to be configurable
	resourceManager := resource.NewManager(10*time.Second, 20*time.Second, make(map[string]time.Duration))
	node := &Proxy{
		ctx:            ctx1,
		cancel:         cancel,
		searchResultCh: make(chan *internalpb.SearchResults, n),
		// shardMgr:        mgr,
		simpleLimiter: NewSimpleLimiter(Params.QuotaConfig.AllocWaitInterval.GetAsDuration(time.Millisecond), Params.QuotaConfig.AllocRetryTimes.GetAsUint()),
		// lbPolicy:        lbPolicy,
		resourceManager: resourceManager,
		slowQueries:     expirable.NewLRU[Timestamp, *metricsinfo.SlowQuery](20, nil, time.Minute*15),
	}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)
	expr.Register("proxy", node)
	hookutil.InitOnceHook()
	mlog.Debug(ctx, "create a new Proxy instance", mlog.Any("state", node.stateCode.Load()))
	return node, nil
}

// UpdateStateCode updates the state code of Proxy.
func (node *Proxy) UpdateStateCode(code commonpb.StateCode) {
	node.stateCode.Store(int32(code))
}

func (node *Proxy) GetStateCode() commonpb.StateCode {
	return commonpb.StateCode(node.stateCode.Load())
}

// Register registers proxy at etcd
func (node *Proxy) Register() error {
	node.session.Register()
	metrics.NumNodes.WithLabelValues(paramtable.GetStringNodeID(), typeutil.ProxyRole).Inc()
	mlog.Info(context.TODO(), "Proxy Register Finished")
	// TODO Reset the logger
	// Params.initLogCfg()
	return nil
}

// initSession initialize the session of Proxy.
func (node *Proxy) initSession() error {
	node.session = sessionutil.NewSession(node.ctx)
	if node.session == nil {
		return errors.New("new session failed, maybe etcd cannot be connected")
	}
	node.session.Init(typeutil.ProxyRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.ProxyRole, node.session.ServerID)
	return nil
}

// initRateCollector creates and starts rateCollector in Proxy.
func (node *Proxy) initRateCollector() error {
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, true)
	if err != nil {
		return err
	}
	rateCol.Register(internalpb.RateType_DMLInsert.String())
	rateCol.Register(internalpb.RateType_DMLDelete.String())
	// TODO: add bulkLoad rate
	rateCol.Register(internalpb.RateType_DQLSearch.String())
	rateCol.Register(internalpb.RateType_DQLQuery.String())
	return nil
}

// Init initialize proxy.
func (node *Proxy) Init() error {
	mlog.Info(context.TODO(), "init session for Proxy")
	if err := node.initSession(); err != nil {
		mlog.Warn(context.TODO(), "failed to init Proxy's session", mlog.Err(err))
		return err
	}
	mlog.Info(context.TODO(), "init session for Proxy done")

	err := node.initRateCollector()
	if err != nil {
		return err
	}
	mlog.Info(context.TODO(), "Proxy init rateCollector done", mlog.Int64("nodeID", paramtable.GetNodeID()))

	idAllocator, err := allocator.NewIDAllocator(node.ctx, node.mixCoord, paramtable.GetNodeID())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create id allocator",
			mlog.String("role", typeutil.ProxyRole), mlog.Int64("ProxyID", paramtable.GetNodeID()),
			mlog.Err(err))
		return err
	}
	node.rowIDAllocator = idAllocator
	mlog.Debug(context.TODO(), "create id allocator done", mlog.String("role", typeutil.ProxyRole), mlog.Int64("ProxyID", paramtable.GetNodeID()))

	tsoAllocator, err := newTimestampAllocator(node.mixCoord, paramtable.GetNodeID())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create timestamp allocator",
			mlog.String("role", typeutil.ProxyRole), mlog.Int64("ProxyID", paramtable.GetNodeID()),
			mlog.Err(err))
		return err
	}
	node.tsoAllocator = tsoAllocator
	mlog.Debug(context.TODO(), "create timestamp allocator done", mlog.String("role", typeutil.ProxyRole), mlog.Int64("ProxyID", paramtable.GetNodeID()))

	dmlChannelsFunc := getDmlChannelsFunc(node.ctx, node.mixCoord)
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, defaultInsertRepackFunc)
	node.chMgr = chMgr
	mlog.Debug(context.TODO(), "create channels manager done", mlog.String("role", typeutil.ProxyRole))

	node.sched, err = newTaskScheduler(node.ctx, node.tsoAllocator)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create task scheduler", mlog.String("role", typeutil.ProxyRole), mlog.Err(err))
		return err
	}
	mlog.Debug(context.TODO(), "create task scheduler done", mlog.String("role", typeutil.ProxyRole))

	node.enableComplexDeleteLimit = Params.QuotaConfig.ComplexDeleteLimitEnable.GetAsBool()
	node.metricsCacheManager = metricsinfo.NewMetricsCacheManager()
	mlog.Debug(context.TODO(), "create metrics cache manager done", mlog.String("role", typeutil.ProxyRole))

	if err := InitMetaCache(node.ctx, node.mixCoord); err != nil {
		mlog.Warn(context.TODO(), "failed to init meta cache", mlog.String("role", typeutil.ProxyRole), mlog.Err(err))
		return err
	}
	mlog.Debug(context.TODO(), "init meta cache done", mlog.String("role", typeutil.ProxyRole))

	node.shardMgr = shardclient.NewShardClientMgr(node.mixCoord)
	node.lbPolicy = shardclient.NewLBPolicyImpl(node.shardMgr)

	node.enableMaterializedView = Params.CommonCfg.EnableMaterializedView.GetAsBool()

	// Enable internal rand pool for UUIDv4 generation
	// This is NOT thread-safe and should only be called before the service starts and
	// there is no possibility that New or any other UUID V4 generation function will be called concurrently
	// Only proxy generates UUID for now, and one Milvus process only has one proxy
	uuid.EnableRandPool()
	mlog.Debug(context.TODO(), "enable rand pool for UUIDv4 generation")

	mlog.Info(context.TODO(), "init proxy done", mlog.Int64("nodeID", paramtable.GetNodeID()), mlog.String("Address", node.address))
	return nil
}

// Start starts a proxy node.
func (node *Proxy) Start() error {
	node.shardMgr.Start()
	mlog.Debug(context.TODO(), "start shard client manager done", mlog.String("role", typeutil.ProxyRole))

	node.lbPolicy.Start(node.ctx)

	if err := node.sched.Start(); err != nil {
		mlog.Warn(context.TODO(), "failed to start task scheduler", mlog.String("role", typeutil.ProxyRole), mlog.Err(err))
		return err
	}
	mlog.Debug(context.TODO(), "start task scheduler done", mlog.String("role", typeutil.ProxyRole))

	if err := node.rowIDAllocator.Start(); err != nil {
		mlog.Warn(context.TODO(), "failed to start id allocator", mlog.String("role", typeutil.ProxyRole), mlog.Err(err))
		return err
	}
	mlog.Debug(context.TODO(), "start id allocator done", mlog.String("role", typeutil.ProxyRole))

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey: hookutil.OpTypeNodeID,
		hookutil.NodeIDKey: paramtable.GetNodeID(),
	})

	mlog.Debug(context.TODO(), "update state code", mlog.String("role", typeutil.ProxyRole), mlog.String("State", commonpb.StateCode_Healthy.String()))
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	// register devops api
	RegisterMgrRoute(node)

	return nil
}

// Stop stops a proxy node.
func (node *Proxy) Stop() error {
	if node.rowIDAllocator != nil {
		node.rowIDAllocator.Close()
		mlog.Info(context.TODO(), "close id allocator", mlog.String("role", typeutil.ProxyRole))
	}

	if node.sched != nil {
		node.sched.Close()
		mlog.Info(context.TODO(), "close scheduler", mlog.String("role", typeutil.ProxyRole))
	}

	for _, cb := range node.closeCallbacks {
		cb()
	}

	if node.session != nil {
		node.session.Stop()
	}

	if node.shardMgr != nil {
		node.shardMgr.Close()
	}

	if node.lbPolicy != nil {
		node.lbPolicy.Close()
	}

	if node.resourceManager != nil {
		node.resourceManager.Close()
	}

	node.cancel()
	node.wg.Wait()

	// https://github.com/milvus-io/milvus/issues/12282
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	connection.GetManager().Stop()
	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (node *Proxy) AddStartCallback(callbacks ...func()) {
	node.startCallbacks = append(node.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (node *Proxy) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

func (node *Proxy) SetAddress(address string) {
	node.address = address
}

func (node *Proxy) GetAddress() string {
	return node.address
}

// SetMixCoordClient sets MixCoord client for proxy.
func (node *Proxy) SetMixCoordClient(cli types.MixCoordClient) {
	node.mixCoord = cli
}

func (node *Proxy) SetQueryNodeCreator(f func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)) {
	node.shardMgr.SetClientCreatorFunc(f)
}

// GetRateLimiter returns the rateLimiter in Proxy.
func (node *Proxy) GetRateLimiter() (types.Limiter, error) {
	if node.simpleLimiter == nil {
		return nil, errors.New("nil rate limiter in Proxy")
	}
	return node.simpleLimiter, nil
}
