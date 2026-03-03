package io

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

var (
	statsPool         *conc.Pool[any]
	statsPoolInitOnce sync.Once
)

var (
	bfApplyPool         atomic.Pointer[conc.Pool[any]]
	bfApplyPoolInitOnce sync.Once
)

func initIOPool() {
	capacity := paramtable.Get().DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func GetOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}

func initStatsPool() {
	poolSize := paramtable.Get().DataNodeCfg.ChannelWorkPoolSize.GetAsInt()
	if poolSize <= 0 {
		poolSize = hardware.GetCPUNum()
	}
	statsPool = conc.NewPool[any](poolSize, conc.WithPreAlloc(false), conc.WithNonBlocking(false))
}

func GetOrCreateStatsPool() *conc.Pool[any] {
	statsPoolInitOnce.Do(initStatsPool)
	return statsPool
}

func initMultiReadPool() {
	capacity := paramtable.Get().DataNodeCfg.FileReadConcurrency.GetAsInt()
	if capacity > hardware.GetCPUNum() {
		capacity = hardware.GetCPUNum()
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getMultiReadPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initMultiReadPool)
	return ioPool
}

func resizePool(pool *conc.Pool[any], newSize int, tag string) {
	if newSize <= 0 {
		log.Warn(context.TODO(), "cannot set pool size to non-positive value", log.String("poolTag", tag), log.Int("newSize", newSize))
		return
	}

	err := pool.Resize(newSize)
	if err != nil {
		log.Warn(context.TODO(), "failed to resize pool", log.Err(err), log.String("poolTag", tag), log.Int("newSize", newSize))
		return
	}
	log.Info(context.TODO(), "pool resize successfully", log.String("poolTag", tag), log.Int("newSize", newSize))
}

func ResizeBFApplyPool(evt *config.Event) {
	if evt.HasUpdated {
		pt := paramtable.Get()
		newSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		resizePool(GetBFApplyPool(), newSize, "BFApplyPool")
	}
}

func initBFApplyPool() {
	bfApplyPoolInitOnce.Do(func() {
		pt := paramtable.Get()
		poolSize := hardware.GetCPUNum() * pt.QueryNodeCfg.BloomFilterApplyParallelFactor.GetAsInt()
		log.Info(context.TODO(), "init BFApplyPool", log.Int("poolSize", poolSize))
		pool := conc.NewPool[any](
			poolSize,
		)

		bfApplyPool.Store(pool)
		pt.Watch(pt.QueryNodeCfg.BloomFilterApplyParallelFactor.Key, config.NewHandler("dn.bfapply.parallel", ResizeBFApplyPool))
	})
}

func GetBFApplyPool() *conc.Pool[any] {
	initBFApplyPool()
	return bfApplyPool.Load()
}
