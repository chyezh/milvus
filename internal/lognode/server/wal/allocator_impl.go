package wal

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

// NewAllocator creates a new wal allocator due to configuration.
func NewAllocator() Allocator {
	return &allocatorImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		// TODO: Configurable.
		opener:       wal.NewMQBasedOpener(),
		idAllocator:  util.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, WALExtend](),
	}
}

// allocatorImpl is the implementation of wal.Allocator.
type allocatorImpl struct {
	lifetime     lifetime.Lifetime[lifetime.State]
	opener       wal.Opener
	idAllocator  *util.IDAllocator
	walInstances *typeutil.ConcurrentMap[int64, WALExtend] // store all wal instances allocated by these allocator.
}

// Allocate opens a wal instance for the channel.
func (a *allocatorImpl) Allocate(opt *AllocateOption) (WALExtend, error) {
	if a.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal allocator is on shutdown")
	}
	defer a.lifetime.Done()

	id := a.idAllocator.Allocate()
	wal, err := a.opener.Open(*opt.Channel)
	if err != nil {
		return nil, err
	}

	// wrap the wal into walExtend with cleanup function and interceptors.
	metrics.LogNodeWALTotal.WithLabelValues(paramtable.GetNodeIDString()).Inc()
	walExtend := newWALExtend(wal, func() {
		a.walInstances.Remove(id)
		log.Info("wal deleted from allocator", zap.Int64("id", id))
		metrics.LogNodeWALTotal.WithLabelValues(paramtable.GetNodeIDString()).Dec()
	}, opt.Builders...)
	a.walInstances.Insert(id, walExtend)
	log.Info("new wal created", zap.Int64("id", id))
	return walExtend, nil
}

// Close the wal allocator, release the underlying resources.
func (a *allocatorImpl) Close() {
	a.lifetime.SetState(lifetime.Stopped)
	a.lifetime.Wait()
	a.lifetime.Close()

	// close all wal instances.
	a.walInstances.Range(func(id int64, value WALExtend) bool {
		value.(wal.WAL).Close()
		log.Info("close scanner by allocator success", zap.Int64("id", id))
		return true
	})
	// close the opener
	a.opener.Close()
}
