package wal

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/scanner"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ReadOption is the option for reading records from the wal.
type ReadOption struct {
	DeliverPolicy options.DeliverPolicy
}

// newScannerManager creates a new scanner allocator.
func newScannerManager(manager scanner.Manager, channel logpb.PChannelInfo) *scannerManager {
	return &scannerManager{
		scannerRegistry: newScannerRegistry(channel),
		manager:         manager,
		scanners:        typeutil.NewConcurrentMap[string, scanner.Scanner](), // in using scanners, map the scanner name to scanner.
	}
}

// scannerManager is the implementation of ScannerAllocator.
type scannerManager struct {
	*scannerRegistry
	manager  scanner.Manager
	scanners *typeutil.ConcurrentMap[string, scanner.Scanner]
}

// Read returns a scanner for reading records started from startMessageID.
func (a *scannerManager) Read(ctx context.Context, opt ReadOption) (scanner.Scanner, error) {
	scannerName, err := a.AllocateScannerName()
	if err != nil {
		a.logger.Warn("allocate scanner name fail", zap.Error(err))
		return nil, status.NewInner("allocate scanner name fail: %s", err.Error())
	}

	scanner, err := a.manager.Allocate(ctx, scanner.AllocateParam{
		Channel:       a.channel,
		ScannerName:   scannerName,
		DeliverPolicy: opt.DeliverPolicy,
	})
	if err != nil {
		a.logger.Warn("create new scanner fail", zap.Error(err))
		return nil, status.NewInner("create a new scanner fail: %s", err.Error())
	}

	// wrap the scanner with cleanup function.
	scanner = &scannerCleanupWrapper{
		Scanner: scanner,
		cleanup: func() {
			a.scanners.Remove(scannerName)
			a.logger.Info("scanner deleted from allocator", zap.String("scannerName", scannerName))
		},
	}
	a.scanners.Insert(scannerName, scanner)
	a.logger.Info("new scanner created", zap.String("scannerName", scannerName))
	return scanner, nil
}

// GetLatestMessageID returns the latest message id of the channel.
func (a *scannerManager) GetLatestMessageID(ctx context.Context) (message.MessageID, error) {
	// MQBased WAL only implement GetLatestMessageID at Consumer.
	// So if there is no consumer, GetLatestMessageID will return error.
	// At common use case, consumer always created before GetLatestMessageID.
	if a.scanners.Len() == 0 {
		return nil, status.NewInner("no scanner found for get latest message id")
	}

	// found a consumer to get the latest message id.
	var msgID message.MessageID
	var err error
	a.scanners.Range(func(name string, s scanner.Scanner) bool {
		if q, ok := s.(scanner.LastMessageIDQuerier); ok {
			msgID, err = q.GetLatestMessageID(ctx)
		} else {
			err = status.NewInner("scanner do not implement LastMessageIDQuerier")
		}
		return false
	})
	return msgID, err
}

// DropAllExpiredScanners drop all expired scanner.
func (a *scannerManager) DropAllExpiredScanners(ctx context.Context) error {
	names, err := a.GetAllExpiredScannerNames()
	if err != nil {
		return status.NewInner("get all expired scanner names fail: %s", err.Error())
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(5)
	for _, n := range names {
		name := n
		g.Go(func() error {
			if err := a.manager.Drop(name); err != nil {
				return errors.Wrapf(err, "at scanner name: %s", name)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return status.NewInner("drop all expired scanner fail: %s", err.Error())
	}
	return nil
}

// Close the scanner allocator, release the underlying resources.
func (a *scannerManager) Close() {
	a.scanners.Range(func(scannerName string, scanner scanner.Scanner) bool {
		if err := scanner.Close(); err != nil {
			a.logger.Warn("fail to close scanner by allocator fail", zap.String("scannerName", scannerName), zap.Error(err))
			return true
		}
		a.logger.Info("close scanner by allocator success", zap.String("scannerName", scannerName))
		return true
	})
}

// newScannerRegistry creates a new scanner registry.
func newScannerRegistry(channel logpb.PChannelInfo) *scannerRegistry {
	return &scannerRegistry{
		logger:      log.With(zap.Any("channel", channel)),
		channel:     channel,
		idAllocator: util.NewIDAllocator(),
	}
}

// scannerRegistry is the a registry for manage name and gc of existed scanner.
type scannerRegistry struct {
	logger *log.MLogger

	channel     logpb.PChannelInfo
	idAllocator *util.IDAllocator
}

// AllocateScannerName a scanner name for a scanner.
// The scanner name should be persistent on meta for garbage clean up.
func (m *scannerRegistry) AllocateScannerName() (string, error) {
	name := m.newSubscriptionName()
	// TODO: persistent the subscription name on meta.
	return name, nil
}

// GetAllExpiredScannerNames get all expired scanner.
// (term < current term scanner)
func (m *scannerRegistry) GetAllExpiredScannerNames() ([]string, error) {
	// TODO: scan all scanner of these channel, drop all expired scanner.
	return nil, nil
}

// newSubscriptionName generates a new subscription name.
func (m *scannerRegistry) newSubscriptionName() string {
	id := m.idAllocator.Allocate()
	return fmt.Sprintf("%s/%d/%d", m.channel.Name, m.channel.Term, id)
}

// scannerCleanupWrapper is a wrapper of scanner to add cleanup function.
type scannerCleanupWrapper struct {
	scanner.Scanner
	cleanup func()
}

// scannerCleanupWrapper overrides Scanner Close function.
func (sw *scannerCleanupWrapper) Close() error {
	err := sw.Scanner.Close()
	sw.cleanup()
	return err
}
