package recovery

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// StartScannerTask starts the background scanner that tails WAL messages into recovery storage.
func (r *recoveryStorageImpl) StartScannerTask(param ScannerTaskParam) {
	r.mu.Lock()
	if r.scannerTaskNotifier != nil {
		r.mu.Unlock()
		return
	}
	r.scannerTaskNotifier = syncutil.NewAsyncTaskNotifier[struct{}]()
	r.scannerTaskMetrics = newScannerTaskMetrics(r.channel)
	r.emptyTimeTickCounter = metrics.WALFlusherEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), r.channel.Name)
	notifier := r.scannerTaskNotifier
	scannerTaskMetrics := r.scannerTaskMetrics
	r.mu.Unlock()

	go r.scannerTask(notifier, scannerTaskMetrics, param)
}

func (r *recoveryStorageImpl) scannerTask(
	notifier *syncutil.AsyncTaskNotifier[struct{}],
	scannerTaskMetrics *scannerTaskMetrics,
	param ScannerTaskParam,
) (err error) {
	logger := resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		zap.String("subtask", "wal-scanner"),
		zap.String("pchannel", r.channel.String()))
	defer func() {
		notifier.Finish(struct{}{})
		if err == nil {
			logger.Info("recovery storage wal scanner task stop")
			return
		}
		if !errors.Is(err, context.Canceled) {
			logger.DPanic("recovery storage wal scanner task stopped with unexpected error", zap.Error(err))
			return
		}
		logger.Warn("recovery storage wal scanner task is canceled before executing", zap.Error(err))
	}()

	if param.StartingRateLimit != nil {
		// Protect WAL append while the recovery-storage scanner is starting.
		param.StartingRateLimit.EnterSlowdownMode(nil)
	}

	logger.Info("recovery storage wal scanner task start")
	l, err := param.WAL.GetWithContext(notifier.Context())
	if err != nil {
		return errors.Wrap(err, "when get wal from future")
	}

	var checkpoint message.MessageID
	if param.RecoverySnapshot != nil && param.RecoverySnapshot.Checkpoint != nil {
		checkpoint = param.RecoverySnapshot.Checkpoint.MetaCheckpoint.MessageID
	}
	scanner, err := r.generateScanner(notifier.Context(), l, checkpoint, param)
	if err != nil {
		return errors.Wrap(err, "when generate scanner")
	}
	defer scanner.Close()

	scannerTaskMetrics.IntoState(scannerTaskStateWorking)
	defer scannerTaskMetrics.IntoState(scannerTaskStateClosing)
	if param.StartingRateLimit != nil {
		param.StartingRateLimit.EnterRecoveryMode()
	}

	for {
		select {
		case <-notifier.Context().Done():
			return nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				logger.Warn("recovery storage wal scanner task is closing for closed scanner channel")
				return nil
			}
			scannerTaskMetrics.ObserveMetrics(msg.TimeTick())
			if err := r.dispatchScannedMessage(notifier.Context(), msg); err != nil {
				return err
			}
		}
	}
}

func (r *recoveryStorageImpl) closeScannerTask() {
	r.mu.Lock()
	notifier := r.scannerTaskNotifier
	scannerTaskMetrics := r.scannerTaskMetrics
	r.mu.Unlock()

	if notifier == nil {
		return
	}
	notifier.Cancel()
	notifier.BlockUntilFinish()
	if scannerTaskMetrics != nil {
		scannerTaskMetrics.Close()
	}
	r.mu.Lock()
	if r.scannerTaskNotifier == notifier {
		r.scannerTaskNotifier = nil
		r.scannerTaskMetrics = nil
	}
	r.mu.Unlock()
}

func (r *recoveryStorageImpl) generateScanner(ctx context.Context, l wal.WAL, checkpoint message.MessageID, param ScannerTaskParam) (wal.Scanner, error) {
	handler := make(adaptor.ChanMessageHandler, 64)
	readOpt := wal.ReadOption{
		VChannel:       "", // Consume all messages from the pchannel.
		MesasgeHandler: handler,
		DeliverPolicy:  options.DeliverPolicyAll(),
	}
	if param.ScannerRateLimit != nil {
		readOpt.RateLimitControl = param.ScannerRateLimit
	}
	if checkpoint != nil {
		r.Logger().Info("recovery storage wal scanner task scans from checkpoint", zap.Stringer("checkpointMessageID", checkpoint))
		readOpt.DeliverPolicy = options.DeliverPolicyStartFrom(checkpoint)
	} else {
		r.Logger().Info("recovery storage wal scanner task scans from the earliest checkpoint")
	}
	return l.Read(ctx, readOpt)
}

func (r *recoveryStorageImpl) dispatchScannedMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if msg.MessageType() == message.MessageTypeTimeTick && !msg.IsPersisted() {
		timetick := msg.TimeTick()
		threshold := paramtable.Get().StreamingCfg.FlushEmptyTimeTickMaxFilterInterval.GetAsDurationByParse()
		if tsoutil.CalculateDuration(timetick, r.lastDispatchTimeTick) < threshold.Milliseconds() {
			r.emptyTimeTickCounter.Inc()
			return nil
		}
	}
	timetick := msg.TimeTick()
	defer func() {
		r.lastDispatchTimeTick = timetick
	}()

	if err := r.ObserveMessage(ctx, msg); err != nil {
		r.Logger().Warn("failed to observe message", zap.Error(err))
		return err
	}
	return nil
}
