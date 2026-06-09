package growing

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type transformLogChunkWriter interface {
	WriteTransformLogChunk(ctx context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error
}

type flushTransformLogBufferTask struct {
	vchannel     *vChannelView
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *flushTransformLogBufferTask) Name() string {
	return "growing-flush-transform-log-buffer"
}

func (t *flushTransformLogBufferTask) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *flushTransformLogBufferTask) Done() bool {
	return t.done.Load()
}

func (t *flushTransformLogBufferTask) Run(ctx context.Context) error {
	err := t.run(ctx)
	if err == nil {
		t.done.Store(true)
	}
	return err
}

func (t *flushTransformLogBufferTask) run(ctx context.Context) error {
	vchannel := t.vchannel
	var nextTask scheduler.Task
	runtime := vchannel.runtime
	for {
		var targetTimeTick uint64
		var chunk *streamingpb.TransformLogChunk
		var nextTargetTimeTick uint64
		var liveEntries []*streamingpb.TransformLogEntry
		vchannel.mu.Lock()
		targetTimeTick = vchannel.transformLogBuffer.FlushTargetTimeTick()
		if targetTimeTick > vchannel.meta.GetDataCheckpointTimeTick() {
			transformMeta := ensureTransformLogMeta(vchannel.meta)
			chunk = vchannel.transformLogBuffer.FlushChunk(transformMeta.GetNextChunkId(), targetTimeTick)
		}
		vchannel.mu.Unlock()

		if chunk != nil {
			if vchannel.transformLogChunkWriter == nil {
				return errors.New("transform log chunk writer is nil")
			}
			if err := vchannel.transformLogChunkWriter.WriteTransformLogChunk(ctx, vchannel.Name(), chunk); err != nil {
				return err
			}
		}

		vchannel.mu.Lock()
		if chunk != nil {
			toTimeTick := chunk.GetEntries()[len(chunk.GetEntries())-1].GetTimeTick()
			vchannel.transformLogBuffer.DiscardThrough(toTimeTick)
			vchannel.retainedTransformLogChunks = append(vchannel.retainedTransformLogChunks, chunk)
			liveEntries = chunk.GetEntries()
			transformMeta := ensureTransformLogMeta(vchannel.meta)
			if toTimeTick > transformMeta.GetCheckpointTimeTick() {
				transformMeta.CheckpointTimeTick = toTimeTick
			}
			if chunk.GetChunkId() >= transformMeta.GetNextChunkId() {
				transformMeta.NextChunkId = chunk.GetChunkId() + 1
			}
			durableTimeTick := toTimeTick
			if !vchannel.transformLogBuffer.HasFlushWorkThrough(targetTimeTick) {
				durableTimeTick = targetTimeTick
			}
			vchannel.MarkDeleteDataDurable(durableTimeTick)
		} else if targetTimeTick > vchannel.meta.GetDataCheckpointTimeTick() {
			vchannel.MarkDeleteDataDurable(targetTimeTick)
		}
		currentFlushTarget := vchannel.transformLogBuffer.FlushTargetTimeTick()
		vchannel.transformLogBuffer.FinishFlush()
		switch {
		case currentFlushTarget > vchannel.meta.GetDataCheckpointTimeTick():
			nextTargetTimeTick = currentFlushTarget
		case vchannel.transformLogBuffer.HasFlushWorkThrough(currentFlushTarget):
			nextTargetTimeTick = currentFlushTarget
		case vchannel.transformLogBuffer.ShouldFlush():
			nextTargetTimeTick = vchannel.transformLogBuffer.DataTimeTick()
		}
		if nextTargetTimeTick > 0 {
			nextTask = vchannel.StartFlushTransformLogBufferTaskLocked(nextTargetTimeTick)
		}
		vchannel.mu.Unlock()
		if len(liveEntries) > 0 {
			vchannel.publishTransformLogEntries(liveEntries)
		}
		vchannel.NotifyDataUpdated()
		break
	}
	if nextTask != nil {
		runtime.Scheduler.Submit(nextTask)
	}
	return nil
}

func ensureTransformLogMeta(meta *streamingpb.VChannelMeta) *streamingpb.VChannelTransformLogMeta {
	if meta.TransformLogMeta == nil {
		meta.TransformLogMeta = &streamingpb.VChannelTransformLogMeta{}
	}
	return meta.TransformLogMeta
}
