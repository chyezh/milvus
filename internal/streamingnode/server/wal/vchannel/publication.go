package vchannel

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ApplySegmentPublication resolves the existing observed segment. A completion
// is not a CreateSegment event and must never resurrect a retired module.
func (m *PChannelRecoveryManager) ApplySegmentPublication(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	module, ok := m.modules.Get(meta.GetVchannel())
	if !ok {
		return merr.WrapErrServiceInternalMsg("published segment %d has no local vchannel %s", meta.GetSegmentId(), meta.GetVchannel())
	}
	module.mu.Lock()
	view := module.segments[meta.GetSegmentId()]
	module.mu.Unlock()
	if view == nil {
		return merr.WrapErrServiceInternalMsg("published segment %d was not observed locally", meta.GetSegmentId())
	}
	return view.ApplyStoragePublished(ctx, meta)
}

func (m *PChannelRecoveryManager) ApplyVChannelPublication(meta *streamingpb.VChannelMeta) error {
	module, ok := m.modules.Get(meta.GetVchannel())
	if !ok {
		return merr.WrapErrServiceInternalMsg("published materialization has no local vchannel %s", meta.GetVchannel())
	}
	module.markL0Materialized(meta.GetTransformMaterializedTimeTick())
	return nil
}
