package streamingnode

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *catalog) CommitWALRecoveryRoot(ctx context.Context, channel string, term int64, slot, rootID string) error {
	if slot == "" || len(slot) > 256 || rootID == "" || len(rootID) > 256 || term <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid WAL recovery root identity")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	owner, err := c.metaKV.Load(ctx, buildConsumeCheckpointKey(channel))
	if err != nil {
		return err
	}
	checkpoint := &streamingpb.WALCheckpoint{}
	if err := proto.Unmarshal([]byte(owner), checkpoint); err != nil {
		return merr.WrapErrDataIntegrity(err, "decode WAL recovery root owner")
	}
	if checkpoint.Term != term {
		return merr.WrapErrServiceUnavailable("WAL recovery root publisher was fenced")
	}
	value, err := c.loadRetentionValue(ctx, retentionKey(channel, "roots", rootID))
	if err != nil {
		return err
	}
	if value == "" {
		return merr.WrapErrServiceUnavailable("WAL recovery root must be retained before publication")
	}
	root := &streamingpb.WALObjectRetentionRoot{}
	if err := proto.Unmarshal([]byte(value), root); err != nil {
		return merr.WrapErrDataIntegrity(err, "decode WAL recovery root")
	}
	if root.RootId != rootID || root.Released {
		return merr.WrapErrDataIntegrityMsg("WAL recovery root identity disagrees with its slot")
	}
	slotKey := retentionKey(channel, "recovery-slots", slot)
	previous, err := c.loadRetentionValue(ctx, slotKey)
	if err != nil {
		return err
	}
	saves := map[string]string{slotKey: rootID}
	var removes []string
	if previous != "" && previous != rootID {
		saves[retentionKey(channel, "released", previous)] = previous
		removes = append(removes, retentionKey(channel, "roots", previous))
	}
	return c.commitRetention(ctx, channel, epoch, saves, removes, predicates.ValueEqual(buildConsumeCheckpointKey(channel), owner))
}
