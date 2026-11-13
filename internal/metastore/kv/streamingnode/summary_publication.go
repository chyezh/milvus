package streamingnode

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func summaryPublicationKey(channel string) string {
	return retentionPrefix(channel) + "summary-publication"
}

func (c *catalog) LoadWALSummaryPublication(ctx context.Context, channel string) (*streamingpb.WALSummaryPublication, error) {
	value, err := c.loadRetentionValue(ctx, summaryPublicationKey(channel))
	if err != nil || value == "" {
		return nil, err
	}
	publication := &streamingpb.WALSummaryPublication{}
	if err := proto.Unmarshal([]byte(value), publication); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL summary publication")
	}
	if publication.Term <= 0 || publication.ObjectKey == "" || len(publication.RootIds) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid WAL summary publication")
	}
	return publication, nil
}

// Publication serializes the immutable image pointer, its complete roots and
// retirement of obsolete recovery roots. A delayed object PUT cannot move this
// pointer; a delayed publisher loses either the expected predecessor or term.
func (c *catalog) PublishWALSummary(ctx context.Context, channel, previous string, publication *streamingpb.WALSummaryPublication) error {
	if publication.GetTerm() <= 0 || publication.GetObjectKey() == "" || len(publication.GetRootIds()) == 0 || len(publication.RootIds) > 128 {
		return merr.WrapErrServiceInternalMsg("invalid WAL summary publication")
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
		return merr.WrapErrDataIntegrity(err, "decode WAL summary publisher")
	}
	if checkpoint.Term != publication.Term {
		return merr.WrapErrServiceUnavailable("WAL summary publisher was fenced")
	}
	current, err := c.LoadWALSummaryPublication(ctx, channel)
	if err != nil {
		return err
	}
	if current.GetObjectKey() != previous && !proto.Equal(current, publication) {
		return merr.WrapErrServiceUnavailable("WAL summary publication changed concurrently")
	}
	roots, err := c.ListWALObjectRetentionRoots(ctx, channel)
	if err != nil {
		return err
	}
	found := make(map[string]bool, len(publication.RootIds))
	imageRetained := false
	prefix := "summary-recovery/" + strconv.FormatInt(publication.Term, 10) + "/"
	for _, root := range roots {
		if slices.Contains(publication.RootIds, root.RootId) {
			if root.Runtime != nil || !strings.HasPrefix(root.RootId, prefix) {
				return merr.WrapErrDataIntegrityMsg("WAL summary publication has an invalid recovery root")
			}
			found[root.RootId] = true
			imageRetained = imageRetained || slices.Contains(root.ObjectKeys, publication.ObjectKey)
		}
	}
	if len(found) != len(publication.RootIds) || !imageRetained {
		return merr.WrapErrServiceUnavailable("WAL summary image and dependencies must be retained before publication")
	}
	encoded, err := proto.Marshal(publication)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL summary publication")
	}
	saves := map[string]string{summaryPublicationKey(channel): string(encoded)}
	var removes []string
	// A bounded batch also handles abandoned preparations. Remaining roots are
	// harmless and are collected by the next publication (including retries).
	for _, root := range roots {
		if found[root.RootId] || root.Runtime != nil || !strings.HasPrefix(root.RootId, "summary-recovery/") {
			continue
		}
		ownerTerm, _, ok := strings.Cut(strings.TrimPrefix(root.RootId, "summary-recovery/"), "/")
		term, err := strconv.ParseInt(ownerTerm, 10, 64)
		if !ok || err != nil || term > publication.Term {
			continue
		}
		if len(saves)+len(removes)+3 > c.metaKV.MaxTxnOps() {
			break
		}
		saves[retentionKey(channel, "released", root.RootId)] = root.RootId
		removes = append(removes, retentionKey(channel, "roots", root.RootId))
	}
	return c.commitRetention(ctx, channel, epoch, saves, removes, predicates.ValueEqual(buildConsumeCheckpointKey(channel), owner))
}
