package walsummary

import (
	"context"
	"io/fs"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Images are retained before PUT, so the catalog excludes every current or
// in-flight publication. Only exact unreferenced immutable keys are deleted.
func (s *Store) sweepManifestImages(ctx context.Context, budget int) (int, bool, error) {
	prefix := buildManifestPrefix(s.chunkManager, s.pchannel)
	deleted, finished := 0, true
	var deleteErr error
	err := s.chunkManager.WalkWithPrefix(ctx, prefix, false, func(info *storage.ChunkObjectInfo) bool {
		owner, _, ok := strings.Cut(strings.TrimPrefix(info.FilePath, prefix), "_")
		term, err := strconv.ParseInt(owner, 10, 64)
		if !ok || err != nil || term > s.term {
			return true
		}
		removed, err := s.deleteObject(ctx, info.FilePath)
		if err != nil {
			deleteErr = err
			return false
		}
		if removed {
			deleted++
		}
		if deleted >= budget {
			finished = false
			return false
		}
		return true
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return deleted, false, merr.Wrap(err, "failed to sweep summary images")
	}
	return deleted, finished && deleteErr == nil, deleteErr
}
