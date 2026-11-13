package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"context"
	"io/fs"
	"slices"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetManifestObjectKeysAsync enumerates the exact immutable dependencies of a
// pinned revision, including indirect milvus-table sources. It never lists a
// segment directory: unrelated revisions and unpublished files are not roots.
// Keys retain the backend spelling supplied by storage. The caller must acquire
// deletion protection for the entire result before publishing the revision to
// another runtime; reading a manifest alone does not establish retention.
func GetManifestObjectKeysAsync(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig, maxKeys int) ([]string, error) {
	if maxKeys <= 0 || maxKeys > 100000 {
		return nil, merr.WrapErrServiceInternalMsg("invalid manifest dependency budget")
	}
	keys := make(map[string]struct{})
	bytes := 0
	add := func(key string) error {
		if key == "" {
			return merr.WrapErrDataIntegrityMsg("manifest dependency has an empty object key")
		}
		if _, exists := keys[key]; exists {
			return nil
		}
		if len(keys) >= maxKeys || bytes+len(key) > 64<<20 {
			return merr.WrapErrServiceUnavailable("manifest dependency budget exhausted")
		}
		keys[key] = struct{}{}
		bytes += len(key)
		return nil
	}
	visited := make(map[string]struct{})
	pending := []string{manifestPath}
	for len(pending) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		physical, err := ManifestFilePath(current)
		if err != nil {
			return nil, err
		}
		if _, seen := visited[physical]; seen {
			continue
		}
		visited[physical] = struct{}{}
		if err := add(physical); err != nil {
			return nil, err
		}
		nested, err := readManifestObjectKeys(ctx, io, current, config, maxKeys, add)
		if err != nil {
			return nil, err
		}
		if len(pending)+len(nested) > maxKeys {
			return nil, merr.WrapErrServiceUnavailable("indirect manifest dependency budget exhausted")
		}
		pending = append(pending, nested...)
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	slices.Sort(result)
	return result, nil
}

func readManifestObjectKeys(ctx context.Context, io *ManifestIOContext, manifestPath string, config *indexpb.StorageConfig, maxKeys int, add func(string) error) ([]string, error) {
	manifest, err := getManifestAsync(ctx, io, manifestPath, config)
	if err != nil {
		return nil, err
	}
	defer C.loon_manifest_destroy(manifest)
	groups := &manifest.column_groups
	if groups.num_of_column_groups > 0 && groups.column_group_array == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing column groups")
	}
	var nested []string
	for _, group := range unsafe.Slice(groups.column_group_array, int(groups.num_of_column_groups)) {
		if group.num_of_files > 0 && group.files == nil {
			return nil, merr.WrapErrDataIntegrityMsg("manifest has missing column group files")
		}
		for _, file := range unsafe.Slice(group.files, int(group.num_of_files)) {
			if source := columnGroupFileProperty(&file, milvusTableSourceManifestPathProperty); source != "" {
				if len(nested) == maxKeys {
					return nil, merr.WrapErrServiceUnavailable("indirect manifest dependency budget exhausted")
				}
				nested = append(nested, source)
				continue
			}
			if err := add(C.GoString(file.path)); err != nil {
				return nil, err
			}
		}
	}
	deltas, err := deltaLogsFromManifest(manifestPath, manifest)
	if err != nil {
		return nil, err
	}
	for _, field := range deltas {
		for _, log := range field.Binlogs {
			if err := add(log.LogPath); err != nil {
				return nil, err
			}
		}
	}
	stats, err := manifestStats(manifest, manifestPath)
	if err != nil {
		return nil, err
	}
	for _, stat := range stats {
		for _, key := range stat.Paths {
			if err := add(key); err != nil {
				return nil, err
			}
		}
	}
	indexes, err := manifestIndexInfos(manifest, manifestPath)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		for _, key := range index.IndexFileKeys {
			if key == "." || !fs.ValidPath(key) {
				return nil, merr.WrapErrDataIntegrityMsg("manifest index has an invalid relative file key")
			}
			if err := add(strings.TrimRight(index.Path, "/") + "/" + key); err != nil {
				return nil, err
			}
		}
	}
	if manifest.lob_files.num_files > 0 && manifest.lob_files.files == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest has missing LOB files")
	}
	for _, lob := range manifestLobFiles(manifest) {
		if err := add(lob.Path); err != nil {
			return nil, err
		}
	}
	return nested, nil
}
