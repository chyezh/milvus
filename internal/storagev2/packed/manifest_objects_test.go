package packed

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestObjectKeysRetainEverySectionOfExactRevision(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(2)
	defer io.Close()
	base := path.Join(cfg.RootPath, "objects/segment")
	data := path.Join(base, "_data/100.parquet")
	delta := path.Join(base, "_delta/1")
	stat := path.Join(base, "_stats/bm25.100/1")
	lob := path.Join(cfg.RootPath, "lob/partition/shared")
	manifest, err := CommitManifestUpdates(MarshalManifestPath(base, ManifestEarliest), cfg, &ManifestUpdates{
		ColumnGroups: []ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []ColumnGroupFileEntry{{Path: data, EndIndex: 5}}}},
		DeltaLogs:    []DeltaLogEntry{{Path: delta, NumEntries: 1}},
		Stats:        []StatEntry{{Key: "bm25.100", Files: []string{stat}}},
		Indexes:      []ManifestIndexInfo{{ColumnName: "100", IndexName: "index", IndexType: "FLAT", Path: "2/1", FieldID: 100, IndexID: 1, BuildID: 2, IndexFileKeys: []string{"index", "meta"}}},
	})
	require.NoError(t, err)
	manifest, err = AddLobFilesToTransaction(manifest, cfg, []LobFileInfo{{Path: lob, FieldID: 100, TotalRows: 5, ValidRows: 5, FileSizeBytes: 10}})
	require.NoError(t, err)
	physical, err := ManifestFilePath(manifest)
	require.NoError(t, err)
	want := []string{physical, data, delta, stat, lob, path.Join(base, "_index/2/1/index"), path.Join(base, "_index/2/1/meta")}
	for _, file := range want[1:] {
		require.NoError(t, WriteFile(cfg, file, []byte(file)))
	}
	// A later manifest changes statistics. Capturing the old revision must not
	// silently follow the latest pointer or pin unrelated directory contents.
	newStat := path.Join(base, "_stats/bm25.100/2")
	_, err = CommitManifestUpdates(manifest, cfg, &ManifestUpdates{Stats: []StatEntry{{Key: "bm25.100", Files: []string{newStat}}}})
	require.NoError(t, err)
	keys, err := GetManifestObjectKeysAsync(t.Context(), io, manifest, cfg, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, want, keys)
	for _, key := range keys {
		_, err := ReadFile(cfg, key)
		require.NoError(t, err)
	}
	_, err = GetManifestObjectKeysAsync(t.Context(), io, manifest, cfg, len(want)-1)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable, "budget cannot silently truncate dependencies")
	for _, budget := range []int{0, 100001} {
		_, err := GetManifestObjectKeysAsync(t.Context(), io, manifest, cfg, budget)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = GetManifestObjectKeysAsync(ctx, io, manifest, cfg, 100)
	require.ErrorIs(t, err, context.Canceled)
	_, err = GetManifestObjectKeysAsync(t.Context(), io, "malformed", cfg, 100)
	require.Error(t, err)
	io.Close()
	_, err = GetManifestObjectKeysAsync(t.Context(), io, manifest, cfg, 100)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestManifestObjectKeysIncludeIndirectSources(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	io := NewManifestIOContext(2)
	defer io.Close()
	source := createBaseManifest(t, path.Join(cfg.RootPath, "objects/source"), cfg)
	target, err := CreateMilvusTableManifestFromSegmentManifests(path.Join(cfg.RootPath, "objects/target"), []string{"pk", "ts"},
		[]Fragment{{FilePath: source, RowCount: 5, EndRow: 5}}, cfg, ExternalSpecContext{MilvusTablePKMode: MilvusTablePrimaryKeyModeVirtual})
	require.NoError(t, err)
	sourceKeys, err := GetManifestObjectKeysAsync(t.Context(), io, source, cfg, 100)
	require.NoError(t, err)
	targetKeys, err := GetManifestObjectKeysAsync(t.Context(), io, target, cfg, 100)
	require.NoError(t, err)
	for _, key := range sourceKeys {
		require.Contains(t, targetKeys, key)
	}
	physical, err := ManifestFilePath(target)
	require.NoError(t, err)
	require.Contains(t, targetKeys, physical)
}
