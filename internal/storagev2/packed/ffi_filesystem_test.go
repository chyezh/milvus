package packed

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newLocalFFIStorageConfig(t *testing.T) (*indexpb.StorageConfig, string) {
	t.Helper()

	dir := t.TempDir()
	return &indexpb.StorageConfig{
		RootPath:    dir,
		BucketName:  dir,
		StorageType: "local",
	}, dir
}

func TestFFIFileSystemReadFile(t *testing.T) {
	storageConfig, dir := newLocalFFIStorageConfig(t)
	filePath := filepath.Join(dir, "ffi_filesystem", "read.bin")
	data := []byte("0123456789")

	require.NoError(t, WriteFile(storageConfig, filePath, data))

	got, err := ReadFile(storageConfig, filePath, 0, ReadFullFileSize)
	require.NoError(t, err)
	require.Equal(t, data, got)

	got, err = ReadFile(storageConfig, filePath, 2, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("2345"), got)

	got, err = ReadFile(storageConfig, filePath, int64(len(data)), 0)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = ReadFile(storageConfig, filePath, -1, 1)
	require.ErrorIs(t, err, merr.ErrIoInvalidRange)

	_, err = ReadFile(storageConfig, filePath, 0, -2)
	require.ErrorIs(t, err, merr.ErrIoInvalidRange)

	_, err = ReadFile(storageConfig, filePath, 1, ReadFullFileSize)
	require.ErrorIs(t, err, merr.ErrIoInvalidRange)

	_, err = ReadFile(storageConfig, filePath, int64(len(data))+1, 0)
	require.ErrorIs(t, err, merr.ErrIoInvalidRange)

	_, err = ReadFile(storageConfig, filePath, int64(len(data)), 1)
	require.ErrorIs(t, err, merr.ErrIoInvalidRange)
}

func TestFFIFileSystemReadEmptyFile(t *testing.T) {
	storageConfig, dir := newLocalFFIStorageConfig(t)
	filePath := filepath.Join(dir, "ffi_filesystem", "empty.bin")

	require.NoError(t, WriteFile(storageConfig, filePath, nil))

	got, err := ReadFile(storageConfig, filePath, 0, ReadFullFileSize)
	require.NoError(t, err)
	require.Empty(t, got)

	got, err = ReadFile(storageConfig, filePath, 0, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestFFIFileSystemRemoveFile(t *testing.T) {
	storageConfig, dir := newLocalFFIStorageConfig(t)
	filePath := filepath.Join(dir, "ffi_filesystem", "remove.bin")

	require.NoError(t, WriteFile(storageConfig, filePath, []byte("to be removed")))
	require.NoError(t, RemoveFile(storageConfig, filePath))

	_, err := ReadFile(storageConfig, filePath, 0, ReadFullFileSize)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	err = RemoveFile(storageConfig, filePath)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
}

func TestFFIFileSystemMerrSemantics(t *testing.T) {
	storageConfig, dir := newLocalFFIStorageConfig(t)
	missingPath := filepath.Join(dir, "ffi_filesystem", "missing.bin")

	_, err := ReadFile(nil, missingPath, 0, ReadFullFileSize)
	require.ErrorIs(t, err, merr.ErrIoInvalidArgument)

	_, err = ReadFile(storageConfig, missingPath, 0, ReadFullFileSize)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	err = RemoveFile(storageConfig, missingPath)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	err = WriteFile(nil, missingPath, []byte("data"))
	require.ErrorIs(t, err, merr.ErrIoInvalidArgument)

	err = RemoveFile(nil, missingPath)
	require.ErrorIs(t, err, merr.ErrIoInvalidArgument)
}
