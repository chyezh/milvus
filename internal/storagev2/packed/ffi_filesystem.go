// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

/*
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_filesystem_c.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadFullFileSize tells ReadFile to read the whole file from offset 0.
const ReadFullFileSize int64 = -1

const maxReadAllocSize = int64(int(^uint(0) >> 1))

// WriteFile writes raw bytes to a file using milvus-storage filesystem FFI.
// filePath is the full storage path (rootPath/basePath/...).
func WriteFile(
	storageConfig *indexpb.StorageConfig,
	filePath string,
	data []byte,
) error {
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return merr.WrapErrIoInvalidArgument(filePath, err)
	}
	defer C.loon_properties_free(cProperties)

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(filePath))

	// Get filesystem handle (LRU-cached by C++ side)
	var fsHandle C.FileSystemHandle
	result := C.loon_filesystem_get(cProperties, cPath, pathLen, &fsHandle)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoFailed); err != nil {
		return fmt.Errorf("failed to get filesystem: %w", err)
	}
	defer C.loon_filesystem_destroy(fsHandle)

	// Local filesystem requires parent directories to exist before writing.
	// Object stores (S3/MinIO/GCS) don't have real directories.
	if storageConfig.GetStorageType() == "local" {
		if idx := strings.LastIndex(filePath, "/"); idx > 0 {
			dir := filePath[:idx]
			cDir := C.CString(dir)
			defer C.free(unsafe.Pointer(cDir))
			result = C.loon_filesystem_create_dir(fsHandle, cDir, C.uint32_t(len(dir)), true)
			if err := handleLoonFilesystemResult(result, dir, merr.WrapErrIoFailed); err != nil {
				return fmt.Errorf("failed to create parent directory %q: %w", dir, err)
			}
		}
	}

	// Write file atomically
	var dataPtr *C.uint8_t
	if len(data) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}

	result = C.loon_filesystem_write_file(
		fsHandle,
		cPath, pathLen,
		dataPtr, C.uint64_t(len(data)),
		nil, 0, // no file metadata
	)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoFailed); err != nil {
		return fmt.Errorf("failed to write file %q: %w", filePath, err)
	}
	return nil
}

// ReadFile reads raw bytes from a file using milvus-storage filesystem FFI.
//
// The filePath argument follows WriteFile semantics: it is the full storage
// path (rootPath/basePath/...). Use size == ReadFullFileSize with offset 0 for
// a full-file read. Use size >= 0 for range reads. A zero-size range validates
// that the file exists and that offset is not past EOF, then returns an empty
// slice without asking the FFI layer to read bytes because the C API rejects
// nbytes == 0.
func ReadFile(
	storageConfig *indexpb.StorageConfig,
	filePath string,
	offset int64,
	size int64,
) ([]byte, error) {
	if offset < 0 {
		return nil, merr.WrapErrIoInvalidRange(filePath, fmt.Errorf("offset %d must be non-negative", offset))
	}
	if size < ReadFullFileSize {
		return nil, merr.WrapErrIoInvalidRange(filePath, fmt.Errorf("size %d must be non-negative or ReadFullFileSize", size))
	}
	if size == ReadFullFileSize && offset != 0 {
		return nil, merr.WrapErrIoInvalidRange(filePath, fmt.Errorf("full read offset must be 0, got %d", offset))
	}
	if size > maxReadAllocSize {
		return nil, merr.WrapErrIoEntityTooLarge(filePath, fmt.Errorf("read range is too large to allocate: %d", size))
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, merr.WrapErrIoInvalidArgument(filePath, err)
	}
	defer C.loon_properties_free(cProperties)

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(filePath))

	fsHandle, err := getFileSystemHandle(cProperties, cPath, pathLen, filePath)
	if err != nil {
		return nil, err
	}
	defer C.loon_filesystem_destroy(fsHandle)

	if size == ReadFullFileSize {
		fileSize, err := getFileSize(fsHandle, cPath, pathLen, filePath)
		if err != nil {
			return nil, err
		}
		if fileSize == 0 {
			return []byte{}, nil
		}
		if fileSize > uint64(maxReadAllocSize) {
			return nil, merr.WrapErrIoEntityTooLarge(filePath, fmt.Errorf("file is too large to read into memory: %d", fileSize))
		}
		size = int64(fileSize)
	}

	if size == 0 {
		fileSize, err := getFileSize(fsHandle, cPath, pathLen, filePath)
		if err != nil {
			return nil, err
		}
		if uint64(offset) > fileSize {
			return nil, merr.WrapErrIoInvalidRange(filePath, fmt.Errorf("offset %d is past file size %d", offset, fileSize))
		}
		return []byte{}, nil
	}

	data := make([]byte, int(size))
	result := C.loon_filesystem_read_file(
		fsHandle,
		cPath, pathLen,
		C.uint64_t(offset), C.uint64_t(size),
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
	)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoInvalidRange); err != nil {
		return nil, fmt.Errorf("failed to read file %q at offset %d with size %d: %w", filePath, offset, size, err)
	}
	return data, nil
}

// RemoveFile removes a file using milvus-storage filesystem FFI.
// filePath is the full storage path (rootPath/basePath/...).
func RemoveFile(
	storageConfig *indexpb.StorageConfig,
	filePath string,
) error {
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return merr.WrapErrIoInvalidArgument(filePath, err)
	}
	defer C.loon_properties_free(cProperties)

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	pathLen := C.uint32_t(len(filePath))

	fsHandle, err := getFileSystemHandle(cProperties, cPath, pathLen, filePath)
	if err != nil {
		return err
	}
	defer C.loon_filesystem_destroy(fsHandle)

	result := C.loon_filesystem_delete_file(fsHandle, cPath, pathLen)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoInvalidArgument); err != nil {
		return fmt.Errorf("failed to remove file %q: %w", filePath, err)
	}
	return nil
}

func getFileSystemHandle(
	cProperties *C.LoonProperties,
	cPath *C.char,
	pathLen C.uint32_t,
	filePath string,
) (C.FileSystemHandle, error) {
	var fsHandle C.FileSystemHandle
	result := C.loon_filesystem_get(cProperties, cPath, pathLen, &fsHandle)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoFailed); err != nil {
		return 0, fmt.Errorf("failed to get filesystem: %w", err)
	}
	return fsHandle, nil
}

func getFileSize(
	fsHandle C.FileSystemHandle,
	cPath *C.char,
	pathLen C.uint32_t,
	filePath string,
) (uint64, error) {
	var fileSize C.uint64_t
	result := C.loon_filesystem_get_file_info(fsHandle, cPath, pathLen, &fileSize)
	if err := handleLoonFilesystemResult(result, filePath, merr.WrapErrIoFailed); err != nil {
		return 0, fmt.Errorf("failed to get file info for %q: %w", filePath, err)
	}
	return uint64(fileSize), nil
}

func handleLoonFilesystemResult(
	ffiResult C.LoonFFIResult,
	filePath string,
	wrapLogicalError func(string, error) error,
) error {
	defer C.loon_ffi_free_result(&ffiResult)
	if C.loon_ffi_is_success(&ffiResult) != 0 {
		return nil
	}

	errMsg := C.loon_ffi_get_errmsg(&ffiResult)
	errStr := "Unknown error"
	if errMsg != nil {
		errStr = C.GoString(errMsg)
	}
	err := errors.New(errStr)

	switch ffiResult.err_code {
	case C.LOON_FILE_NOT_FOUND:
		return merr.WrapErrIoKeyNotFound(filePath, errStr)
	case C.LOON_INVALID_ARGS:
		return merr.WrapErrIoInvalidArgument(filePath, err)
	case C.LOON_LOGICAL_ERROR:
		return wrapLogicalError(filePath, err)
	case C.LOON_MEMORY_ERROR:
		return merr.WrapErrIoEntityTooLarge(filePath, err)
	default:
		return merr.WrapErrIoFailed(filePath, err)
	}
}
