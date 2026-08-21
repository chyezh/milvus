// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"context"
	"path"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// legacyTransformLogChunkPrefix is the object storage prefix of the legacy
// per-vchannel transform log chunks. The pre-summary format persisted one
// TransformLogChunk per flush under
// <root>/transform-log/<pchannel>/<vchannel>/chunks/<chunkID>.pb. The format
// is deprecated; the migration reads it once to build the first summary chunk.
const legacyTransformLogChunkPrefix = "transform-log"

// readLegacyTransformLogChunks reads every chunk of one legacy vchannel in
// chunk id order and returns their entries concatenated. A vchannel with no
// chunks (or with only already-truncated ones) yields no entries.
func readLegacyTransformLogChunks(
	ctx context.Context,
	chunkManager storage.ChunkManager,
	pchannel string,
	vchannel string,
	fromChunkID uint64,
) ([]*streamingpb.TransformLogEntry, error) {
	entries := make([]*streamingpb.TransformLogEntry, 0)
	chunkID := fromChunkID
	for {
		payload, err := chunkManager.Read(ctx, legacyTransformLogChunkPath(chunkManager.RootPath(), pchannel, vchannel, chunkID))
		if err != nil {
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				break
			}
			return nil, merr.Wrap(err, "read legacy transform log chunk")
		}
		chunk := &streamingpb.TransformLogChunk{}
		if err := proto.Unmarshal(payload, chunk); err != nil {
			return nil, merr.Wrap(err, "decode legacy transform log chunk")
		}
		if chunk.GetChunkId() != chunkID {
			return nil, merr.WrapErrDataIntegrityMsg(
				"legacy transform log chunk id mismatch: expected %d, got %d",
				chunkID, chunk.GetChunkId(),
			)
		}
		for _, entry := range chunk.GetEntries() {
			if entry == nil {
				continue
			}
			entries = append(entries, entry)
		}
		chunkID++
	}
	return entries, nil
}

// legacyTransformLogChunkPath returns the object key of one legacy chunk.
func legacyTransformLogChunkPath(rootPath string, pchannel string, vchannel string, chunkID uint64) string {
	return path.Join(
		rootPath,
		legacyTransformLogChunkPrefix,
		pchannel,
		vchannel,
		"chunks",
		strconv.FormatUint(chunkID, 10)+".pb",
	)
}
