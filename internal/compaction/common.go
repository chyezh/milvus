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

package compaction

import (
	"context"
	sio "io"

	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func ComposeDeleteFromDeltalogs(ctx context.Context, io io.BinlogIO, paths []string) (map[interface{}]typeutil.Timestamp, error) {
	pk2Ts := make(map[interface{}]typeutil.Timestamp)

	if len(paths) == 0 {
		mlog.Debug(ctx, "input deltalog paths is empty, skip")
		return pk2Ts, nil
	}

	blobs := make([]*storage.Blob, 0)
	binaries, err := io.Download(ctx, paths)
	if err != nil {
		mlog.Warn(ctx, "compose delete wrong, fail to download deltalogs",
			mlog.Strings("path", paths),
			mlog.Err(err))
		return nil, err
	}

	for i := range binaries {
		blobs = append(blobs, &storage.Blob{Value: binaries[i]})
	}
	reader, err := storage.CreateDeltalogReader(blobs)
	if err != nil {
		mlog.Error(ctx, "compose delete wrong, malformed delta file", mlog.Err(err))
		return nil, err
	}
	defer reader.Close()

	for {
		dl, err := reader.NextValue()
		if err != nil {
			if err == sio.EOF {
				break
			}
			mlog.Error(ctx, "compose delete wrong, failed to read deltalogs", mlog.Err(err))
			return nil, err
		}

		if ts, ok := pk2Ts[(*dl).Pk.GetValue()]; ok && ts > (*dl).Ts {
			continue
		}
		pk2Ts[(*dl).Pk.GetValue()] = (*dl).Ts
	}

	mlog.Info(ctx, "compose delete end", mlog.Int("delete entries counts", len(pk2Ts)))
	return pk2Ts, nil
}
