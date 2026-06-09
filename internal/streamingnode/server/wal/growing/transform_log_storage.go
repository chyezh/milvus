package growing

import (
	"context"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type transformLogChunkStore interface {
	transformLogChunkWriter
	ReadTransformLogChunk(ctx context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error)
}

type objectTransformLogChunkStore struct {
	chunkManager storage.ChunkManager
	pchannel     string
}

func NewObjectTransformLogChunkStore(chunkManager storage.ChunkManager, pchannel string) transformLogChunkStore {
	return &objectTransformLogChunkStore{
		chunkManager: chunkManager,
		pchannel:     pchannel,
	}
}

func (s *objectTransformLogChunkStore) WriteTransformLogChunk(ctx context.Context, vchannel string, chunk *streamingpb.TransformLogChunk) error {
	bytes, err := proto.Marshal(chunk)
	if err != nil {
		return err
	}
	return s.chunkManager.Write(ctx, s.chunkPath(vchannel, chunk.GetChunkId()), bytes)
}

func (s *objectTransformLogChunkStore) ReadTransformLogChunk(ctx context.Context, vchannel string, chunkID uint64) (*streamingpb.TransformLogChunk, error) {
	bytes, err := s.chunkManager.Read(ctx, s.chunkPath(vchannel, chunkID))
	if err != nil {
		return nil, err
	}
	chunk := &streamingpb.TransformLogChunk{}
	if err := proto.Unmarshal(bytes, chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

func (s *objectTransformLogChunkStore) chunkPath(vchannel string, chunkID uint64) string {
	return path.Join(
		s.chunkManager.RootPath(),
		"transform-log",
		s.pchannel,
		vchannel,
		"chunks",
		strconv.FormatUint(chunkID, 10)+".pb",
	)
}
