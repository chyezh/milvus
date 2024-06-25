package block

import "github.com/milvus-io/milvus/internal/util/streamingutil/message"

// ImmutableBLockList is a continous sequence of ImmutableBlock.
type ImmutableBLockList struct {
	blocks []ImmutableBlock
}

func NewMutableBlockList() *MutableBlockList {
	return &MutableBlockList{
		blocks: make([]ImmutableBlock, 0),
		tail:   NewMutableBlock(),
	}
}

type MutableBlockList struct {
	blocks []ImmutableBlock
	tail   MutableBlock
}

func (mbl *MutableBlockList) Append(msg []message.ImmutableMessage) {
	mbl.tail.Append(msg)
}

// IntoImmutable converts MutableBlockList to ImmutableBLockList.
func (mbl *MutableBlockList) IntoImmutable() *ImmutableBLockList {
	return &ImmutableBLockList{
		blocks: append(mbl.blocks, mbl.tail.Seal()),
	}
}
