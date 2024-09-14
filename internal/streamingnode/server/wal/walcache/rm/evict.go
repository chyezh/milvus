package rm

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// blockOperator is the block operator with evicted flag.
type blockOperator struct {
	evicted  bool
	operator walcache.BlockOperator
}

// pchannelInfos is the pchannelInfos with term and operators.
type pchannelInfos struct {
	term      int
	operators []*blockOperator
}

// evictOrder is the evict order.
type evictOrder struct {
	evictOrder []*blockOperator
	pchannels  map[types.PChannelInfo][]*blockOperator
	evictor    func(walcache.BlockOperator)
}

func (eo *evictOrder) Push(wbo walcache.BlockOperator) {
	bo := &blockOperator{
		evicted:  false,
		operator: wbo,
	}
	eo.evictOrder = append(eo.evictOrder, bo)
	if eo.pchannels[bo.operator.PChannel()] == nil {
		eo.pchannels[bo.operator.PChannel()] = make([]*blockOperator, 5)
	}
	eo.pchannels[bo.operator.PChannel()] = append(eo.pchannels[bo.operator.PChannel()], bo)
}

// EvictByPChannel evicts all blocks in the pchannel.
func (eo *evictOrder) GetAndRemoveByPChannelInfo(p types.PChannelInfo) []*blockOperator {
	bos := eo.pchannels[p]
	delete(eo.pchannels, p)
	return bos
}

// Peek returns the block operator that will be evicted next.
func (eo *evictOrder) Peek() *blockOperator {
	eo.popUntilNotEvicted()
	if len(eo.evictOrder) == 0 {
		return nil
	}
	return eo.evictOrder[0]
}

// PopAndEvict pops the evicted block.
func (eo *evictOrder) PopAndEvict() {
	eo.popUntilNotEvicted()
	if len(eo.evictOrder) > 0 {
		eo.evictOrder[0].evicted = true
		eo.evictOrder = eo.evictOrder[1:]
	}
}

func (eo *evictOrder) popUntilNotEvicted() {
	if len(eo.evictOrder) == 0 {
		return
	}
	for len(eo.evictOrder) > 0 && eo.evictOrder[0].evicted {
		eo.evictOrder[0] = nil
		eo.evictOrder = eo.evictOrder[1:]
	}
}
