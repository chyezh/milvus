package msgdispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestSendTimeout(t *testing.T) {
	target := newTarget(&StreamConfig{
		VChannel: "test1",
		Pos:      &msgpb.MsgPosition{},
	}, false)

	time.Sleep(paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second))

	counter := 0
	for i := 0; i < 10; i++ {
		err := target.send(&msgstream.MsgPack{})
		if err != nil {
			log.Error(context.TODO(), "send failed", log.Int("idx", i), log.Err(err))
			counter++
		}
	}
	assert.Equal(t, counter, 0)
}

func TestSendTimeTickFiltering(t *testing.T) {
	target := newTarget(&StreamConfig{
		VChannel: "test1",
		Pos:      &msgpb.MsgPosition{},
	}, true)
	target.send(&msgstream.MsgPack{
		EndPositions: []*msgpb.MsgPosition{
			{
				Timestamp: 1,
			},
		},
	})

	target.send(&msgstream.MsgPack{
		EndPositions: []*msgpb.MsgPosition{
			{
				Timestamp: 1,
			},
		},
	})
}
