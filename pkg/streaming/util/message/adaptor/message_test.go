package adaptor

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestNewMsgPackFromInsertMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	fieldCount := map[int64]int{
		3: 1000,
		4: 2000,
		5: 3000,
		6: 5000,
	}
	tt := uint64(time.Now().UnixNano())
	immutableMessages := make([]message.ImmutableMessage, 0, len(fieldCount))
	for segmentID, rowNum := range fieldCount {
		insertMsg := message.CreateTestInsertMessage(t, segmentID, rowNum, tt, id)
		immutableMessage := insertMsg.WithOldVersion().IntoImmutableMessage(id)
		immutableMessages = append(immutableMessages, immutableMessage)
	}

	pack, err := NewMsgPackFromMessage(immutableMessages...)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
	assert.Len(t, pack.Msgs, len(fieldCount))

	for _, msg := range pack.Msgs {
		insertMsg := msg.(*msgstream.InsertMsg)
		rowNum, ok := fieldCount[insertMsg.GetSegmentID()]
		assert.True(t, ok)

		assert.Len(t, insertMsg.Timestamps, rowNum)
		assert.Len(t, insertMsg.RowIDs, rowNum)
		assert.Len(t, insertMsg.FieldsData, 2)
		for _, fieldData := range insertMsg.FieldsData {
			if data := fieldData.GetScalars().GetBoolData(); data != nil {
				assert.Len(t, data.Data, rowNum)
			} else if data := fieldData.GetScalars().GetIntData(); data != nil {
				assert.Len(t, data.Data, rowNum)
			}
		}

		for _, ts := range insertMsg.Timestamps {
			assert.Equal(t, ts, tt)
		}
	}
}

func TestNewMsgPackFromInsertMessageSplitsPartitionsByHeaderRows(t *testing.T) {
	id := rmq.NewRmqID(1)
	tt := uint64(time.Now().UnixNano())

	mutableMsg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 100,
			Partitions: []*message.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        2,
					SegmentAssignment: &message.SegmentAssignment{
						SegmentId: 1000,
					},
				},
				{
					PartitionId: 20,
					Rows:        3,
					SegmentAssignment: &message.SegmentAssignment{
						SegmentId: 2000,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Insert,
			},
			CollectionID: 100,
			NumRows:      5,
			RowIDs:       []int64{1, 2, 3, 4, 5},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "pk",
					FieldId:   1,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
							},
						},
					},
				},
			},
		}).
		MustBuildMutable().
		WithTimeTick(tt).
		WithLastConfirmedUseMessageID()

	pack, err := NewMsgPackFromMessage(mutableMsg.IntoImmutableMessage(id))
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Len(t, pack.Msgs, 2)

	first := pack.Msgs[0].(*msgstream.InsertMsg)
	assert.Equal(t, int64(10), first.GetPartitionID())
	assert.Equal(t, int64(1000), first.GetSegmentID())
	assert.Equal(t, uint64(2), first.GetNumRows())
	assert.Equal(t, []int64{1, 2}, first.GetRowIDs())
	assert.Equal(t, []int64{1, 2}, first.GetFieldsData()[0].GetScalars().GetLongData().GetData())

	second := pack.Msgs[1].(*msgstream.InsertMsg)
	assert.Equal(t, int64(20), second.GetPartitionID())
	assert.Equal(t, int64(2000), second.GetSegmentID())
	assert.Equal(t, uint64(3), second.GetNumRows())
	assert.Equal(t, []int64{3, 4, 5}, second.GetRowIDs())
	assert.Equal(t, []int64{3, 4, 5}, second.GetFieldsData()[0].GetScalars().GetLongData().GetData())

	for _, insertMsg := range []*msgstream.InsertMsg{first, second} {
		assert.Equal(t, "v1", insertMsg.ShardName)
		assert.Equal(t, tt, insertMsg.Base.Timestamp)
		assert.Len(t, insertMsg.Timestamps, int(insertMsg.GetNumRows()))
		for _, ts := range insertMsg.Timestamps {
			assert.Equal(t, tt, ts)
		}
	}
}

func TestNewMsgPackFromCreateCollectionMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	msg := message.CreateTestCreateCollectionMessage(t, 1, tt, id)
	immutableMessage := msg.IntoImmutableMessage(id)

	pack, err := NewMsgPackFromMessage(immutableMessage)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}

func TestNewMsgPackFromCreateSegmentMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	mutableMsg, err := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	immutableCreateSegmentMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	pack, err := NewMsgPackFromMessage(immutableCreateSegmentMsg)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}

func TestNewMsgPackFromCreateIndexMessage(t *testing.T) {
	id := rmq.NewRmqID(1)

	tt := uint64(time.Now().UnixNano())
	mutableMsg, err := message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{}).
		WithBody(&message.CreateIndexMessageBody{}).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	immutableCreateIndexMsg := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	pack, err := NewMsgPackFromMessage(immutableCreateIndexMsg)
	assert.NoError(t, err)
	assert.NotNil(t, pack)
	assert.Equal(t, tt, pack.BeginTs)
	assert.Equal(t, tt, pack.EndTs)
}
