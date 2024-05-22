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

package pulsar

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestPulsarID_Serialize(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}

	binary := pid.Serialize()
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_AtEarliestPosition(t *testing.T) {
	mid := pulsar.EarliestMessageID()
	pid := &pulsarID{
		messageID: mid,
	}
	assert.True(t, pid.AtEarliestPosition())

	mid = pulsar.LatestMessageID()
	pid = &pulsarID{
		messageID: mid,
	}
	assert.False(t, pid.AtEarliestPosition())
}

func TestLessOrEqualThan(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	ret, err := pid1.LessOrEqualThan(pid2.Serialize())
	assert.NoError(t, err)
	assert.True(t, ret)

	ret, err = pid2.LessOrEqualThan(pid1.Serialize())
	assert.NoError(t, err)
	assert.False(t, ret)

	ret, err = pid2.LessOrEqualThan([]byte{1})
	assert.Error(t, err)
	assert.False(t, ret)
}

func TestPulsarID_Compare(t *testing.T) {
	ids := []*pulsarID{
		newMessageIDOfPulsar(0, 0, 0),
		newMessageIDOfPulsar(0, 0, 1),
		newMessageIDOfPulsar(0, 0, 1000),
		newMessageIDOfPulsar(0, 1, 0),
		newMessageIDOfPulsar(0, 1, 1000),
		newMessageIDOfPulsar(0, 1000, 0),
		newMessageIDOfPulsar(1, 0, 0),
		newMessageIDOfPulsar(1, 1000, 0),
		newMessageIDOfPulsar(2, 0, 0),
	}

	for x, idx := range ids {
		for y, idy := range ids {
			assert.Equal(t, idx.EQ(idy), x == y)
			assert.Equal(t, idy.EQ(idx), x == y)
			assert.Equal(t, idy.LT(idx), x > y)
			assert.Equal(t, idy.LTE(idx), x >= y)
			assert.Equal(t, idx.LT(idy), x < y)
			assert.Equal(t, idx.LTE(idy), x <= y)
		}
	}
}

func TestPulsarID_Equal(t *testing.T) {
	msg1 := pulsar.EarliestMessageID()
	pid1 := &pulsarID{
		messageID: msg1,
	}

	msg2 := pulsar.LatestMessageID()
	pid2 := &pulsarID{
		messageID: msg2,
	}

	{
		ret, err := pid1.Equal(pid1.Serialize())
		assert.NoError(t, err)
		assert.True(t, ret)
	}

	{
		ret, err := pid1.Equal(pid2.Serialize())
		assert.NoError(t, err)
		assert.False(t, ret)
	}
}

func Test_SerializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	assert.NotNil(t, binary)
	assert.NotZero(t, len(binary))
}

func Test_DeserializePulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	binary := SerializePulsarMsgID(mid)
	res, err := DeserializePulsarMsgID(binary)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func Test_PulsarMsgIDToString(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := msgIDToString(mid)
	assert.NotNil(t, str)
	assert.NotZero(t, len(str))
}

func Test_StringToPulsarMsgID(t *testing.T) {
	mid := pulsar.EarliestMessageID()

	str := msgIDToString(mid)
	res, err := stringToMsgID(str)
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

// only for pulsar id unittest.
type MessageIdData struct {
	LedgerId   *uint64 `protobuf:"varint,1,req,name=ledgerId" json:"ledgerId,omitempty"`
	EntryId    *uint64 `protobuf:"varint,2,req,name=entryId" json:"entryId,omitempty"`
	Partition  *int32  `protobuf:"varint,3,opt,name=partition,def=-1" json:"partition,omitempty"`
	BatchIndex *int32  `protobuf:"varint,4,opt,name=batch_index,json=batchIndex,def=-1" json:"batch_index,omitempty"`
}

func (m *MessageIdData) Reset()         { *m = MessageIdData{} }
func (m *MessageIdData) String() string { return proto.CompactTextString(m) }

func (*MessageIdData) ProtoMessage() {}

// newMessageIDOfPulsar only for test.
func newMessageIDOfPulsar(ledgerID uint64, entryID uint64, batchIdx int32) *pulsarID {
	id := &MessageIdData{
		LedgerId:   &ledgerID,
		EntryId:    &entryID,
		BatchIndex: &batchIdx,
	}
	msg, err := proto.Marshal(id)
	if err != nil {
		panic(err)
	}
	msgID, err := pulsar.DeserializeMessageID(msg)
	if err != nil {
		panic(err)
	}

	return &pulsarID{
		messageID: msgID,
	}
}
