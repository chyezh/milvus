package adaptor

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var UnmashalerDispatcher = (&msgstream.ProtoUDFactory{}).NewUnmarshalDispatcher()

// FromMessageToMsgPack converts message to msgpack.
// Same TimeTick must be sent with same msgpack.
// !!! Msgs must be keep same time tick.
// TODO: remove this function after remove the msgstream implementation.
func NewMsgPackFromMessage(msgs ...message.ImmutableMessage) (*msgstream.MsgPack, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	allTsMsgs := make([]msgstream.TsMsg, 0, len(msgs))

	var finalErr error
	for _, msg := range msgs {
		// Parse a transaction message into multiple tsMsgs.
		if msg.MessageType() == message.MessageTypeTxn {
			tsMsgs, err := parseTxnMsg(msg)
			if err != nil {
				finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert txn message to msgpack, %v", msg.MessageID()))
				continue
			}
			allTsMsgs = append(allTsMsgs, tsMsgs...)
			continue
		}

		tsMsgs, err := parseSingleMsg(msg)
		if err != nil {
			finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert message to msgpack, %v", msg.MessageID()))
			continue
		}
		allTsMsgs = append(allTsMsgs, tsMsgs...)
	}
	if len(allTsMsgs) == 0 {
		return nil, finalErr
	}

	// msgs is sorted by time tick.
	// Postition use the last confirmed message id.
	// 1. So use the first tsMsgs's Position can read all messages which timetick is greater or equal than the first tsMsgs's BeginTs.
	//    In other words, from the StartPositions, you can read the full msgPack.
	// 2. Use the last tsMsgs's Position as the EndPosition, you can read all messages following the msgPack.
	beginTs := allTsMsgs[0].BeginTs()
	endTs := allTsMsgs[len(allTsMsgs)-1].EndTs()
	startPosition := allTsMsgs[0].Position()
	endPosition := allTsMsgs[len(allTsMsgs)-1].Position()
	// filter the TimeTick message.
	tsMsgs := make([]msgstream.TsMsg, 0, len(allTsMsgs))
	for _, msg := range allTsMsgs {
		if msg.Type() == commonpb.MsgType_TimeTick {
			continue
		}
		tsMsgs = append(tsMsgs, msg)
	}
	return &msgstream.MsgPack{
		BeginTs:        beginTs,
		EndTs:          endTs,
		Msgs:           tsMsgs,
		StartPositions: []*msgstream.MsgPosition{startPosition},
		EndPositions:   []*msgstream.MsgPosition{endPosition},
	}, finalErr
}

// parseTxnMsg converts a txn message to ts message list.
func parseTxnMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	txnMsg := message.AsImmutableTxnMessage(msg)
	if txnMsg == nil {
		panic("unreachable code, message must be a txn message")
	}

	tsMsgs := make([]msgstream.TsMsg, 0, txnMsg.Size())
	err := txnMsg.RangeOver(func(im message.ImmutableMessage) error {
		parsedMsgs, err := parseSingleMsg(im)
		if err != nil {
			return err
		}
		tsMsgs = append(tsMsgs, parsedMsgs...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tsMsgs, nil
}

// parseSingleMsg converts message to ts message.
func parseSingleMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	switch msg.Version() {
	case message.VersionV1, message.VersionOld:
		return fromMessageToTsMsgV1(msg)
	case message.VersionV2:
		return fromMessageToTsMsgV2(msg)
	default:
		panic("unsupported message version")
	}
}

// fromMessageToTsMsgV1 converts message to ts message.
func fromMessageToTsMsgV1(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	tsMsg, err := UnmashalerDispatcher.Unmarshal(msg.Payload(), MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal message")
	}

	tsMsgs, err := recoverMessageFromHeader(tsMsg, msg)
	if err != nil {
		return nil, err
	}
	setTsAndPosition(tsMsgs, msg)
	return tsMsgs, nil
}

// fromMessageToTsMsgV2 converts message to ts message.
func fromMessageToTsMsgV2(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	var tsMsgs []msgstream.TsMsg
	var err error
	switch msg.MessageType() {
	case message.MessageTypeFlush:
		tsMsgs, err = oneTsMsg(NewFlushMessageBody(msg))
	case message.MessageTypeManualFlush:
		tsMsgs, err = oneTsMsg(NewManualFlushMessageBody(msg))
	case message.MessageTypeFlushAll:
		tsMsgs, err = oneTsMsg(NewFlushAllMessageBody(msg))
	case message.MessageTypeCreateSegment:
		tsMsgs, err = oneTsMsg(NewCreateSegmentMessageBody(msg))
	case message.MessageTypeSchemaChange:
		tsMsgs, err = oneTsMsg(NewSchemaChangeMessageBody(msg))
	case message.MessageTypeAlterCollection:
		tsMsgs, err = oneTsMsg(NewAlterCollectionMessageBody(msg))
	case message.MessageTypeTruncateCollection:
		tsMsgs, err = oneTsMsg(NewTruncateCollectionMessageBody(msg))
	case message.MessageTypeAlterWAL:
		tsMsgs, err = oneTsMsg(NewAlterWALMessageBody(msg))
	case message.MessageTypeCreateIndex:
		tsMsgs, err = oneTsMsg(NewCreateIndexMessageBody(msg))
	default:
		panic("unsupported message type")
	}
	if err != nil {
		return nil, err
	}
	setTsAndPosition(tsMsgs, msg)
	return tsMsgs, nil
}

func oneTsMsg(tsMsg msgstream.TsMsg, err error) ([]msgstream.TsMsg, error) {
	if err != nil {
		return nil, err
	}
	return []msgstream.TsMsg{tsMsg}, nil
}

func setTsAndPosition(tsMsgs []msgstream.TsMsg, msg message.ImmutableMessage) {
	position := &msgpb.MsgPosition{
		ChannelName: msg.VChannel(),
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: msg.TimeTick(),
		WALName:   commonpb.WALName(msg.WALName()),
	}
	for _, tsMsg := range tsMsgs {
		tsMsg.SetTs(msg.TimeTick())
		tsMsg.SetPosition(position)
	}
}

// recoverMessageFromHeader recovers message from header.
func recoverMessageFromHeader(tsMsg msgstream.TsMsg, msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMessage, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to insert message")
		}
		return recoverInsertMsgsFromHeader(tsMsg.(*msgstream.InsertMsg), insertMessage)
	case message.MessageTypeDelete:
		deleteMessage, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to delete message")
		}
		return oneTsMsg(recoverDeleteMsgFromHeader(tsMsg.(*msgstream.DeleteMsg), deleteMessage))
	case message.MessageTypeImport:
		importMessage, err := message.AsImmutableImportMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to import message")
		}
		return oneTsMsg(recoverImportMsgFromHeader(tsMsg.(*msgstream.ImportMsg), importMessage.Header(), msg.TimeTick()))
	default:
		return []msgstream.TsMsg{tsMsg}, nil
	}
}

// recoverInsertMsgFromHeader recovers insert message from header.
func recoverInsertMsgsFromHeader(insertMsg *msgstream.InsertMsg, msg message.ImmutableInsertMessageV1) ([]msgstream.TsMsg, error) {
	header := msg.Header()
	timetick := msg.TimeTick()

	if insertMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}

	tsMsgs := make([]msgstream.TsMsg, 0, len(header.GetPartitions()))
	rowOffset := uint64(0)
	for _, assignment := range header.GetPartitions() {
		if assignment.GetSegmentAssignment().GetSegmentId() == 0 {
			panic("unreachable code, partition id is not exist")
		}
		recovered, err := recoverInsertRequestFromPartitionAssignment(insertMsg.InsertRequest, assignment, rowOffset, timetick, msg.VChannel())
		if err != nil {
			return nil, err
		}
		tsMsgs = append(tsMsgs, recovered)
		rowOffset += assignment.GetRows()
	}
	if rowOffset != insertMsg.GetNumRows() {
		return nil, merr.WrapErrServiceInternalMsg("insert header rows %d does not match body rows %d", rowOffset, insertMsg.GetNumRows())
	}
	return tsMsgs, nil
}

func recoverInsertRequestFromPartitionAssignment(insertRequest *msgpb.InsertRequest, assignment *message.PartitionSegmentAssignment, rowOffset uint64, timetick uint64, vchannel string) (msgstream.TsMsg, error) {
	rows := assignment.GetRows()
	if rowOffset+rows > insertRequest.GetNumRows() {
		return nil, merr.WrapErrServiceInternalMsg("insert header rows exceed body rows, offset %d rows %d body rows %d",
			rowOffset, rows, insertRequest.GetNumRows())
	}

	recovered := proto.Clone(insertRequest).(*msgpb.InsertRequest)
	recovered.PartitionID = assignment.GetPartitionId()
	if recovered.GetPartitionID() != insertRequest.GetPartitionID() {
		recovered.PartitionName = ""
	}
	recovered.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()
	recovered.NumRows = rows
	recovered.RowIDs = sliceInt64(insertRequest.GetRowIDs(), rowOffset, rows)
	recovered.FieldsData = make([]*schemapb.FieldData, len(insertRequest.GetFieldsData()))

	idxComputer := typeutil.NewFieldDataIdxComputer(insertRequest.GetFieldsData())
	for rowIdx := rowOffset; rowIdx < rowOffset+rows; rowIdx++ {
		fieldIdxs := idxComputer.Compute(int64(rowIdx))
		typeutil.AppendFieldData(recovered.FieldsData, insertRequest.GetFieldsData(), int64(rowIdx), fieldIdxs...)
	}

	timestamps := make([]uint64, rows)
	for i := 0; i < len(timestamps); i++ {
		timestamps[i] = timetick
	}
	recovered.Timestamps = timestamps
	if recovered.Base == nil {
		recovered.Base = &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}
	}
	recovered.Base.Timestamp = timetick
	recovered.ShardName = vchannel
	return &msgstream.InsertMsg{InsertRequest: recovered}, nil
}

func sliceInt64(values []int64, offset uint64, rows uint64) []int64 {
	if len(values) == 0 {
		return nil
	}
	start := int(offset)
	end := int(offset + rows)
	return append([]int64(nil), values[start:end]...)
}

func recoverDeleteMsgFromHeader(deleteMsg *msgstream.DeleteMsg, msg message.ImmutableDeleteMessageV1) (msgstream.TsMsg, error) {
	header := msg.Header()
	timetick := msg.TimeTick()

	if deleteMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}
	timestamps := make([]uint64, len(deleteMsg.Timestamps))
	for i := 0; i < len(timestamps); i++ {
		timestamps[i] = timetick
	}
	deleteMsg.Timestamps = timestamps
	deleteMsg.ShardName = msg.VChannel()
	return deleteMsg, nil
}

func recoverImportMsgFromHeader(importMsg *msgstream.ImportMsg, _ *message.ImportMessageHeader, timetick uint64) (msgstream.TsMsg, error) {
	importMsg.Base.Timestamp = timetick
	return importMsg, nil
}
