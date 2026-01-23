package adaptor

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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

		// Parse an upsert message into delete + insert messages.
		// Upsert semantically equals to a Txn(Delete, Insert), but packed in a single message for efficiency.
		// We unpack it here so downstream components (DataNode) only see Delete and Insert messages.
		if msg.MessageType() == message.MessageTypeUpsert {
			tsMsgs, err := parseUpsertMsg(msg)
			if err != nil {
				finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert upsert message to msgpack, %v", msg.MessageID()))
				continue
			}
			allTsMsgs = append(allTsMsgs, tsMsgs...)
			continue
		}

		tsMsg, err := parseSingleMsg(msg)
		if err != nil {
			finalErr = errors.CombineErrors(finalErr, errors.Wrapf(err, "Failed to convert message to msgpack, %v", msg.MessageID()))
			continue
		}
		allTsMsgs = append(allTsMsgs, tsMsg)
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
		var tsMsg msgstream.TsMsg
		tsMsg, err := parseSingleMsg(im)
		if err != nil {
			return err
		}
		tsMsgs = append(tsMsgs, tsMsg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tsMsgs, nil
}

// parseUpsertMsg converts an upsert message to delete + insert ts message list.
// Upsert message is unpacked into Delete followed by Insert to maintain semantic consistency
// with the original Txn-wrapped Delete+Insert approach.
func parseUpsertMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	// Upsert is V2 message, convert to ImmutableUpsertMessageV2
	upsertMsg, err := message.AsImmutableUpsertMessageV2(msg)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to convert message to upsert message")
	}

	// Get the message body which contains InsertRequest and DeleteRequest
	body, err := upsertMsg.Body()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get upsert message body")
	}

	header := upsertMsg.Header()
	timetick := upsertMsg.TimeTick()
	vchannel := upsertMsg.VChannel()

	// Create msgPosition for both delete and insert messages
	msgPosition := &msgpb.MsgPosition{
		ChannelName: vchannel,
		MsgID:       MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:    "",
		Timestamp:   timetick,
		WALName:     commonpb.WALName(msg.WALName()),
	}

	// Unpack into Delete + Insert messages
	// Order: Delete first, then Insert (same as original Txn behavior)
	tsMsgs := make([]msgstream.TsMsg, 0, 2)

	// Parse delete part
	if body.GetDeleteRequest() != nil {
		deleteReq := body.GetDeleteRequest()
		deleteMsg := &msgstream.DeleteMsg{
			DeleteRequest: deleteReq,
		}
		deleteMsg.SetTs(timetick)
		deleteMsg.SetPosition(msgPosition)

		// Set timestamps for all delete operations
		timestamps := make([]uint64, len(deleteMsg.Timestamps))
		for i := range timestamps {
			timestamps[i] = timetick
		}
		deleteMsg.Timestamps = timestamps
		deleteMsg.ShardName = vchannel

		tsMsgs = append(tsMsgs, deleteMsg)
	}

	// Parse insert part
	if body.GetInsertRequest() != nil {
		insertReq := body.GetInsertRequest()
		insertMsg := &msgstream.InsertMsg{
			InsertRequest: insertReq,
		}
		insertMsg.SetTs(timetick)
		insertMsg.SetPosition(msgPosition)

		// Find the partition assignment for insert
		var assignment *message.PartitionSegmentAssignment
		for _, p := range header.Partitions {
			if p.GetPartitionId() == insertMsg.GetPartitionID() {
				assignment = p
				break
			}
		}
		if assignment == nil || assignment.GetSegmentAssignment().GetSegmentId() == 0 {
			return nil, errors.New("partition assignment not found or segment id is 0")
		}

		// Set insert message fields from header
		insertMsg.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()
		timestamps := make([]uint64, insertMsg.GetNumRows())
		for i := range timestamps {
			timestamps[i] = timetick
		}
		insertMsg.Timestamps = timestamps
		insertMsg.Base.Timestamp = timetick
		insertMsg.ShardName = vchannel

		tsMsgs = append(tsMsgs, insertMsg)
	}

	return tsMsgs, nil
}

// parseSingleMsg converts message to ts message.
func parseSingleMsg(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
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
func fromMessageToTsMsgV1(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	tsMsg, err := UnmashalerDispatcher.Unmarshal(msg.Payload(), MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal message")
	}
	tsMsg.SetTs(msg.TimeTick())
	tsMsg.SetPosition(&msgpb.MsgPosition{
		ChannelName: msg.VChannel(),
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: msg.TimeTick(),
		WALName:   commonpb.WALName(msg.WALName()),
	})

	return recoverMessageFromHeader(tsMsg, msg)
}

// fromMessageToTsMsgV2 converts message to ts message.
func fromMessageToTsMsgV2(msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	var tsMsg msgstream.TsMsg
	var err error
	switch msg.MessageType() {
	case message.MessageTypeFlush:
		tsMsg, err = NewFlushMessageBody(msg)
	case message.MessageTypeManualFlush:
		tsMsg, err = NewManualFlushMessageBody(msg)
	case message.MessageTypeFlushAll:
		tsMsg, err = NewFlushAllMessageBody(msg)
	case message.MessageTypeCreateSegment:
		tsMsg, err = NewCreateSegmentMessageBody(msg)
	case message.MessageTypeSchemaChange:
		tsMsg, err = NewSchemaChangeMessageBody(msg)
	case message.MessageTypeAlterCollection:
		tsMsg, err = NewAlterCollectionMessageBody(msg)
	case message.MessageTypeTruncateCollection:
		tsMsg, err = NewTruncateCollectionMessageBody(msg)
	case message.MessageTypeAlterWAL:
		tsMsg, err = NewAlterWALMessageBody(msg)
	default:
		panic("unsupported message type")
	}
	if err != nil {
		return nil, err
	}
	tsMsg.SetTs(msg.TimeTick())
	tsMsg.SetPosition(&msgpb.MsgPosition{
		ChannelName: msg.VChannel(),
		// from the last confirmed message id, you can read all messages which timetick is greater or equal than current message id.
		MsgID:     MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(),
		MsgGroup:  "", // Not important any more.
		Timestamp: msg.TimeTick(),
		WALName:   commonpb.WALName(msg.WALName()),
	})
	return tsMsg, nil
}

// recoverMessageFromHeader recovers message from header.
func recoverMessageFromHeader(tsMsg msgstream.TsMsg, msg message.ImmutableMessage) (msgstream.TsMsg, error) {
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insertMessage, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to insert message")
		}
		// insertMsg has multiple partition and segment assignment is done by insert message header.
		// so recover insert message from header before send it.
		return recoverInsertMsgFromHeader(tsMsg.(*msgstream.InsertMsg), insertMessage)
	case message.MessageTypeDelete:
		deleteMessage, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to delete message")
		}
		return recoverDeleteMsgFromHeader(tsMsg.(*msgstream.DeleteMsg), deleteMessage)
	case message.MessageTypeUpsert:
		upsertMessage, err := message.AsImmutableUpsertMessageV2(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to upsert message")
		}
		return recoverUpsertMsgFromHeader(tsMsg.(*msgstream.UpsertMsg), upsertMessage)
	case message.MessageTypeImport:
		importMessage, err := message.AsImmutableImportMessageV1(msg)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert message to import message")
		}
		return recoverImportMsgFromHeader(tsMsg.(*msgstream.ImportMsg), importMessage.Header(), msg.TimeTick())
	default:
		return tsMsg, nil
	}
}

// recoverInsertMsgFromHeader recovers insert message from header.
func recoverInsertMsgFromHeader(insertMsg *msgstream.InsertMsg, msg message.ImmutableInsertMessageV1) (msgstream.TsMsg, error) {
	header := msg.Header()
	timetick := msg.TimeTick()

	if insertMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}
	// header promise a batch insert on vchannel in future, so header has multiple partition.
	var assignment *message.PartitionSegmentAssignment
	for _, p := range header.Partitions {
		if p.GetPartitionId() == insertMsg.GetPartitionID() {
			assignment = p
			break
		}
	}
	if assignment.GetSegmentAssignment().GetSegmentId() == 0 {
		panic("unreachable code, partition id is not exist")
	}

	insertMsg.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()
	// timetick should has been assign at streaming node.
	// so overwrite the timetick on insertRequest.
	timestamps := make([]uint64, insertMsg.GetNumRows())
	for i := 0; i < len(timestamps); i++ {
		timestamps[i] = timetick
	}
	insertMsg.Timestamps = timestamps
	insertMsg.Base.Timestamp = timetick
	insertMsg.ShardName = msg.VChannel()
	return insertMsg, nil
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

// recoverUpsertMsgFromHeader recovers upsert message from header.
// Upsert contains both insert and delete operations, need to recover both parts.
func recoverUpsertMsgFromHeader(upsertMsg *msgstream.UpsertMsg, msg message.ImmutableUpsertMessageV2) (msgstream.TsMsg, error) {
	header := msg.Header()
	timetick := msg.TimeTick()
	vchannel := msg.VChannel()

	// Recover insert part
	insertMsg := upsertMsg.InsertMsg
	if insertMsg == nil {
		return nil, errors.New("upsert message insert part is nil")
	}

	if insertMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}

	// Find the partition assignment for insert
	var assignment *message.PartitionSegmentAssignment
	for _, p := range header.Partitions {
		if p.GetPartitionId() == insertMsg.GetPartitionID() {
			assignment = p
			break
		}
	}
	if assignment == nil || assignment.GetSegmentAssignment().GetSegmentId() == 0 {
		panic("unreachable code, partition id is not exist or segment id is 0")
	}

	// Set insert message fields from header
	insertMsg.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()
	insertTimestamps := make([]uint64, insertMsg.GetNumRows())
	for i := 0; i < len(insertTimestamps); i++ {
		insertTimestamps[i] = timetick
	}
	insertMsg.Timestamps = insertTimestamps
	insertMsg.Base.Timestamp = timetick
	insertMsg.ShardName = vchannel

	// Recover delete part
	deleteMsg := upsertMsg.DeleteMsg
	if deleteMsg == nil {
		return nil, errors.New("upsert message delete part is nil")
	}

	if deleteMsg.GetCollectionID() != header.GetCollectionId() {
		panic("unreachable code, collection id is not equal")
	}

	// Set delete message fields
	deleteTimestamps := make([]uint64, len(deleteMsg.Timestamps))
	for i := 0; i < len(deleteTimestamps); i++ {
		deleteTimestamps[i] = timetick
	}
	deleteMsg.Timestamps = deleteTimestamps
	deleteMsg.ShardName = vchannel

	return upsertMsg, nil
}
