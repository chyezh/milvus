package options

import (
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

// DeliverOrder is the order of delivering messages.
type (
	DeliverPolicy = *streamingpb.DeliverPolicy
	DeliverFilter = *streamingpb.DeliverFilter
)

// DeliverPolicyAll delivers all messages.
func DeliverPolicyAll() DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_All{
			All: &emptypb.Empty{},
		},
	}
}

// DeliverLatest delivers the latest message.
func DeliverPolicyLatest() DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_Latest{
			Latest: &emptypb.Empty{},
		},
	}
}

// DeliverEarliest delivers the earliest message.
func DeliverPolicyStartFrom(messageID message.MessageID) DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_StartFrom{
			StartFrom: &streamingpb.MessageID{
				Id: messageID.Marshal(),
			},
		},
	}
}

// DeliverPolicyStartAfter delivers the message after the specified message.
func DeliverPolicyStartAfter(messageID message.MessageID) DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_StartAfter{
			StartAfter: &streamingpb.MessageID{
				Id: messageID.Marshal(),
			},
		},
	}
}

// DeliverFilterTimeTickGT filters messages by time tick greater than the specified time tick.
func DeliverFilterTimeTickGT(timetick uint64) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_TimeTickGt{
			TimeTickGt: &streamingpb.DeliverFilterTimeTickGT{
				TimeTick: timetick,
			},
		},
	}
}

// DeliverFilterTimeTickGTE filters messages by time tick greater or equal than the specified time tick.
func DeliverFilterTimeTickGTE(timetick uint64) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_TimeTickGte{
			TimeTickGte: &streamingpb.DeliverFilterTimeTickGTE{
				TimeTick: timetick,
			},
		},
	}
}

// DeliverFilterVChannel filters messages by vchannel.
func DeliverFilterVChannel(vchannel string) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_Vchannel{
			Vchannel: &streamingpb.DeliverFilterVChannel{
				Vchannel: vchannel,
			},
		},
	}
}
