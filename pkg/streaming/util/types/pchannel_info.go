package types

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	InitialTerm  int64      = -1
	AccessModeRW AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE) // It's the default option.
	AccessModeRO AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY)
)

type AccessMode streamingpb.PChannelAccessMode

func (m AccessMode) String() string {
	switch m {
	case AccessModeRO:
		return "ro"
	case AccessModeRW:
		return "rw"
	default:
		panic("undefined access mode")
	}
}

// NewPChannelInfoFromProto converts protobuf PChannelInfo to PChannelInfo
func NewPChannelInfoFromProto(pchannel *streamingpb.PChannelInfo) PChannelInfo {
	if pchannel.GetName() == "" {
		panic("pchannel name is empty")
	}
	if pchannel.GetTerm() <= 0 {
		panic("pchannel term is empty or negetive")
	}
	accessMode := AccessMode(pchannel.GetAccessMode())
	_ = accessMode.String() // assertion.
	return PChannelInfo{
		Name:       pchannel.GetName(),
		Term:       pchannel.GetTerm(),
		AccessMode: accessMode,
	}
}

// NewProtoFromPChannelInfo converts PChannelInfo to protobuf PChannelInfo
func NewProtoFromPChannelInfo(pchannel PChannelInfo) *streamingpb.PChannelInfo {
	if pchannel.Name == "" {
		panic("pchannel name is empty")
	}
	if pchannel.Term <= 0 {
		panic("pchannel term is empty or negetive")
	}
	return &streamingpb.PChannelInfo{
		Name:       pchannel.Name,
		Term:       pchannel.Term,
		AccessMode: streamingpb.PChannelAccessMode(pchannel.AccessMode),
	}
}

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name       string     // name of pchannel.
	Term       int64      // term of pchannel.
	AccessMode AccessMode // Access mode, if AccessModeRO, the wal impls should be read-only, the append operation will panics.
	// If accessMode is AccessModeRW, the wal impls should be read-write,
	// and it will fence the old rw wal impls or wait the old rw wal impls close.
}

func (c PChannelInfo) String() string {
	return fmt.Sprintf("%s:%s@%d", c.Name, c.AccessMode, c.Term)
}

type PChannelInfoAssigned struct {
	Channel PChannelInfo
	Node    StreamingNodeInfo
}

// PchannelAssignState is the state of pchannel assignment.
type PChannelAssignState struct {
	Channel   PChannelInfo
	Available bool // Channel should be closed if it's not available.
}

// String returns the string representation of the applyWALRequest.
func (state PChannelAssignState) String() string {
	return fmt.Sprintf("(%s,%t)", state.Channel.String(), state.Available)
}

// Before returns whether the state is before other state.
func (state PChannelAssignState) Before(other PChannelAssignState) bool {
	// w1 is before w2 if term of w1 is less than w2.
	// or w1 is available and w2 is not available in same term.
	// because wal should always be available before unavailable in same term.
	// (1, true) -> (1, false) is allowed.
	// (1, true) -> (2, false) is allowed.
	// (1, false) -> (2, true) is allowed.
	// (1, false) -> (1, true) is not allowed.
	return state.Channel.Term < other.Channel.Term || (state.Channel.Term == other.Channel.Term && state.Available && !other.Available)
}
