package types

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const (
	InitialTerm  int64      = -1
	AccessModeRO AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY)
	AccessModeRW AccessMode = AccessMode(streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE)
)

type AccessMode streamingpb.PChannelAccessMode

// Match checks if the access mode m can satisfy the access mode m2.
func (m AccessMode) Match(m2 AccessMode) bool {
	if m == AccessModeRW {
		return true
	}
	// if m is read only, the m2 must be read only.
	return m2 != AccessModeRW
}

func (m AccessMode) String() string {
	switch m {
	case AccessModeRO:
		return "RW"
	case AccessModeRW:
		return "RO"
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
	return PChannelInfo{
		Name: pchannel.GetName(),
		Term: pchannel.GetTerm(),
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
		Name: pchannel.Name,
		Term: pchannel.Term,
	}
}

// PChannelInfo is the struct for pchannel info.
type PChannelInfo struct {
	Name string // name of pchannel.
	Term int64  // term of pchannel.
}

func (c *PChannelInfo) String() string {
	return fmt.Sprintf("%s@%d", c.Name, c.Term)
}

type PChannelInfoAssigned struct {
	Channel PChannelInfo
	Node    StreamingNodeInfo
}
