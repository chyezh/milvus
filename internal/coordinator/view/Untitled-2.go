package view

type RequestSegmentRequest struct {
	Loaded   []int64
	Released []int64 // The segment id that request to be released, it should be released asynchronously.
}

type DoneResult struct {
	SegmentID int64
	Err       error
}

type RequestSegmentResponse struct {
	Loaded <-chan DoneResult // The segment id that request to be loaded will be sent to this channel.
	// All the segmentID inputed at the request'loaded will should be sent to this channel.
	// Error should be returned if the segment can not be loaded on these node.
	// After all the segmentID has been sent to this channel, the channel will be closed.
	// The Done channel will be promised consumed by the caller.
}

// SegmentManager is the interface that manages the segment at querynode.
// Underlying segment manager should support ref-counting-based segment state transition.
type SegmentManager interface {
	// RequestSegment will request the segment to be loaded at querynode.
	// The segment will be loaded async, the result will be sent to the Done channel.
	RequestSegment(req *RequestSegmentRequest) (resp *RequestSegmentResponse)
}
