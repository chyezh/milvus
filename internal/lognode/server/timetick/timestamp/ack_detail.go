package timestamp

// newDefaultAckDetail creates a new default timestampAckDetail.
func newDefaultAckDetail(ts uint64) *AckDetail {
	return &AckDetail{
		Timestamp: ts,
		IsSync:    false,
		Err:       nil,
	}
}

// AckDetail records the information of timestampAck.
// TODO: fill it up.
type AckDetail struct {
	Timestamp uint64
	IsSync    bool
	Err       error
}

// AckDetails records the information of AckDetail.
// Used to analyze the ack details.
// TODO: add more analysis methods. e.g. such as counter function with filter.
type AckDetails struct {
	detail []*AckDetail
}

// AddDetails adds details to AckDetails.
func (ad *AckDetails) AddDetails(details []*AckDetail) {
	if len(details) == 0 {
		return
	}
	if len(ad.detail) == 0 {
		ad.detail = details
	}
	ad.detail = append(ad.detail, details...)
}

// Empty returns true if the AckDetails is empty.
func (ad *AckDetails) Empty() bool {
	return len(ad.detail) == 0
}

// Len returns the count of AckDetail.
func (ad *AckDetails) Len() int {
	return len(ad.detail)
}

// LastAllAcknowledgedTimestamp returns the last timestamp which all timestamps before it have been acknowledged.
// panic if no timestamp has been acknowledged.
func (ad *AckDetails) LastAllAcknowledgedTimestamp() uint64 {
	return ad.detail[len(ad.detail)-1].Timestamp
}

// Clear clears the AckDetails.
func (ad *AckDetails) Clear() {
	ad.detail = nil
}
