package timestamp

type AckOption func(*AckDetail)

// OptSync marks the timestampAck is sync message.
func OptSync() AckOption {
	return func(detail *AckDetail) {
		detail.IsSync = true
	}
}

// OptError marks the timestamp ack with error info.
func OptError(err error) AckOption {
	return func(detail *AckDetail) {
		detail.Err = err
	}
}
