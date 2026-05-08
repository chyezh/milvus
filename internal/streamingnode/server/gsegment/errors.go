// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import "github.com/cockroachdb/errors"

// ErrContinue signals that a Poll stage completed and the task should be
// re-polled (the task itself will have advanced its internal stage and may have
// flipped CPUBound() to target the other pool). Returning ErrContinue is not
// a failure — it's just a yield point so the scheduler can hand the task off
// between the CPU and IO worker pools.
var ErrContinue = errors.New("gsegment: task should continue")

// retryableError wraps a transient error so the scheduler knows to put the
// task on the retry heap with exponential backoff rather than fail it outright.
type retryableError struct {
	inner error
}

// NewRetryableError wraps err so that the scheduler treats the failure as
// transient and retries it with exponential backoff. Callers should use this
// for IO errors against object storage, network blips, etc.
func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{inner: err}
}

func (e *retryableError) Error() string {
	return "retryable: " + e.inner.Error()
}

func (e *retryableError) Unwrap() error { return e.inner }

// IsRetryable reports whether err was wrapped by NewRetryableError.
func IsRetryable(err error) bool {
	var r *retryableError
	return errors.As(err, &r)
}
