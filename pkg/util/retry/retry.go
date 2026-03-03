// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func getCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}
	return file + ":" + strconv.Itoa(line)
}

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	if !funcutil.CheckCtxValid(ctx) {
		return ctx.Err()
	}

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var lastErr error

	for i := uint(0); c.attempts == 0 || i < c.attempts; i++ {
		if err := fn(); err != nil {
			if i%4 == 0 {
				log.Warn(context.TODO(), "retry func failed",
					log.Uint("retried", i),
					log.Err(err),
					log.String("caller", getCaller(2)))
			}

			if !IsRecoverable(err) {
				isContextErr := errors.IsAny(err, context.Canceled, context.DeadlineExceeded)
				log.Warn(context.TODO(), "retry func failed, not be recoverable",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.Bool("isContextErr", isContextErr),
					log.String("caller", getCaller(2)),
				)
				if isContextErr && lastErr != nil {
					return lastErr
				}
				return err
			}
			if c.isRetryErr != nil && !c.isRetryErr(err) {
				log.Warn(context.TODO(), "retry func failed, not be retryable",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.String("caller", getCaller(2)),
				)
				return err
			}

			deadline, ok := ctx.Deadline()
			if ok && time.Until(deadline) < c.sleep {
				isContextErr := errors.IsAny(err, context.Canceled, context.DeadlineExceeded)
				log.Warn(context.TODO(), "retry func failed, deadline",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.Bool("isContextErr", isContextErr),
					log.String("caller", getCaller(2)),
				)
				if isContextErr && lastErr != nil {
					return lastErr
				}
				return err
			}

			lastErr = err

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				log.Warn(context.TODO(), "retry func failed, ctx done",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.String("caller", getCaller(2)),
				)
				return lastErr
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	if lastErr != nil {
		log.Warn(context.TODO(), "retry func failed, reach max retry",
			log.Uint("attempt", c.attempts),
		)
	}
	return lastErr
}

// Do will run function with retry mechanism.
// fn is the func to run, return err and shouldRetry flag.
// Option can control the retry times and timeout.
func Handle(ctx context.Context, fn func() (bool, error), opts ...Option) error {
	if !funcutil.CheckCtxValid(ctx) {
		return ctx.Err()
	}

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var lastErr error
	for i := uint(0); c.attempts == 0 || i < c.attempts; i++ {
		if shouldRetry, err := fn(); err != nil {
			if i%4 == 0 {
				log.Warn(context.TODO(), "retry func failed",
					log.Uint("retried", i),
					log.String("caller", getCaller(2)),
					log.Err(err),
				)
			}

			if !shouldRetry {
				isContextErr := errors.IsAny(err, context.Canceled, context.DeadlineExceeded)
				log.Warn(context.TODO(), "retry func failed, not be recoverable",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.Bool("isContextErr", isContextErr),
					log.String("caller", getCaller(2)),
				)
				if isContextErr && lastErr != nil {
					return lastErr
				}
				return err
			}

			deadline, ok := ctx.Deadline()
			if ok && time.Until(deadline) < c.sleep {
				isContextErr := errors.IsAny(err, context.Canceled, context.DeadlineExceeded)
				log.Warn(context.TODO(), "retry func failed, deadline",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.Bool("isContextErr", isContextErr),
					log.String("caller", getCaller(2)),
				)
				if isContextErr && lastErr != nil {
					return lastErr
				}
				return err
			}

			lastErr = err

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				log.Warn(context.TODO(), "retry func failed, ctx done",
					log.Uint("retried", i),
					log.Uint("attempt", c.attempts),
					log.String("caller", getCaller(2)),
				)
				return lastErr
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	if lastErr != nil {
		log.Warn(context.TODO(), "retry func failed, reach max retry",
			log.Uint("attempt", c.attempts),
			log.String("caller", getCaller(2)),
		)
	}
	return lastErr
}

// errUnrecoverable is error instance for unrecoverable.
var errUnrecoverable = errors.New("unrecoverable error")

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return merr.Combine(err, errUnrecoverable)
}

// IsRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsRecoverable(err error) bool {
	return !errors.Is(err, errUnrecoverable)
}
