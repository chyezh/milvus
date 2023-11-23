package interceptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// NewChainedInterceptor creates a new chained interceptor.
func NewChainedInterceptor(interceptors ...AppendInterceptor) AppendInterceptorWithReady {
	if len(interceptors) == 0 {
		return nil
	}
	calls := make([]AppendInterceptorCall, 0, len(interceptors))
	for _, i := range interceptors {
		calls = append(calls, i.Do)
	}
	return &chainedInterceptor{
		closed:       make(chan struct{}),
		interceptors: interceptors,
		do:           chainUnaryClientInterceptors(calls),
	}
}

// chainedInterceptor chains all interceptors into one.
type chainedInterceptor struct {
	closed       chan struct{}
	interceptors []AppendInterceptor
	do           func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)
}

// Ready wait all interceptors to be ready.
func (c *chainedInterceptor) Ready() <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for _, i := range c.interceptors {
			// check if ready is implemented
			if r, ok := i.(AppendInterceptorWithReady); ok {
				select {
				case <-r.Ready():
				case <-c.closed:
					return
				}
			}
		}
		close(ready)
	}()
	return ready
}

// Do execute the chained interceptors.
func (c *chainedInterceptor) Do(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error) {
	return c.do(ctx, msg, append)
}

// Close close all interceptors.
func (c *chainedInterceptor) Close() {
	for _, i := range c.interceptors {
		i.Close()
	}
	close(c.closed)
}

// chainUnaryClientInterceptors chains all unary client interceptors into one.
func chainUnaryClientInterceptors(interceptorCalls []AppendInterceptorCall) AppendInterceptorCall {
	if len(interceptorCalls) == 0 {
		return nil
	} else if len(interceptorCalls) == 1 {
		return interceptorCalls[0]
	} else {
		return func(ctx context.Context, msg message.MutableMessage, invoker Append) (message.MessageID, error) {
			return interceptorCalls[0](ctx, msg, getChainUnaryInvoker(interceptorCalls, 0, invoker))
		}
	}
}

// getChainUnaryInvoker recursively generate the chained unary invoker.
func getChainUnaryInvoker(interceptors []AppendInterceptorCall, idx int, finalInvoker Append) Append {
	// all interceptor is called, so return the final invoker.
	if idx == len(interceptors)-1 {
		return finalInvoker
	}
	// recursively generate the chained invoker.
	return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return interceptors[idx+1](ctx, msg, getChainUnaryInvoker(interceptors, idx+1, finalInvoker))
	}
}
