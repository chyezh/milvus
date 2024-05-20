package cgo

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestMain(m *testing.M) {
	init := NewFollyInit()
	defer init.Destroy()

	exitCode := m.Run()
	if exitCode > 0 {
		os.Exit(exitCode)
	}
}

func TestFutureWithSuccessCase(t *testing.T) {
	// Test success case.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   100,
	})
	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.NoError(t, err)
	assert.Equal(t, 100, int(*result))
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.
	freeCInt(result)
	runtime.GC()
}

func TestFutureWithCaseNoInterrupt(t *testing.T) {
	// Test success case.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoNoInterrupt,
	})
	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.NoError(t, err)
	assert.Equal(t, 0, int(*result))
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.
	freeCInt(result)

	// Test cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   caseNoNoInterrupt,
	})
	start = time.Now()
	result, err = future.BlockAndLeakyGet()
	// the future is timeout by the context after 200ms, so the future should be done in 1s but not 2s.
	assert.Less(t, time.Since(start).Seconds(), 1.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Nil(t, result)

	future.blockUntilReleasable()
	assert.Greater(t, time.Since(start).Seconds(), 2.0)
	runtime.GC()
}

// TestFutures test the future implementation.
func TestFutures(t *testing.T) {
	// Test failed case, throw folly exception.
	future := createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowStdException,
	})
	start := time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err := future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreUnsupported)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)

	// Test failed case, throw std exception.
	future = createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowFollyException,
	})
	start = time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err = future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyOtherException)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.

	// Test failed case, throw std exception.
	future = createFutureWithTestCase(context.Background(), testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  10,
		caseNo:   caseNoThrowSegcoreException,
	})
	start = time.Now()
	future.BlockUntilReady() // test block until ready too.
	result, err = future.BlockAndLeakyGet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreNotImplemented)
	assert.Nil(t, result)
	// The inner function sleep 1 seconds, so the future cost must be greater than 0.5 seconds.
	assert.Greater(t, time.Since(start).Seconds(), 0.5)
	// free the result after used.

	// Test cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   100,
	})
	// canceled before the future(2s) is ready.
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start = time.Now()
	result, err = future.BlockAndLeakyGet()
	// the future is canceled by the context after 200ms, so the future should be done in 1s but not 2s.
	assert.Less(t, time.Since(start).Seconds(), 1.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Nil(t, result)

	// Test cancellation.
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	future = createFutureWithTestCase(ctx, testCase{
		interval: 100 * time.Millisecond,
		loopCnt:  20,
		caseNo:   100,
	})
	start = time.Now()
	result, err = future.BlockAndLeakyGet()
	// the future is timeout by the context after 200ms, so the future should be done in 1s but not 2s.
	assert.Less(t, time.Since(start).Seconds(), 1.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Nil(t, result)
	runtime.GC()
}

func TestConcurrent(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(4)
		// success case
		go func() {
			defer wg.Done()
			// Test success case.
			future := createFutureWithTestCase(context.Background(), testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   100,
			})
			result, err := future.BlockAndLeakyGet()
			assert.NoError(t, err)
			assert.Equal(t, 100, int(*result))
			freeCInt(result)
		}()

		// fail case
		go func() {
			defer wg.Done()
			// Test success case.
			future := createFutureWithTestCase(context.Background(), testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   caseNoThrowStdException,
			})
			result, err := future.BlockAndLeakyGet()
			assert.Error(t, err)
			assert.Nil(t, result)
		}()

		// timeout case
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			future := createFutureWithTestCase(ctx, testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  20,
				caseNo:   100,
			})
			result, err := future.BlockAndLeakyGet()
			assert.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
			assert.True(t, errors.Is(err, context.DeadlineExceeded))
			assert.Nil(t, result)
		}()

		// no interrupt with timeout case
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			future := createFutureWithTestCase(ctx, testCase{
				interval: 100 * time.Millisecond,
				loopCnt:  10,
				caseNo:   caseNoNoInterrupt,
			})
			result, err := future.BlockAndLeakyGet()
			assert.Nil(t, result)
			assert.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
			assert.True(t, errors.Is(err, context.DeadlineExceeded))
		}()
	}
	wg.Wait()
	assert.Eventually(t, func() bool {
		stat := futureManager.Stat()
		fmt.Printf("active count: %d, gc count: %d\n", stat.ActiveCount, stat.GCCount)
		return stat.ActiveCount == 0 && stat.GCCount == 0
	}, 5*time.Second, 100*time.Millisecond)
	runtime.GC()
}
