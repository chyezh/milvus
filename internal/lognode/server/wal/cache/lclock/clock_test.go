package lclock

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestLogicClock(t *testing.T) {
	m := NewLogicClockManager()
	info := m.ConfirmedInfo()
	assert.Equal(t, int64(0), info.LAC)
	assert.Equal(t, int64(0), info.LFC)

	unconfirmedClocks := make([]UnconfirmedLogicClock, 0)
	for i := 0; i < 10; i++ {
		c, err := m.AllocateClock(context.Background())
		assert.NoError(t, err)
		unconfirmedClocks = append(unconfirmedClocks, c)
	}
	info = m.ConfirmedInfo()
	assert.Equal(t, int64(0), info.LAC)
	assert.Equal(t, int64(0), info.LFC)

	for i := 0; i < 10; i++ {
		unconfirmedClocks[i].Confirm(nil)
		info := m.ConfirmedInfo()
		if i == 9 {
			assert.Equal(t, int64(21), info.LAC)
		} else {
			assert.Equal(t, int64(i+1), info.LAC)
		}
		assert.Equal(t, int64(0), info.LFC)
	}
	info = m.ConfirmedInfo()
	assert.Equal(t, int64(21), info.LAC)
	assert.Equal(t, int64(0), info.LFC)
}

func TestLogicClockWithError(t *testing.T) {
	m := NewLogicClockManager()
	info := m.ConfirmedInfo()
	assert.Equal(t, int64(0), info.LAC)
	assert.Equal(t, int64(0), info.LFC)

	unconfirmedClocks := make([]UnconfirmedLogicClock, 0)
	for i := 0; i < 10; i++ {
		c, err := m.AllocateClock(context.Background())
		assert.NoError(t, err)
		unconfirmedClocks = append(unconfirmedClocks, c)
	}
	for i := 0; i < 5; i++ {
		unconfirmedClocks[i].Confirm(nil)
		info := m.ConfirmedInfo()
		assert.Equal(t, int64(i+1), info.LAC)
		assert.Equal(t, int64(0), info.LFC)
	}
	confirmedClock := unconfirmedClocks[5].Confirm(errors.New("test"))
	info = m.ConfirmedInfo()
	assert.Equal(t, int64(6), info.LAC)
	assert.Equal(t, int64(confirmedClock), info.LFC)
	for i := 6; i < 10; i++ {
		unconfirmedClocks[i].Confirm(nil)
		info := m.ConfirmedInfo()
		if i == 9 {
			assert.Equal(t, int64(21), info.LAC)
		} else {
			assert.Equal(t, int64(i+1), info.LAC)
		}
		assert.Equal(t, int64(confirmedClock), info.LFC)
	}
}

func TestConcurrency(t *testing.T) {
	m := NewLogicClockManager()
	errConfirmed := atomic.NewInt64(0)
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			clock, err := m.AllocateClock(context.Background())
			assert.NoError(t, err)
			time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)
			if clock.Clock() == 5 {
				errConfirmed.Store(clock.Confirm(errors.New("test")))
			} else {
				clock.Confirm(nil)
			}
		}()
	}
	assert.Eventually(t, func() bool {
		info := m.ConfirmedInfo()
		return errConfirmed.Load() == info.LFC && info.LAC == 21
	}, time.Second, 10*time.Millisecond)
	wg.Wait()
	info := m.ConfirmedInfo()
	assert.Equal(t, errConfirmed.Load(), info.LFC)
	assert.Equal(t, int64(21), info.LAC)
}

func TestFencing(t *testing.T) {
	m := NewLogicClockManager()
	{
		// There's 10 concurrent, so fencing happens.
		unconfirmedClocks := make([]UnconfirmedLogicClock, 0)
		for i := 0; i < 10; i++ {
			clock, err := m.AllocateClock(context.Background())
			assert.NoError(t, err)
			unconfirmedClocks = append(unconfirmedClocks, clock)
		}
		errConfirmedClock := unconfirmedClocks[5].Confirm(errors.New("test"))

		// new clock allocation should be blocked until fencing is done.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := m.AllocateClock(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		ch := make(chan struct{}, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			clock, err := m.AllocateClock(ctx)
			assert.NoError(t, err)
			clock.Confirm(nil)
			ch <- struct{}{}
		}()

		for i := 0; i < 10; i++ {
			if i != 5 {
				unconfirmedClocks[i].Confirm(nil)
			}
		}
		<-ch
		info := m.ConfirmedInfo()
		assert.Equal(t, errConfirmedClock, info.LFC)
		assert.Equal(t, int64(24), info.LAC)
	}

	{
		// no fencing if there's only one concurrent.
		clock, err := m.AllocateClock(context.Background())
		assert.NoError(t, err)
		clock.Confirm(errors.New("test"))

		clock, err = m.AllocateClock(context.Background())
		assert.NoError(t, err)
		clock.Confirm(nil)
	}
}
