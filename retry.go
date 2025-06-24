// Simple package for retries with exponential backoff.
package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/facebookgo/clock"
)

var RetriesExceeded = fmt.Errorf("retries exceeded")

// Function used to determine if an error should be retried or not.
// Returns nil to indicate the operation should be retried.
// Since the predicate must parse the error, it is allowed to return
// a wrapped error to avoid having to parse the error multiple times.
type RetryableErrorPredicate func(error) error

// RetryCounter provides a counter for tracking retries
type RetryCounter struct {
	maxTries        int
	delay           time.Duration
	delayMultiplier float32
	jitter          float32
	tries           int
}

// RetryPolicy allows retrying a function when specific errors occur.
type RetryPolicy struct {
	RetryCounter
	shouldRetry RetryableErrorPredicate
	clock       clock.Clock
}

const ConnectionResetError = "connection reset by peer"
const InternalServerError = "InternalServerError"
const ThrottlingError = "Throttling"

func IsRetryable(err error) error {
	errtext := err.Error()
	if strings.Contains(errtext, ConnectionResetError) ||
		strings.Contains(errtext, InternalServerError) ||
		strings.Contains(errtext, ThrottlingError) {
		return nil
	}
	return err
}

// Delay returns the duration of the wait before the next retry attempt.
// RetriesExceeded is returned if there have been too many attempts.
func Delay(try int, maxTries int, delay time.Duration, delayMultiplier float32) (nextDelay time.Duration, err error) {
	c := NewCounter(maxTries, delay, delayMultiplier)
	for i := 0; i < try; i++ {
		if nextDelay, err = c.Delay(); err != nil {
			return
		}
	}
	return
}

// Retry will retry a given function if it returns an retryable error,
// until the maximum number of tries has been exhausted. If the
// function returns more than just an error code, the caller should
// use a closure to capture the other return values. Example:
//
//	var foo ImportantStuff
//	var err error
//
//	err = retry.Retry(... params ..., func() error {
//	    err, foo = ImportantFunction()
//	    return err
//	})
//
//	if err != nil {
//	    return err
//	}
//
//	DoMoreStuffWithFoo(foo)
func Retry(maxTries int, delay time.Duration, delayMultiplier float32, shouldRetry RetryableErrorPredicate, f func() error) (err error) {
	r := NewRetryPolicy(maxTries, delay, delayMultiplier, shouldRetry)
	return r.Retry(f)
}

// NewCounter creates a RetryCounter with the given parameters.
func NewCounter(maxTries int, delay time.Duration, delayMultiplier float32) *RetryCounter {
	return &RetryCounter{
		maxTries:        maxTries,
		delay:           delay,
		delayMultiplier: delayMultiplier,
		jitter:          0.1,
	}
}

// New creates a RetryPolicy with the given parameters.
func NewRetryPolicy(maxTries int, delay time.Duration, delayMultiplier float32, shouldRetry RetryableErrorPredicate) *RetryPolicy {
	return &RetryPolicy{
		RetryCounter: RetryCounter{
			maxTries:        maxTries,
			delay:           delay,
			delayMultiplier: delayMultiplier,
			jitter:          0.1,
		},
		shouldRetry: shouldRetry,
		clock:       clock.New(),
	}
}

// Delay returns the duration of the wait before the next retry attempt.
// RetriesExceeded is returned if there have been too many attempts.
func (rc *RetryCounter) Delay() (delay time.Duration, err error) {
	rc.tries++
	if rc.tries >= rc.maxTries {
		return 0, RetriesExceeded
	}
	delay = rc.delay
	// Without jitter it's possible for multiple updates to
	// keep retrying at exactly the same time and keep
	// failing. This should be enough to knock them a bit out
	// of sync.
	jitter := rand.Int63n(int64(float32(delay) * rc.jitter))

	if rand.Int63n(1000) > 500 {
		jitter *= -1
	}

	rc.delay = time.Duration(int64(float32(delay) * rc.delayMultiplier))
	return delay + time.Duration(jitter), nil
}

// Retry will retry a given function if it returns an retryable error,
// until the maximum number of tries has been exhausted. If the
// function returns more than just an error code, the caller should
// use a closure to capture the other return values. Example:
//
//	retryPolicy := retry.New(...params...)
//	var foo ImportantStuff
//	var err error
//
//	err = retryPolicy.Retry(func() error {
//	    err, foo = ImportantFunction()
//	    return err
//	})
//
//	if err != nil {
//	    return err
//	}
//
//	DoMoreStuffWithFoo(foo)
func (rp *RetryPolicy) Retry(f func() error) (err error) {
	for {
		if err = f(); err == nil {
			return nil
		}
		if re := rp.shouldRetry(err); re != nil {
			return re
		}
		if delay, e := rp.Delay(); e != nil {
			return err
		} else {
			rp.clock.Sleep(delay)
		}
	}
}
