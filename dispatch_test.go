package bus

import (
	"errors"
	"testing"
	"time"

	"github.com/infrago/infra"
)

func TestIsRetryableDispatchError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "nil", err: nil, retryable: false},
		{name: "common error", err: errors.New("boom"), retryable: true},
		{name: "retry result", err: DispatchError{res: infra.Retry}, retryable: true},
		{name: "fail result", err: DispatchError{res: infra.Fail}, retryable: true},
		{name: "fail retry flag", err: DispatchError{res: infra.Fail.With("x").Retry()}, retryable: true},
		{name: "invalid retry flag", err: DispatchError{res: infra.Invalid.With("x").Retry()}, retryable: true},
		{name: "invalid result", err: DispatchError{res: infra.Invalid}, retryable: false},
		{name: "denied result", err: DispatchError{res: infra.Denied}, retryable: false},
		{name: "unsigned result", err: DispatchError{res: infra.Unsigned}, retryable: false},
		{name: "unauthed result", err: DispatchError{res: infra.Unauthed}, retryable: false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryableDispatchError(c.err); got != c.retryable {
				t.Fatalf("retryable=%v, got=%v", c.retryable, got)
			}
		})
	}
}

func TestDispatchFinal(t *testing.T) {
	retries := []time.Duration{3 * time.Second, 10 * time.Second, 30 * time.Second}
	cases := []struct {
		attempt int
		final   bool
	}{
		{attempt: 1, final: false},
		{attempt: 2, final: false},
		{attempt: 3, final: false},
		{attempt: 4, final: true},
	}
	for _, c := range cases {
		if got := DispatchFinal(retries, c.attempt); got != c.final {
			t.Fatalf("attempt=%d final=%v got=%v", c.attempt, c.final, got)
		}
	}
}

func TestDispatchRetryDelay(t *testing.T) {
	retries := []time.Duration{3 * time.Second, 10 * time.Second, 30 * time.Second}
	cases := []struct {
		attempt int
		delay   time.Duration
		ok      bool
	}{
		{attempt: 1, delay: 3 * time.Second, ok: true},
		{attempt: 2, delay: 10 * time.Second, ok: true},
		{attempt: 3, delay: 30 * time.Second, ok: true},
		{attempt: 4, delay: 0, ok: false},
	}
	for _, c := range cases {
		delay, ok := DispatchRetryDelay(retries, c.attempt)
		if ok != c.ok || delay != c.delay {
			t.Fatalf("attempt=%d delay=%v ok=%v gotDelay=%v gotOK=%v", c.attempt, c.delay, c.ok, delay, ok)
		}
	}
}
