package redisx

import (
	"strings"
	"testing"
	"time"
)

func TestTryLockTTLValidation(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := p.TryLock(t.Context(), "k", ttl); err == nil || !strings.Contains(err.Error(), "ttl must be positive") {
			t.Errorf("ttl=%v 期望参数错误, 得到 %v", ttl, err)
		}
	}

	l := &Lock{}
	if err := l.Refresh(t.Context(), 0); err == nil || !strings.Contains(err.Error(), "ttl must be positive") {
		t.Errorf("Refresh ttl=0 期望参数错误, 得到 %v", err)
	}
}

func TestLockTTLRejectsSubMillisecond(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	if _, err := p.TryLock(t.Context(), "k", time.Nanosecond); err == nil ||
		!strings.Contains(err.Error(), "at least 1ms") {
		t.Fatalf("TryLock sub-ms ttl = %v, want minimum ttl error", err)
	}

	l := &Lock{}
	if err := l.Refresh(t.Context(), time.Nanosecond); err == nil ||
		!strings.Contains(err.Error(), "at least 1ms") {
		t.Fatalf("Refresh sub-ms ttl = %v, want minimum ttl error", err)
	}
}
