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
