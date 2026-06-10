package redisx

import (
	"errors"
	"fmt"
	"testing"
)

func TestInitErrorStableOrderAndChain(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("connection refused")
	ie := &InitError{Failed: map[int]error{
		3: fmt.Errorf("redis: ping db=3: %w", sentinel),
		1: errors.New("redis: ping db=1: timeout"),
		2: errors.New("redis: ping db=2: auth failed"),
	}}

	want := "redisx: partial init failed: redis: ping db=1: timeout; redis: ping db=2: auth failed; redis: ping db=3: connection refused"
	for range 3 {
		if got := ie.Error(); got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	}

	var asTarget *InitError
	var err error = ie
	if !errors.As(err, &asTarget) || len(asTarget.Failed) != 3 {
		t.Fatalf("errors.As 提取失败: %v", asTarget)
	}
	if !errors.Is(err, sentinel) {
		t.Fatal("errors.Is 未穿透到底层错误")
	}
}
