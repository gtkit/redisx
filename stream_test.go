package redisx

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestConsumeStreamValidation(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	if err := p.ConsumeStream(t.Context(), StreamConfig{Stream: "s", Group: "g", Consumer: "c"}, nil); err == nil ||
		!strings.Contains(err.Error(), "handler is nil") {
		t.Errorf("nil handler 期望参数错误, 得到 %v", err)
	}

	h := func(redis.XMessage) error { return nil }
	tests := []struct {
		name string
		cfg  StreamConfig
		want string
	}{
		{"缺Stream", StreamConfig{Group: "g", Consumer: "c"}, "Stream is required"},
		{"缺Group", StreamConfig{Stream: "s", Consumer: "c"}, "Group is required"},
		{"缺Consumer", StreamConfig{Stream: "s", Group: "g"}, "Consumer is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := p.ConsumeStream(t.Context(), tt.cfg, h); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("期望含 %q 的错误, 得到 %v", tt.want, err)
			}
		})
	}
}

func TestXAddNilAndArgsImmutable(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app"}
	if err := p.XAdd(t.Context(), nil).Err(); err == nil || !strings.Contains(err.Error(), "args is nil") {
		t.Errorf("nil args 期望参数错误, 得到 %v", err)
	}
}
