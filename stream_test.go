package redisx

import (
	"errors"
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
		{
			"MaxDeliver为负",
			StreamConfig{Stream: "s", Group: "g", Consumer: "c", MaxDeliver: -1},
			"MaxDeliver must be >= 0",
		},
		{
			"启用死信缺死信流",
			StreamConfig{Stream: "s", Group: "g", Consumer: "c", MaxDeliver: 3},
			"DeadLetterStream is required",
		},
		{
			"死信流与源流同名",
			StreamConfig{Stream: "s", Group: "g", Consumer: "c", MaxDeliver: 3, DeadLetterStream: "s"},
			"must differ from Stream",
		},
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

	p := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
	if err := p.XAdd(t.Context(), nil).Err(); err == nil || !strings.Contains(err.Error(), "args is nil") {
		t.Errorf("nil args 期望参数错误, 得到 %v", err)
	}

	if err := p.XPendingExt(t.Context(), nil).Err(); err == nil || !strings.Contains(err.Error(), "args is nil") {
		t.Errorf("XPendingExt nil args 期望参数错误, 得到 %v", err)
	}
}

func TestSafeStreamHandlePassesBizError(t *testing.T) {
	t.Parallel()

	want := errors.New("biz failed")
	bizErr, panicErr := safeStreamHandle(func(redis.XMessage) error { return want }, redis.XMessage{})
	if !errors.Is(bizErr, want) || panicErr != nil {
		t.Errorf("safeStreamHandle = (%v, %v), want (%v, nil)", bizErr, panicErr, want)
	}

	bizErr, panicErr = safeStreamHandle(func(redis.XMessage) error { panic("boom") }, redis.XMessage{})
	if bizErr != nil || panicErr == nil || !strings.Contains(panicErr.Error(), "handler panic") {
		t.Errorf("panic 路径 = (%v, %v), 期望 panicErr 含 handler panic", bizErr, panicErr)
	}
}

// TestHandleBizErrorInvokesOnError 验证业务失败路径：不触碰 rdb（不 ACK）、
// OnError 携带原始消息与错误被调用、回调 panic 终止消费。
func TestHandleBizErrorInvokesOnError(t *testing.T) {
	t.Parallel()

	bizErr := errors.New("biz failed")
	var gotMsg redis.XMessage
	var gotErr error
	s := &streamConsumer{
		cfg: StreamConfig{OnError: func(m redis.XMessage, err error) {
			gotMsg, gotErr = m, err
		}},
		handler: func(redis.XMessage) error { return bizErr },
	}

	msg := redis.XMessage{ID: "1-0"}
	if err := s.handle(t.Context(), msg); err != nil {
		t.Fatalf("业务失败应继续消费, 得到终止错误 %v", err)
	}
	if gotMsg.ID != "1-0" || !errors.Is(gotErr, bizErr) {
		t.Errorf("OnError 收到 (%v, %v), want (1-0, %v)", gotMsg.ID, gotErr, bizErr)
	}

	s.cfg.OnError = func(redis.XMessage, error) { panic("callback boom") }
	if err := s.handle(t.Context(), msg); err == nil || !strings.Contains(err.Error(), "OnError panic") {
		t.Errorf("回调 panic 期望终止错误, 得到 %v", err)
	}
}
