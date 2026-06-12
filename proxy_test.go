package redisx

import (
	"slices"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestProxyKey(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app"}
	if got := p.Key("user:1"); got != "app:user:1" {
		t.Errorf("Key() = %q, want %q", got, "app:user:1")
	}

	empty := &Proxy{}
	if got := empty.Key("user:1"); got != "user:1" {
		t.Errorf("空前缀 Key() = %q, want %q", got, "user:1")
	}
}

func TestProxyKeys(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app"}

	got := p.keys([]string{"a", "b"})
	want := []string{"app:a", "app:b"}
	if !slices.Equal(got, want) {
		t.Errorf("keys() = %v, want %v", got, want)
	}

	if got := p.keys(nil); len(got) != 0 {
		t.Errorf("keys(nil) = %v, want 空", got)
	}

	exported := p.Keys("a", "b")
	if !slices.Equal(exported, want) {
		t.Errorf("Keys() = %v, want %v", exported, want)
	}

	if got := p.Keys(); len(got) != 0 {
		t.Errorf("Keys() 无参 = %v, want 空", got)
	}
}

func TestRawClient(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	p := &Proxy{rdb: rdb, prefix: "app"}
	if p.RawClient() != rdb {
		t.Error("RawClient() 应返回底层 *redis.Client 本身")
	}
}

func TestMSetOddArguments(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app"}
	cmd := p.MSet(t.Context(), "k1", "v1", "k2")
	if cmd.Err() == nil {
		t.Fatal("MSet 奇数参数期望返回错误，实际为 nil")
	}
	if !strings.Contains(cmd.Err().Error(), "even number of arguments") {
		t.Errorf("MSet 错误 = %q, 期望包含参数个数说明", cmd.Err().Error())
	}
}

func TestMSetNonStringKey(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app"}
	cmd := p.MSet(t.Context(), []byte("k1"), "v1")
	if cmd.Err() == nil {
		t.Fatal("MSet 非 string key 期望返回错误，实际为 nil")
	}
	if !strings.Contains(cmd.Err().Error(), "must be string") || !strings.Contains(cmd.Err().Error(), "[]uint8") {
		t.Errorf("MSet 错误 = %q, 期望包含类型说明", cmd.Err().Error())
	}
}

func TestConsumeArgValidation(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	if err := p.Consume(t.Context(), nil, "ch"); err == nil || !strings.Contains(err.Error(), "handler is nil") {
		t.Errorf("nil handler 期望参数错误, 得到 %v", err)
	}
	if err := p.Consume(t.Context(), func(*redis.Message) {}); err == nil || !strings.Contains(err.Error(), "at least one channel") {
		t.Errorf("空频道期望参数错误, 得到 %v", err)
	}

	if err := p.ConsumePattern(t.Context(), nil, "events:*"); err == nil || !strings.Contains(err.Error(), "handler is nil") {
		t.Errorf("ConsumePattern nil handler 期望参数错误, 得到 %v", err)
	}
	if err := p.ConsumePattern(t.Context(), func(*redis.Message) {}); err == nil || !strings.Contains(err.Error(), "at least one pattern") {
		t.Errorf("ConsumePattern 空模式期望参数错误, 得到 %v", err)
	}
}
