package redisx

import (
	"slices"
	"strings"
	"testing"
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
