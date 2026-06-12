package redisx

import (
	"slices"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestProxyKey(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
	if got := p.Key("user:1"); got != "app:user:1" {
		t.Errorf("Key() = %q, want %q", got, "app:user:1")
	}

	custom := &Proxy{prefix: "app", prefixSeparator: "."}
	if got := custom.Key("user:1"); got != "app.user:1" {
		t.Errorf("自定义连接符 Key() = %q, want %q", got, "app.user:1")
	}

	noSeparator := &Proxy{prefix: "app"}
	if got := noSeparator.Key("user:1"); got != "appuser:1" {
		t.Errorf("空连接符 Key() = %q, want %q", got, "appuser:1")
	}

	empty := &Proxy{}
	if got := empty.Key("user:1"); got != "user:1" {
		t.Errorf("空前缀 Key() = %q, want %q", got, "user:1")
	}
}

func TestProxyKeys(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}

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

func TestProxyChannel(t *testing.T) {
	t.Parallel()

	keyPrefixed := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
	if got := keyPrefixed.channel("events"); got != "events" {
		t.Errorf("channel() with key prefix = %q, want events", got)
	}

	channelPrefixed := &Proxy{channelPrefix: "app", channelPrefixSeparator: defaultChannelPrefixSeparator}
	if got := channelPrefixed.channel("events"); got != "app:events" {
		t.Errorf("channel() = %q, want app:events", got)
	}
	if got := channelPrefixed.channels([]string{"events", "jobs"}); !slices.Equal(got, []string{"app:events", "app:jobs"}) {
		t.Errorf("channels() = %v, want prefixed channels", got)
	}
}

func TestRawClient(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	p := &Proxy{rdb: rdb, prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
	if p.RawClient() != rdb {
		t.Error("RawClient() 应返回底层 *redis.Client 本身")
	}
}

func TestProxyWithPrefix(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	base := &Proxy{
		rdb:                    rdb,
		prefix:                 "base",
		prefixSeparator:        ".",
		channelPrefix:          "topic",
		channelPrefixSeparator: "/",
	}
	derived := base.WithPrefix("cache")

	if derived == base {
		t.Fatal("WithPrefix() returned receiver")
	}
	if derived.RawClient() != rdb {
		t.Fatal("WithPrefix() did not preserve RawClient")
	}
	if got := base.Key("k"); got != "base.k" {
		t.Errorf("base Key() = %q, want base.k", got)
	}
	if got := derived.Key("k"); got != "cache.k" {
		t.Errorf("derived Key() = %q, want cache.k", got)
	}
	if got := derived.channel("events"); got != "topic/events" {
		t.Errorf("derived channel() = %q, want topic/events", got)
	}
}

func TestWrapClient(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	p, err := WrapClient(rdb, "test", ":")
	if err != nil {
		t.Fatalf("WrapClient() = %v", err)
	}
	if p.RawClient() != rdb {
		t.Fatal("WrapClient() did not preserve RawClient")
	}
	if got := p.Key("k"); got != "test:k" {
		t.Errorf("Key() = %q, want test:k", got)
	}
	if got := p.channel("events"); got != "events" {
		t.Errorf("channel() = %q, want events", got)
	}

	tests := []struct {
		name      string
		rdb       *redis.Client
		prefix    string
		separator string
		wantErr   string
	}{
		{name: "nil client", rdb: nil, prefix: "test", separator: ":", wantErr: "non-nil redis client"},
		{name: "invalid prefix", rdb: rdb, prefix: "tenant[1]", separator: ":", wantErr: "key prefix"},
		{name: "invalid separator", rdb: rdb, prefix: "test", separator: "*", wantErr: "key prefix separator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := WrapClient(tt.rdb, tt.prefix, tt.separator)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("WrapClient() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestMSetOddArguments(t *testing.T) {
	t.Parallel()

	p := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
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

	p := &Proxy{prefix: "app", prefixSeparator: defaultKeyPrefixSeparator}
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
