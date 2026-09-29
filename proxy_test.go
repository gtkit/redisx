package redisx

import (
	"bufio"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
	derived, err := base.WithPrefix("cache")
	if err != nil {
		t.Fatalf("WithPrefix() = %v", err)
	}

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

	if _, err := base.WithPrefix("tenant[1]"); err == nil || !strings.Contains(err.Error(), "key prefix") {
		t.Fatalf("WithPrefix() invalid prefix = %v, want key prefix error", err)
	}
}

func TestWrapClient(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	p, err := WrapClient(
		rdb,
		WithProxyPrefix("test"),
		WithProxyPrefixSeparator(":"),
		WithProxyChannelPrefix("topic"),
		WithProxyChannelPrefixSeparator("."),
	)
	if err != nil {
		t.Fatalf("WrapClient() = %v", err)
	}
	if p.RawClient() != rdb {
		t.Fatal("WrapClient() did not preserve RawClient")
	}
	if got := p.Key("k"); got != "test:k" {
		t.Errorf("Key() = %q, want test:k", got)
	}
	if got := p.channel("events"); got != "topic.events" {
		t.Errorf("channel() = %q, want topic.events", got)
	}

	tests := []struct {
		name    string
		rdb     *redis.Client
		opts    []ProxyOption
		wantErr string
	}{
		{name: "nil client", rdb: nil, opts: []ProxyOption{WithProxyPrefix("test")}, wantErr: "non-nil redis client"},
		{name: "invalid prefix", rdb: rdb, opts: []ProxyOption{WithProxyPrefix("tenant[1]")}, wantErr: "key prefix"},
		{name: "invalid separator", rdb: rdb, opts: []ProxyOption{WithProxyPrefixSeparator("*")}, wantErr: "key prefix separator"},
		{name: "invalid channel prefix", rdb: rdb, opts: []ProxyOption{WithProxyChannelPrefix("topic?")}, wantErr: "channel prefix"},
		{name: "invalid channel separator", rdb: rdb, opts: []ProxyOption{WithProxyChannelPrefixSeparator("[")}, wantErr: "channel prefix separator"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := WrapClient(tt.rdb, tt.opts...)
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

func TestZRangeByScoreNilOptions(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	cmd := p.ZRangeByScore(t.Context(), "z", nil)
	if cmd.Err() == nil {
		t.Fatal("ZRangeByScore nil options 期望返回错误，实际为 nil")
	}
	if !strings.Contains(cmd.Err().Error(), "options are nil") {
		t.Errorf("ZRangeByScore nil options 错误 = %q", cmd.Err().Error())
	}
}

func TestEvalScriptNil(t *testing.T) {
	t.Parallel()

	p := &Proxy{}
	cmd := p.EvalScript(t.Context(), nil, []string{"k"})
	if cmd.Err() == nil {
		t.Fatal("EvalScript nil script 期望返回错误，实际为 nil")
	}
	if !strings.Contains(cmd.Err().Error(), "script is nil") {
		t.Errorf("EvalScript nil script 错误 = %q", cmd.Err().Error())
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

// silentSubscribeServer 启动一个最小 RESP 服务端：握手命令正常应答（HELLO 回 Redis
// 错误让 go-redis 降级到 RESP2，CLIENT 回 +OK），对 SUBSCRIBE / PSUBSCRIBE 不回复。
// 只"接受连接但不回复"的服务端会让超时发生在握手阶段，测不到订阅确认。
func silentSubscribeServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go serveSilentSubscribe(conn)
		}
	}()
	return ln.Addr().String()
}

func serveSilentSubscribe(conn net.Conn) {
	rd := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(rd)
		if err != nil {
			return
		}
		var reply string
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			reply = "-ERR unknown command 'HELLO'\r\n"
		case "SUBSCRIBE", "PSUBSCRIBE":
			continue
		default:
			reply = "+OK\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func readRESPArray(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || n <= 0 {
		return nil, errors.New("bad array header")
	}
	args := make([]string, n)
	for i := range args {
		if _, err := rd.ReadString('\n'); err != nil { // $len
			return nil, err
		}
		v, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args[i] = strings.TrimRight(v, "\r\n")
	}
	return args, nil
}

// TestConsumeConfirmationBounded 验证订阅确认受 ReadTimeout 约束：服务端不回确认、
// ctx 只可取消无 deadline 时，Consume / ConsumePattern 按读超时返回而不永久阻塞。
func TestConsumeConfirmationBounded(t *testing.T) {
	t.Parallel()

	for name, consume := range map[string]func(context.Context, *Proxy) error{
		"Consume": func(ctx context.Context, p *Proxy) error {
			return p.Consume(ctx, func(*redis.Message) {}, "ch")
		},
		"ConsumePattern": func(ctx context.Context, p *Proxy) error {
			return p.ConsumePattern(ctx, func(*redis.Message) {}, "ch:*")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rdb := redis.NewClient(&redis.Options{
				Addr:        silentSubscribeServer(t),
				ReadTimeout: 300 * time.Millisecond,
				MaxRetries:  -1,
			})
			t.Cleanup(func() { _ = rdb.Close() })
			p, err := WrapClient(rdb)
			if err != nil {
				t.Fatalf("WrapClient: %v", err)
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- consume(ctx, p) }()

			select {
			case err := <-done:
				var ne net.Error
				if !errors.As(err, &ne) || !ne.Timeout() {
					t.Fatalf("返回 %v，期望订阅确认阶段的读超时错误", err)
				}
			case <-time.After(3 * time.Second):
				cancel() // 旧实现下取消同样无效，这里只为不留孤儿 goroutine
				t.Fatal("订阅确认未受 ReadTimeout 约束，3s 内未返回")
			}
		})
	}
}
