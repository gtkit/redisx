package redisx

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPrefixKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		separator string
		key       string
		want      string
	}{
		{name: "空前缀", prefix: "", separator: ":", key: "user:1", want: "user:1"},
		{name: "普通前缀", prefix: "app", separator: ":", key: "user:1", want: "app:user:1"},
		{name: "自定义连接符", prefix: "app", separator: ".", key: "user:1", want: "app.user:1"},
		{name: "空连接符", prefix: "app", separator: "", key: "user:1", want: "appuser:1"},
		{name: "多级前缀", prefix: "app:demo", separator: ":", key: "user:1", want: "app:demo:user:1"},
		{name: "空key", prefix: "app", separator: ":", key: "", want: "app:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prefixKey(tt.prefix, tt.separator, tt.key); got != tt.want {
				t.Errorf("prefixKey(%q, %q, %q) = %q, want %q", tt.prefix, tt.separator, tt.key, got, tt.want)
			}
		})
	}
}

func TestNewClientValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "缺少Addr",
			opts:    nil,
			wantErr: "addr is required",
		},
		{
			name:    "默认DB为负数",
			opts:    []Option{WithAddr("127.0.0.1:6379"), WithDB(-1)},
			wantErr: "invalid default db -1",
		},
		{
			name:    "InitDBs含负数",
			opts:    []Option{WithAddr("127.0.0.1:6379"), WithInitDBs(0, -2)},
			wantErr: "invalid db -2 in init list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewClient(tt.opts...)
			if err == nil {
				_ = c.Close()
				t.Fatalf("NewClient() 期望返回错误，实际为 nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("NewClient() 错误 = %q, 期望包含 %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// recordHook 是只计数 ProcessHook 调用次数的测试 Hook。
type recordHook struct {
	processed atomic.Int32
}

func (h *recordHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *recordHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.processed.Add(1)
		return next(ctx, cmd)
	}
}

func (h *recordHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestAddHookInstallsOnAllClients 验证 AddHook 装到全部已初始化 DB：
// 命令即使因连接失败而报错，ProcessHook 仍会被调用，可据此计数。
func TestAddHookInstallsOnAllClients(t *testing.T) {
	t.Parallel()

	newDeadClient := func(db int) *redis.Client {
		// 127.0.0.1:1 无监听服务，连接立即被拒绝
		return redis.NewClient(&redis.Options{
			Addr:        "127.0.0.1:1",
			DB:          db,
			DialTimeout: 500 * time.Millisecond,
			MaxRetries:  -1, // 关闭重试，保证每条命令只触发一次 ProcessHook
		})
	}
	c := &Client{clients: map[int]*redis.Client{0: newDeadClient(0), 1: newDeadClient(1)}}
	t.Cleanup(func() { _ = c.Close() })

	hook := &recordHook{}
	c.AddHook(hook)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for db, rdb := range c.clients {
		if err := rdb.Ping(ctx).Err(); err == nil {
			t.Fatalf("db=%d 对无监听地址的 Ping 期望失败，实际成功", db)
		}
	}

	if got := hook.processed.Load(); got != 2 {
		t.Errorf("ProcessHook 调用次数 = %d, want 2", got)
	}
}

// TestAddHookNil 验证 nil hook 被静默忽略，不 panic 也不影响命令执行链。
func TestAddHookNil(t *testing.T) {
	t.Parallel()

	c := &Client{clients: map[int]*redis.Client{}}
	c.AddHook(nil) // 不应 panic
}

// TestPoolStats 验证按 DB 编号返回每个已初始化客户端的池统计。
func TestPoolStats(t *testing.T) {
	t.Parallel()

	c := &Client{clients: map[int]*redis.Client{
		0: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		2: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
	}}
	t.Cleanup(func() { _ = c.Close() })

	stats := c.PoolStats()
	if len(stats) != 2 {
		t.Fatalf("PoolStats 条目数 = %d, want 2", len(stats))
	}
	for _, db := range []int{0, 2} {
		if stats[db] == nil {
			t.Errorf("db=%d 的池统计为 nil", db)
		}
	}
}

// TestNewClientPartialInitDefaultDBMustSucceed 验证降级模式下 DefaultDB
// 失败仍整体失败：Client 级快捷方法绑定默认 DB，缺它即不可用。
func TestNewClientPartialInitDefaultDBMustSucceed(t *testing.T) {
	t.Parallel()

	// 127.0.0.1:1 无监听服务，连接立即被拒绝
	c, err := NewClient(
		WithAddr("127.0.0.1:1"),
		WithAllowPartialInit(),
		WithInitDBs(1),
		WithDialTimeout(500*time.Millisecond),
	)
	if err == nil {
		_ = c.Close()
		t.Fatal("NewClient() 期望返回错误，实际为 nil")
	}
	if c != nil {
		t.Errorf("DefaultDB 失败时 Client 应为 nil，实际 = %v", c)
	}
	if !strings.Contains(err.Error(), "ping db=0") {
		t.Errorf("NewClient() 错误 = %q, 期望包含默认 DB 的 ping 失败", err.Error())
	}
}
