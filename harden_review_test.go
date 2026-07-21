package redisx

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestIntegrationAcquireLockIdempotentRetry 覆盖获取脚本三分支：新获取、
// 同 token 自获取（模拟底层重试残留）幂等、他人持有返回 false。
func TestIntegrationAcquireLockIdempotentRetry(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()
	p := c.Proxy
	key := p.key("acq")

	if ok, err := acquireLock(ctx, p.rdb, key, "tokenA", time.Minute); err != nil || !ok {
		t.Fatalf("首次获取 = (%v, %v), want (true, nil)", ok, err)
	}
	// 同 token 再次执行（等价底层重试重发）仍视为获得
	if ok, err := acquireLock(ctx, p.rdb, key, "tokenA", time.Minute); err != nil || !ok {
		t.Fatalf("同 token 自获取 = (%v, %v), want (true, nil)", ok, err)
	}
	// 不同 token → 被他人持有
	if ok, err := acquireLock(ctx, p.rdb, key, "tokenB", time.Minute); err != nil || ok {
		t.Fatalf("他人持有 = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestIntegrationLockTTLNoExpiry 验证锁被移除过期时间后 Lock.TTL 返回 ErrLockNoExpiry。
func TestIntegrationLockTTLNoExpiry(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	lock, err := c.TryLock(ctx, "noexp", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	// 制造契约外的永久锁：移除过期时间
	if err := c.DefaultClient().Persist(ctx, lock.Key()).Err(); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if _, err := lock.TTL(ctx); !errors.Is(err, ErrLockNoExpiry) {
		t.Fatalf("TTL after persist = %v, want ErrLockNoExpiry", err)
	}
	_ = lock.Release(ctx)
}

// TestIntegrationFencedLock 覆盖 fencing token 单调、互斥、释放后令牌前进、
// 释放不误删他人锁。
func TestIntegrationFencedLock(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	l1, err := c.FencedLock(ctx, "fk", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock #1: %v", err)
	}
	if l1.Fence() <= 0 {
		t.Fatalf("fence #1 = %d, want > 0", l1.Fence())
	}

	// 互斥：持有期间他人获取失败
	if _, err := c.FencedLock(ctx, "fk", time.Minute); !errors.Is(err, ErrLockNotObtained) {
		t.Fatalf("持锁期间再获取 = %v, want ErrLockNotObtained", err)
	}

	if err := l1.Release(ctx); err != nil {
		t.Fatalf("Release #1: %v", err)
	}

	// 释放后再获取，fence 严格前进
	l2, err := c.FencedLock(ctx, "fk", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock #2: %v", err)
	}
	if l2.Fence() <= l1.Fence() {
		t.Fatalf("fence #2 = %d, want > %d", l2.Fence(), l1.Fence())
	}
	_ = l2.Release(ctx)

	// 释放不误删他人锁：手动改写锁值模拟他人重新持有
	l3, err := c.FencedLock(ctx, "fk2", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock fk2: %v", err)
	}
	if err := c.DefaultClient().Set(ctx, l3.Key(), "someone-else", time.Minute).Err(); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if err := l3.Release(ctx); !errors.Is(err, ErrLockLost) {
		t.Fatalf("Release after takeover = %v, want ErrLockLost", err)
	}
}

// TestIntegrationDeadLetterMetadataNotPolluted 验证业务字段冒充 _redisx_* 元数据时，
// 死信流中的元数据取库注入的真实值，不被污染。
func TestIntegrationDeadLetterMetadataNotPolluted(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	// 业务消息故意带一个冒充元数据的字段
	id, err := c.XAdd(ctx, &redis.XAddArgs{
		Stream: "st",
		Values: map[string]any{"v": "poison", "_redisx_origin_id": "BOGUS"},
	}).Result()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	var attempts, deadLettered atomic.Int32
	cfg := streamCfg("c1")
	cfg.MaxDeliver = 1
	cfg.DeadLetterStream = "dlq"
	cfg.OnError = func(_ redis.XMessage, e error) {
		if errors.Is(e, ErrMessageDeadLettered) {
			deadLettered.Add(1)
		}
	}
	failing := func(redis.XMessage) error {
		attempts.Add(1)
		return errors.New("boom")
	}
	// 第 1 轮投递计数=1，handler 失败；第 2 轮续传计数=2 > MaxDeliver → 死信
	runConsumeUntil(t, c, cfg, failing, func() bool { return attempts.Load() >= 1 })
	runConsumeUntil(t, c, cfg, failing, func() bool { return deadLettered.Load() >= 1 })

	dlMsgs, err := c.XRange(ctx, "dlq", "-", "+").Result()
	if err != nil || len(dlMsgs) != 1 {
		t.Fatalf("死信流 XRange = (%v, %v), want 1 条", dlMsgs, err)
	}
	if got := dlMsgs[0].Values["_redisx_origin_id"]; got != id {
		t.Errorf("_redisx_origin_id = %v, want %s（元数据必须压过同名业务字段）", got, id)
	}
}

// TestIntegrationConcurrentInitMultiDB 验证多 DB 并发初始化后全部可用。
func TestIntegrationConcurrentInitMultiDB(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, WithInitDBs(1, 2, 3))
	for _, db := range []int{0, 1, 2, 3} {
		if _, err := c.SelectDB(db); err != nil {
			t.Errorf("SelectDB(%d) = %v, want nil", db, err)
		}
	}
	if err := c.HealthCheck(t.Context()); err != nil {
		t.Errorf("HealthCheck = %v, want nil", err)
	}
}

// TestIntegrationConcurrentInitPartialFailure 验证降级模式下并发初始化中失败的
// 非默认 DB 聚合为 *InitError，且按编号可提取；成功的 DB 仍可用。
func TestIntegrationConcurrentInitPartialFailure(t *testing.T) {
	t.Parallel()

	addr := testRedisAddr(t)
	// DB 99 超出默认 databases 范围（默认 16 个库）→ 失败；默认 DB0 成功 → 降级返回
	c, err := NewClient(
		WithAddr(addr),
		WithKeyPrefix(testPrefix(t)),
		WithInitDBs(1, 2),
		WithInitDBPrefix(99, "x"),
		WithAllowPartialInit(),
	)
	if c == nil {
		t.Fatalf("降级模式应返回可用 Client，err=%v", err)
	}
	defer c.Close()

	var initErr *InitError
	if !errors.As(err, &initErr) {
		t.Fatalf("err = %v, want *InitError", err)
	}
	if _, ok := initErr.Failed[99]; !ok {
		t.Errorf("InitError.Failed 缺 DB 99: %v", initErr.Failed)
	}
	for _, db := range []int{0, 1, 2} {
		if _, e := c.SelectDB(db); e != nil {
			t.Errorf("SelectDB(%d) = %v, want nil", db, e)
		}
	}
}

// TestNewClientContextCanceled 验证传入已取消 ctx 时初始化中止并返回错误、不返回 Client。
func TestNewClientContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	c, err := NewClientContext(ctx, WithAddr("127.0.0.1:6379"))
	if err == nil || c != nil {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("NewClientContext(canceled) = (%v, %v), want (nil, non-nil err)", c, err)
	}
}

// TestHealthCheckAggregatesAllFailures 验证并发健康检查聚合全部失败且不短路。
func TestHealthCheckAggregatesAllFailures(t *testing.T) {
	t.Parallel()

	opt := func() *redis.Options {
		return &redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1}
	}
	c := &Client{clients: map[int]*redis.Client{
		0: redis.NewClient(opt()),
		1: redis.NewClient(opt()),
	}}
	defer c.Close()

	err := c.HealthCheck(context.Background())
	if err == nil {
		t.Fatal("HealthCheck = nil, want 聚合错误")
	}
	if msg := err.Error(); !strings.Contains(msg, "db=0") || !strings.Contains(msg, "db=1") {
		t.Errorf("聚合错误应同时含 db=0 与 db=1，得到: %v", msg)
	}
}
