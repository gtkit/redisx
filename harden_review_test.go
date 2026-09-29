package redisx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	if _, e := c.FencedLock(ctx, "fk", time.Minute); !errors.Is(e, ErrLockNotObtained) {
		t.Fatalf("持锁期间再获取 = %v, want ErrLockNotObtained", e)
	}

	if e := l1.Release(ctx); e != nil {
		t.Fatalf("Release #1: %v", e)
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

	ctx, cancel := context.WithCancel(t.Context())
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

	err := c.HealthCheck(t.Context())
	if err == nil {
		t.Fatal("HealthCheck = nil, want 聚合错误")
	}
	if msg := err.Error(); !strings.Contains(msg, "db=0") || !strings.Contains(msg, "db=1") {
		t.Errorf("聚合错误应同时含 db=0 与 db=1，得到: %v", msg)
	}
}

// TestIntegrationFencedLockCounterBounded 验证 fencing 计数器与锁 key 同 slot 命名
// （锁 key 追加 ":__fence__"）、带不超过 24h 的 TTL、不产生额外 key；fence 不小于
// 获取前的时间下界、与计数器值逐位一致；同毫秒内连续获取严格递增；计数器被删后
// 由时间下界接管仍严格递增。
func TestIntegrationFencedLockCounterBounded(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()
	raw := c.DefaultClient()

	before := time.Now().UnixMilli() << fenceTimeShift
	l, err := c.FencedLock(ctx, "{order:1}", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock: %v", err)
	}
	after := time.Now().UnixMilli()
	counter := l.Key() + ":__fence__"

	if l.Fence() <= before {
		t.Errorf("fence = %d, want > 时间下界 %d", l.Fence(), before)
	}
	if ms := l.Fence() >> fenceTimeShift; ms > after {
		t.Errorf("fence>>%d = %d 晚于获取完成时刻 %d", fenceTimeShift, ms, after)
	}
	if v, gerr := raw.Get(ctx, counter).Result(); gerr != nil || v != strconv.FormatInt(l.Fence(), 10) {
		t.Errorf("计数器 %s = (%q, %v), want %d（数值必须精确）", counter, v, gerr, l.Fence())
	}
	if ttl, terr := raw.PTTL(ctx, counter).Result(); terr != nil || ttl <= 0 || ttl > fenceCounterTTL {
		t.Errorf("计数器 PTTL = (%v, %v), want (0, %v]", ttl, terr, fenceCounterTTL)
	}
	if rerr := l.Release(ctx); rerr != nil {
		t.Fatalf("Release: %v", rerr)
	}

	// 命名空间内只剩这一个计数器，没有全局计数器
	var keys []string
	for k, serr := range c.ScanKeys(ctx, "*") {
		if serr != nil {
			t.Fatalf("ScanKeys: %v", serr)
		}
		keys = append(keys, k)
	}
	if len(keys) != 1 || keys[0] != "{order:1}:__fence__" {
		t.Errorf("释放后残留 key = %v, want 仅 [{order:1}:__fence__]", keys)
	}

	// 同毫秒内连续获取仍严格递增（依靠计数器 +1，而非时间）
	prev := l.Fence()
	for i := range 50 {
		li, lerr := c.FencedLock(ctx, "{order:1}", time.Minute)
		if lerr != nil {
			t.Fatalf("第 %d 次 FencedLock: %v", i, lerr)
		}
		if li.Fence() <= prev {
			t.Fatalf("第 %d 次 fence = %d, want > %d", i, li.Fence(), prev)
		}
		prev = li.Fence()
		_ = li.Release(ctx)
	}

	// 计数器被删（等价过期）后，时间下界接管，仍严格大于历史值
	if derr := raw.Del(ctx, counter).Err(); derr != nil {
		t.Fatalf("Del counter: %v", derr)
	}
	l2, err := c.FencedLock(ctx, "{order:1}", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock after counter delete: %v", err)
	}
	if l2.Fence() <= prev {
		t.Errorf("计数器删除后 fence = %d, want > %d", l2.Fence(), prev)
	}
	_ = l2.Release(ctx)
}

// TestIntegrationFencedLockLegacyCounter 验证 v1.3.0 写入的小整数无 TTL 计数器：
// 新 fence 严格大于旧值且不小于时间下界，计数器获得 TTL。
func TestIntegrationFencedLockLegacyCounter(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()
	raw := c.DefaultClient()
	counter := c.Key("legacy") + ":__fence__"

	if err := raw.Set(ctx, counter, 41, 0).Err(); err != nil {
		t.Fatalf("seed legacy counter: %v", err)
	}
	before := time.Now().UnixMilli() << fenceTimeShift
	l, err := c.FencedLock(ctx, "legacy", time.Minute)
	if err != nil {
		t.Fatalf("FencedLock: %v", err)
	}
	defer l.Release(ctx)

	if l.Fence() <= 41 || l.Fence() <= before {
		t.Errorf("fence = %d, want > max(41, %d)", l.Fence(), before)
	}
	if ttl, err := raw.PTTL(ctx, counter).Result(); err != nil || ttl <= 0 {
		t.Errorf("旧计数器获取后 PTTL = (%v, %v), want > 0", ttl, err)
	}
}

// TestIntegrationFencedAcquireSelfRetry 验证获取脚本对底层重试幂等：同 token 重发
// 返回本次已分配的同一 fence，不重复计数；不同 token 返回 -1。
func TestIntegrationFencedAcquireSelfRetry(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()
	lockKey := c.Key("retry")
	keys := []string{lockKey, lockKey + fenceKeySuffix}
	run := func(token string) int64 {
		t.Helper()
		floor := time.Now().UnixMilli() << fenceTimeShift
		n, err := fencedAcquireScript.Run(ctx, c.DefaultClient(), keys,
			token, time.Minute.Milliseconds(), floor, fenceCounterTTL.Milliseconds()).Int64()
		if err != nil {
			t.Fatalf("fencedAcquireScript(%s): %v", token, err)
		}
		return n
	}

	first := run("tokenA")
	if first <= 0 {
		t.Fatalf("首次获取 fence = %d, want > 0", first)
	}
	if again := run("tokenA"); again != first {
		t.Errorf("同 token 重发 fence = %d, want %d（不得重复计数）", again, first)
	}
	if other := run("tokenB"); other != -1 {
		t.Errorf("他人持有时 = %d, want -1", other)
	}
}

// TestIntegrationDeadLetterWideMessages 验证死信脚本字面量形式上限（92 个业务字段 +
// 4 个元数据 = 96 对）与回退 unpack 形式（93 个）两侧，死信消息字段与元数据完整。
func TestIntegrationDeadLetterWideMessages(t *testing.T) {
	t.Parallel()

	for _, fields := range []int{92, 93} {
		t.Run(strconv.Itoa(fields), func(t *testing.T) {
			t.Parallel()

			c := newTestClient(t)
			ctx := t.Context()
			values := make(map[string]any, fields)
			for i := range fields {
				values[fmt.Sprintf("f%d", i)] = i
			}
			id, err := c.XAdd(ctx, &redis.XAddArgs{Stream: "st", Values: values}).Result()
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
			runConsumeUntil(t, c, cfg, failing, func() bool { return attempts.Load() >= 1 })
			runConsumeUntil(t, c, cfg, failing, func() bool { return deadLettered.Load() >= 1 })

			dl, err := c.XRange(ctx, "dlq", "-", "+").Result()
			if err != nil || len(dl) != 1 {
				t.Fatalf("死信流 XRange = (%v, %v), want 1 条", dl, err)
			}
			got := dl[0].Values
			if len(got) != fields+4 {
				t.Errorf("死信字段数 = %d, want %d", len(got), fields+4)
			}
			for i := range fields {
				if got[fmt.Sprintf("f%d", i)] != strconv.Itoa(i) {
					t.Fatalf("字段 f%d = %v, want %d", i, got[fmt.Sprintf("f%d", i)], i)
				}
			}
			if got["_redisx_origin_id"] != id || got["_redisx_origin_stream"] != c.Key("st") {
				t.Errorf("元数据 = (%v, %v), want (%s, %s)",
					got["_redisx_origin_id"], got["_redisx_origin_stream"], id, c.Key("st"))
			}
		})
	}
}

// TestIntegrationPrefixFollowsDefaultDB 验证 Prefix() 返回默认 DB 实际生效的前缀，
// 被 WithInitDBPrefix(默认DB, x) 覆盖时与 Key() 拼出的 key 一致。
func TestIntegrationPrefixFollowsDefaultDB(t *testing.T) {
	t.Parallel()

	override := testPrefix(t) + "_d0"
	c := newTestClient(t, WithInitDBPrefix(0, override))
	if c.Prefix() != override {
		t.Errorf("Prefix() = %q, want %q", c.Prefix(), override)
	}
	if want := override + ":k"; c.Key("k") != want {
		t.Errorf("Key(k) = %q, want %q", c.Key("k"), want)
	}
}

// flipCtx 在第 n 次 Done() 调用时变为已取消，用于遍历初始化过程中的全部取消时机。
type flipCtx struct {
	context.Context

	n     int32
	calls atomic.Int32
	ch    chan struct{}
	once  sync.Once
}

func newFlipCtx(n int32) *flipCtx {
	return &flipCtx{Context: context.Background(), n: n, ch: make(chan struct{})}
}

func (f *flipCtx) Done() <-chan struct{} {
	if f.calls.Add(1) >= f.n {
		f.once.Do(func() { close(f.ch) })
	}
	return f.ch
}

func (f *flipCtx) Err() error {
	select {
	case <-f.ch:
		return context.Canceled
	default:
		return nil
	}
}

// TestIntegrationPartialInitCtxCancelAborts 验证降级模式下 ctx 取消不被当作 DB 故障：
// 无论在哪个时机取消，都不会返回"Client 与错误同时非 nil"。同时断言至少一次命中
// "默认 DB 已成功、非默认 DB 因取消失败"的路径，防止测试因取消时机偏移而空转。
//
// 实测（go-redis v9.21）目标路径落在第 4~13 次 Done() 取消，其后为完整成功；遍历
// 到 24 留足余量。关闭空闲连接预热并限制遍历次数，是为了避免大量短连接进入
// TIME_WAIT 耗尽本机临时端口，干扰同时运行的其他拨号测试。
func TestIntegrationPartialInitCtxCancelAborts(t *testing.T) {
	t.Parallel()

	addr := testRedisAddr(t)
	hitNonDefault := 0
	for n := int32(1); n <= 24; n++ {
		c, err := NewClientContext(newFlipCtx(n), WithAddr(addr), WithKeyPrefix(testPrefix(t)),
			WithMinIdleConns(0), WithInitDBs(1, 2, 3), WithAllowPartialInit())
		if c != nil {
			_ = c.Close()
			if err != nil {
				t.Fatalf("n=%d：ctx 取消后返回了可用 Client 与错误 %v，应整体中止", n, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("n=%d：Client 与错误同时为 nil", n)
		}
		for _, db := range []string{"db=1", "db=2", "db=3"} {
			if strings.Contains(err.Error(), "ping "+db+" ") {
				hitNonDefault++
				break
			}
		}
	}
	if hitNonDefault == 0 {
		t.Fatal("未命中'默认 DB 成功、非默认 DB 因取消失败'的路径，测试未覆盖目标分支")
	}
}

// TestIntegrationContextTimeoutEnabled 验证 WithContextTimeoutEnabled 透传到每个 DB，
// 且开启后 ctx deadline 能截断阻塞读：Block=3s 的 ConsumeStream 在 300ms deadline
// 附近返回 DeadlineExceeded，而不是等满 Block。
func TestIntegrationContextTimeoutEnabled(t *testing.T) {
	t.Parallel()

	plain := newTestClient(t, WithInitDBs(1))
	for db := range plain.PoolStats() {
		if rdb, _ := plain.GetClient(db); rdb.Options().ContextTimeoutEnabled {
			t.Errorf("默认 db=%d ContextTimeoutEnabled = true, want false", db)
		}
	}

	c := newTestClient(t, WithInitDBs(1), WithContextTimeoutEnabled())
	for db := range c.PoolStats() {
		if rdb, _ := c.GetClient(db); !rdb.Options().ContextTimeoutEnabled {
			t.Errorf("db=%d ContextTimeoutEnabled = false, want true", db)
		}
	}

	cfg := streamCfg("c1")
	cfg.Block = 3 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.ConsumeStream(ctx, cfg, func(redis.XMessage) error { return nil })
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("ConsumeStream 在 %v 后才返回，ctx deadline 未截断阻塞读", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ConsumeStream = %v, want context.DeadlineExceeded", err)
	}
}

// TestIntegrationStreamCommandErrorTerminates 验证消费中途 Redis 命令出错（消费组被
// 删除，XREADGROUP 返回 NOGROUP）时 ConsumeStream 立即返回携带流名的错误，
// 库内不重试。
func TestIntegrationStreamCommandErrorTerminates(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.ConsumeStream(ctx, streamCfg("c1"), func(redis.XMessage) error { return nil }) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if groups, err := c.XInfoGroups(t.Context(), "st").Result(); err == nil && len(groups) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等待消费组创建超时")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := c.XGroupDestroy(t.Context(), "st", "g1").Err(); err != nil {
		t.Fatalf("XGroupDestroy: %v", err)
	}

	select {
	case err := <-done:
		if err == nil || errors.Is(err, context.Canceled) ||
			!strings.Contains(err.Error(), "NOGROUP") || !strings.Contains(err.Error(), c.Key("st")) {
			t.Errorf("ConsumeStream = %v, want 携带流名的 NOGROUP 错误", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("消费组删除后 ConsumeStream 3s 内未返回")
	}
}
