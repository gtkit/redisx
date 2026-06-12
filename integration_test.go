package redisx

// 真实 Redis 集成测试。
//
// 通过环境变量 REDISX_TEST_ADDR 指定 Redis 地址（默认 127.0.0.1:6379），
// 不可达时整组 skip。每个测试使用从 t.Name() 派生的唯一 key 前缀，
// 结束时 DelByPattern 清理，互不干扰且不污染共享 Redis。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedisAddr(tb testing.TB) string {
	tb.Helper()
	addr := os.Getenv("REDISX_TEST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		tb.Skipf("真实 Redis 不可达 (%s)，跳过集成测试: %v", addr, err)
	}
	_ = conn.Close()
	return addr
}

// testPrefix 从测试名派生唯一 key 前缀。
func testPrefix(tb testing.TB) string {
	tb.Helper()
	return "redisx_it:" + strings.NewReplacer("/", "_", " ", "_", "#", "_").Replace(tb.Name())
}

// newTestClient 创建指向真实 Redis 的客户端，注入唯一前缀并注册清理。
func newTestClient(tb testing.TB, opts ...Option) *Client {
	tb.Helper()
	addr := testRedisAddr(tb)
	all := append([]Option{WithAddr(addr), WithKeyPrefix(testPrefix(tb))}, opts...)
	c, err := NewClient(all...)
	if err != nil {
		tb.Fatalf("NewClient(%s) 失败: %v", addr, err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:usetesting // Cleanup 执行时 tb.Context() 已被取消

		defer cancel()
		for _, p := range c.proxies {
			_, _ = p.DelByPattern(ctx, "*")
		}
		_ = c.Close()
	})
	return c
}

// ──────────────────────────────────────────
//  Client 生命周期
// ──────────────────────────────────────────

func TestIntegrationClientLifecycle(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, WithInitDBs(1), WithDBConfig(2, testPrefix(t)+"_db2"))
	ctx := t.Context()

	if err := c.HealthCheck(ctx); err != nil {
		t.Errorf("HealthCheck() = %v, want nil", err)
	}

	if c.Prefix() != testPrefix(t) {
		t.Errorf("Prefix() = %q, want %q", c.Prefix(), testPrefix(t))
	}

	for _, db := range []int{0, 1, 2} {
		if _, err := c.SelectDB(db); err != nil {
			t.Errorf("SelectDB(%d) = %v, want nil", db, err)
		}
		if rdb, ok := c.GetClient(db); !ok || rdb == nil {
			t.Errorf("GetClient(%d) = (%v, %v), want 非nil", db, rdb, ok)
		}
	}
	if _, err := c.SelectDB(9); err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("SelectDB(9) = %v, 期望未初始化错误", err)
	}
	if c.MustSelectDB(1) == nil {
		t.Error("MustSelectDB(1) 返回 nil")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("MustSelectDB(9) 期望 panic")
			}
		}()
		c.MustSelectDB(9)
	}()

	if c.DefaultClient() == nil {
		t.Error("DefaultClient() 返回 nil")
	}

	stats := c.PoolStats()
	if len(stats) != 3 {
		t.Errorf("PoolStats 条目数 = %d, want 3", len(stats))
	}

	// per-DB 前缀隔离：DB2 写入的 key 实际带独立前缀
	p2 := c.MustSelectDB(2)
	if err := p2.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("DB2 Set 失败: %v", err)
	}
	raw, _ := c.GetClient(2)
	if got, err := raw.Get(ctx, testPrefix(t)+"_db2:k").Result(); err != nil || got != "v" {
		t.Errorf("DB2 原始 key 读取 = (%q, %v), want (v, nil)", got, err)
	}
}

func TestIntegrationHealthCheckFailure(t *testing.T) {
	t.Parallel()

	// 127.0.0.1:1 无监听服务
	c := &Client{clients: map[int]*redis.Client{
		0: redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 300 * time.Millisecond, MaxRetries: -1}),
	}}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.HealthCheck(t.Context()); err == nil || !strings.Contains(err.Error(), "health check failed") {
		t.Errorf("HealthCheck() = %v, 期望失败", err)
	}
}

func TestIntegrationCloseTwice(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	if err := c.Close(); err != nil {
		t.Fatalf("首次 Close() = %v, want nil", err)
	}
	if err := c.Close(); err == nil {
		t.Error("二次 Close() 期望返回已关闭错误")
	}
}

func TestIntegrationPartialInit(t *testing.T) {
	t.Parallel()

	addr := testRedisAddr(t)
	// DB 1000000 超出 Redis databases 上限，SELECT 失败 → 该 DB 初始化失败
	const badDB = 1000000

	t.Run("降级模式返回InitError", func(t *testing.T) {
		t.Parallel()
		c, err := NewClient(
			WithAddr(addr), WithKeyPrefix(testPrefix(t)),
			WithInitDBs(1, badDB), WithAllowPartialInit(),
		)
		if c == nil {
			t.Fatalf("降级模式下 DefaultDB 成功时 Client 不应为 nil, err=%v", err)
		}
		t.Cleanup(func() { _ = c.Close() })

		var ie *InitError
		if !errors.As(err, &ie) {
			t.Fatalf("err = %v, 期望 *InitError", err)
		}
		if _, ok := ie.Failed[badDB]; !ok {
			t.Errorf("InitError.Failed 缺少 db=%d: %v", badDB, ie.Failed)
		}
		if _, err := c.SelectDB(badDB); err == nil {
			t.Error("缺席 DB 的 SelectDB 期望错误")
		}
		if _, err := c.SelectDB(1); err != nil {
			t.Errorf("成功 DB 的 SelectDB = %v, want nil", err)
		}
	})

	t.Run("非降级模式整体失败", func(t *testing.T) {
		t.Parallel()
		c, err := NewClient(WithAddr(addr), WithInitDBs(badDB))
		if err == nil {
			_ = c.Close()
			t.Fatal("期望整体失败")
		}
		if c != nil {
			t.Error("整体失败时 Client 应为 nil")
		}
	})
}

// TestIntegrationMaxRetriesMapping 验证 WithMaxRetries(0) 映射为 go-redis 的
// -1（真关闭重试），正值原样透传。
func TestIntegrationMaxRetriesMapping(t *testing.T) {
	t.Parallel()

	// go-redis 的 init() 会把传入的哨兵规范化：-1（关闭）→ 0，0（默认）→ 3。
	// 因此映射生效时初始化后的 MaxRetries 应为 0；若库内不映射则会是 3。
	c0 := newTestClient(t, WithMaxRetries(0))
	if got := c0.DefaultClient().Options().MaxRetries; got != 0 {
		t.Errorf("WithMaxRetries(0) 初始化后 MaxRetries = %d, want 0（关闭重试）", got)
	}

	c5, err := NewClient(WithAddr(testRedisAddr(t)), WithMaxRetries(5))
	if err != nil {
		t.Fatalf("NewClient 失败: %v", err)
	}
	t.Cleanup(func() { _ = c5.Close() })
	if got := c5.DefaultClient().Options().MaxRetries; got != 5 {
		t.Errorf("WithMaxRetries(5) 底层 MaxRetries = %d, want 5", got)
	}
}

// ──────────────────────────────────────────
//  Proxy 命令代理
// ──────────────────────────────────────────

func TestIntegrationStringCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	if err := c.Set(ctx, "k", "v1", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, _ := c.Get(ctx, "k").Result(); got != "v1" {
		t.Errorf("Get = %q, want v1", got)
	}
	// 前缀拼接验证：原始 key 带前缀
	if got, err := c.DefaultClient().Get(ctx, testPrefix(t)+":k").Result(); err != nil || got != "v1" {
		t.Errorf("原始 key 读取 = (%q, %v), want (v1, nil)", got, err)
	}

	if ok, _ := c.SetNX(ctx, "k", "v2", time.Minute).Result(); ok {
		t.Error("SetNX 已存在 key 期望 false")
	}
	if err := c.SetEX(ctx, "kex", "v", time.Minute).Err(); err != nil {
		t.Errorf("SetEX: %v", err)
	}
	if old, _ := c.GetSet(ctx, "k", "v3").Result(); old != "v1" {
		t.Errorf("GetSet 旧值 = %q, want v1", old)
	}
	if got, _ := c.GetDel(ctx, "k").Result(); got != "v3" {
		t.Errorf("GetDel = %q, want v3", got)
	}
	if _, err := c.Get(ctx, "k").Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("GetDel 后 Get = %v, want redis.Nil", err)
	}

	if n, _ := c.Incr(ctx, "cnt").Result(); n != 1 {
		t.Errorf("Incr = %d, want 1", n)
	}
	if n, _ := c.IncrBy(ctx, "cnt", 10).Result(); n != 11 {
		t.Errorf("IncrBy = %d, want 11", n)
	}
	if n, _ := c.Decr(ctx, "cnt").Result(); n != 10 {
		t.Errorf("Decr = %d, want 10", n)
	}
	if n, _ := c.DecrBy(ctx, "cnt", 5).Result(); n != 5 {
		t.Errorf("DecrBy = %d, want 5", n)
	}
	if f, _ := c.IncrByFloat(ctx, "fcnt", 1.5).Result(); f != 1.5 {
		t.Errorf("IncrByFloat = %v, want 1.5", f)
	}

	if err := c.MSet(ctx, "m1", "a", "m2", "b").Err(); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	vals, err := c.MGet(ctx, "m1", "m2", "missing").Result()
	if err != nil || len(vals) != 3 || vals[0] != "a" || vals[1] != "b" || vals[2] != nil {
		t.Errorf("MGet = (%v, %v), want ([a b <nil>], nil)", vals, err)
	}
}

func TestIntegrationKeyCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	for _, k := range []string{"a", "b"} {
		if err := c.Set(ctx, k, "v", 0).Err(); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	if n, _ := c.Exists(ctx, "a", "b", "missing").Result(); n != 2 {
		t.Errorf("Exists = %d, want 2", n)
	}
	if typ, _ := c.Type(ctx, "a").Result(); typ != "string" {
		t.Errorf("Type = %q, want string", typ)
	}

	if ok, _ := c.Expire(ctx, "a", time.Hour).Result(); !ok {
		t.Error("Expire 期望 true")
	}
	if d, _ := c.TTL(ctx, "a").Result(); d <= 0 || d > time.Hour {
		t.Errorf("TTL = %v, 期望 (0, 1h]", d)
	}
	if d, _ := c.PTTL(ctx, "a").Result(); d <= 0 {
		t.Errorf("PTTL = %v, 期望 > 0", d)
	}
	if ok, _ := c.Persist(ctx, "a").Result(); !ok {
		t.Error("Persist 期望 true")
	}
	if d, _ := c.TTL(ctx, "a").Result(); d != -1 {
		t.Errorf("Persist 后 TTL = %v, want -1", d)
	}
	if ok, _ := c.ExpireAt(ctx, "a", time.Now().Add(time.Hour)).Result(); !ok {
		t.Error("ExpireAt 期望 true")
	}

	if err := c.Rename(ctx, "b", "b2").Err(); err != nil {
		t.Errorf("Rename: %v", err)
	}
	if got, _ := c.Get(ctx, "b2").Result(); got != "v" {
		t.Errorf("Rename 后 Get(b2) = %q, want v", got)
	}

	if n, _ := c.Del(ctx, "a", "b2").Result(); n != 2 {
		t.Errorf("Del = %d, want 2", n)
	}
}

func TestIntegrationHashCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	if n, err := c.HSet(ctx, "h", "f1", "v1", "f2", "v2").Result(); err != nil || n != 2 {
		t.Fatalf("HSet = (%d, %v), want (2, nil)", n, err)
	}
	if got, _ := c.HGet(ctx, "h", "f1").Result(); got != "v1" {
		t.Errorf("HGet = %q, want v1", got)
	}
	all, _ := c.HGetAll(ctx, "h").Result()
	if len(all) != 2 || all["f2"] != "v2" {
		t.Errorf("HGetAll = %v", all)
	}
	if err := c.HMSet(ctx, "h", "f3", "v3").Err(); err != nil {
		t.Errorf("HMSet: %v", err)
	}
	vals, _ := c.HMGet(ctx, "h", "f1", "f3").Result()
	if len(vals) != 2 || vals[1] != "v3" {
		t.Errorf("HMGet = %v", vals)
	}
	if ok, _ := c.HExists(ctx, "h", "f1").Result(); !ok {
		t.Error("HExists(f1) 期望 true")
	}
	if n, _ := c.HLen(ctx, "h").Result(); n != 3 {
		t.Errorf("HLen = %d, want 3", n)
	}
	if n, _ := c.HIncrBy(ctx, "h", "cnt", 7).Result(); n != 7 {
		t.Errorf("HIncrBy = %d, want 7", n)
	}
	if f, _ := c.HIncrByFloat(ctx, "h", "fcnt", 0.5).Result(); f != 0.5 {
		t.Errorf("HIncrByFloat = %v, want 0.5", f)
	}
	if n, _ := c.HDel(ctx, "h", "f1", "f2").Result(); n != 2 {
		t.Errorf("HDel = %d, want 2", n)
	}
}

func TestIntegrationListCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	if n, err := c.RPush(ctx, "l", "a", "b", "c").Result(); err != nil || n != 3 {
		t.Fatalf("RPush = (%d, %v)", n, err)
	}
	if n, _ := c.LPush(ctx, "l", "z").Result(); n != 4 {
		t.Errorf("LPush = %d, want 4", n)
	}
	if n, _ := c.LLen(ctx, "l").Result(); n != 4 {
		t.Errorf("LLen = %d, want 4", n)
	}
	if got, _ := c.LIndex(ctx, "l", 0).Result(); got != "z" {
		t.Errorf("LIndex(0) = %q, want z", got)
	}
	if items, _ := c.LRange(ctx, "l", 0, -1).Result(); len(items) != 4 || items[0] != "z" {
		t.Errorf("LRange = %v", items)
	}
	if got, _ := c.LPop(ctx, "l").Result(); got != "z" {
		t.Errorf("LPop = %q, want z", got)
	}
	if got, _ := c.RPop(ctx, "l").Result(); got != "c" {
		t.Errorf("RPop = %q, want c", got)
	}
	if n, _ := c.LRem(ctx, "l", 0, "a").Result(); n != 1 {
		t.Errorf("LRem = %d, want 1", n)
	}
	if err := c.LTrim(ctx, "l", 0, 0).Err(); err != nil {
		t.Errorf("LTrim: %v", err)
	}
}

func TestIntegrationSetCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	if n, err := c.SAdd(ctx, "s", "a", "b", "c").Result(); err != nil || n != 3 {
		t.Fatalf("SAdd = (%d, %v)", n, err)
	}
	if n, _ := c.SCard(ctx, "s").Result(); n != 3 {
		t.Errorf("SCard = %d, want 3", n)
	}
	if ok, _ := c.SIsMember(ctx, "s", "a").Result(); !ok {
		t.Error("SIsMember(a) 期望 true")
	}
	if members, _ := c.SMembers(ctx, "s").Result(); len(members) != 3 {
		t.Errorf("SMembers = %v", members)
	}
	if got, err := c.SRandMember(ctx, "s").Result(); err != nil || got == "" {
		t.Errorf("SRandMember = (%q, %v)", got, err)
	}
	if got, err := c.SPop(ctx, "s").Result(); err != nil || got == "" {
		t.Errorf("SPop = (%q, %v)", got, err)
	}
	if n, _ := c.SRem(ctx, "s", "a", "b", "c").Result(); n != 2 {
		t.Errorf("SPop 后 SRem = %d, want 2", n)
	}
}

func TestIntegrationZSetCommands(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	members := []redis.Z{{Score: 1, Member: "a"}, {Score: 2, Member: "b"}, {Score: 3, Member: "c"}}
	if n, err := c.ZAdd(ctx, "z", members...).Result(); err != nil || n != 3 {
		t.Fatalf("ZAdd = (%d, %v)", n, err)
	}
	if f, _ := c.ZScore(ctx, "z", "b").Result(); f != 2 {
		t.Errorf("ZScore = %v, want 2", f)
	}
	if r, _ := c.ZRank(ctx, "z", "c").Result(); r != 2 {
		t.Errorf("ZRank = %d, want 2", r)
	}
	if items, _ := c.ZRange(ctx, "z", 0, -1).Result(); len(items) != 3 || items[0] != "a" {
		t.Errorf("ZRange = %v", items)
	}
	if items, _ := c.ZRevRange(ctx, "z", 0, 0).Result(); len(items) != 1 || items[0] != "c" {
		t.Errorf("ZRevRange = %v, want [c]", items)
	}
	byScore, _ := c.ZRangeByScore(ctx, "z", &redis.ZRangeBy{Min: "2", Max: "3"}).Result()
	if len(byScore) != 2 || byScore[0] != "b" {
		t.Errorf("ZRangeByScore = %v, want [b c]", byScore)
	}
	if n, _ := c.ZCard(ctx, "z").Result(); n != 3 {
		t.Errorf("ZCard = %d, want 3", n)
	}
	if n, _ := c.ZCount(ctx, "z", "1", "2").Result(); n != 2 {
		t.Errorf("ZCount = %d, want 2", n)
	}
	if f, _ := c.ZIncrBy(ctx, "z", 10, "a").Result(); f != 11 {
		t.Errorf("ZIncrBy = %v, want 11", f)
	}
	if n, _ := c.ZRemRangeByScore(ctx, "z", "11", "11").Result(); n != 1 {
		t.Errorf("ZRemRangeByScore = %d, want 1", n)
	}
	if n, _ := c.ZRem(ctx, "z", "b", "c").Result(); n != 2 {
		t.Errorf("ZRem = %d, want 2", n)
	}
}

func TestIntegrationScanAndDelByPattern(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	const total = 30
	for i := range total {
		if err := c.Set(ctx, fmt.Sprintf("user:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := c.Set(ctx, "other:1", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Scan 返回带前缀的完整 key
	keys, _, err := c.Scan(ctx, 0, "user:*", 100).Result()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, testPrefix(t)+":user:") {
			t.Errorf("Scan 返回的 key %q 缺少完整前缀", k)
		}
	}

	deleted, err := c.DelByPattern(ctx, "user:*")
	if err != nil || deleted != total {
		t.Errorf("DelByPattern = (%d, %v), want (%d, nil)", deleted, err, total)
	}
	if got, _ := c.Get(ctx, "other:1").Result(); got != "v" {
		t.Error("DelByPattern 误删了不匹配的 key")
	}
	// 无匹配时返回 0
	if deleted, err := c.DelByPattern(ctx, "user:*"); err != nil || deleted != 0 {
		t.Errorf("二次 DelByPattern = (%d, %v), want (0, nil)", deleted, err)
	}
}

func TestIntegrationPipelineAndLua(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	pipe := c.Pipeline()
	pipe.Set(ctx, c.Key("p1"), "v1", 0)
	pipe.Set(ctx, c.Key("p2"), "v2", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Pipeline Exec: %v", err)
	}
	if got, _ := c.Get(ctx, "p1").Result(); got != "v1" {
		t.Errorf("Pipeline 写入后 Get(p1) = %q, want v1", got)
	}

	tx := c.TxPipeline()
	tx.Del(ctx, c.Keys("p1", "p2")...)
	if _, err := tx.Exec(ctx); err != nil {
		t.Fatalf("TxPipeline Exec: %v", err)
	}

	const script = `return redis.call("set", KEYS[1], ARGV[1])`
	if err := c.Eval(ctx, script, []string{"lua1"}, "ev").Err(); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if got, _ := c.Get(ctx, "lua1").Result(); got != "ev" {
		t.Errorf("Eval 写入后 Get = %q, want ev", got)
	}

	sha, err := c.DefaultClient().ScriptLoad(ctx, script).Result()
	if err != nil {
		t.Fatalf("ScriptLoad: %v", err)
	}
	if err := c.EvalSha(ctx, sha, []string{"lua2"}, "evsha").Err(); err != nil {
		t.Fatalf("EvalSha: %v", err)
	}
	if got, _ := c.Get(ctx, "lua2").Result(); got != "evsha" {
		t.Errorf("EvalSha 写入后 Get = %q, want evsha", got)
	}

	if err := c.EvalScript(ctx, redis.NewScript(script), []string{"lua3"}, "evs").Err(); err != nil {
		t.Fatalf("EvalScript: %v", err)
	}
	if got, _ := c.Get(ctx, "lua3").Result(); got != "evs" {
		t.Errorf("EvalScript 写入后 Get = %q, want evs", got)
	}
}

func TestIntegrationJSONHelpers(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	want := user{Name: "alice", Age: 18}
	if err := SetJSON(ctx, c.Proxy, "u:1", want, time.Minute); err != nil {
		t.Fatalf("SetJSON: %v", err)
	}
	got, err := GetJSON[user](ctx, c.Proxy, "u:1")
	if err != nil || got != want {
		t.Errorf("GetJSON = (%+v, %v), want (%+v, nil)", got, err, want)
	}

	// key 不存在：零值 + redis.Nil
	missing, err := GetJSON[user](ctx, c.Proxy, "u:none")
	if !errors.Is(err, redis.Nil) || missing != (user{}) {
		t.Errorf("miss = (%+v, %v), want (零值, redis.Nil)", missing, err)
	}

	// 损坏数据：错误含完整 key
	if err := c.Set(ctx, "u:bad", "not-json", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := GetJSON[user](ctx, c.Proxy, "u:bad"); err == nil ||
		!strings.Contains(err.Error(), c.Key("u:bad")) {
		t.Errorf("损坏数据错误 = %v, 期望含完整 key", err)
	}

	// 序列化失败：错误含 key 且不发送命令
	if err := SetJSON(ctx, c.Proxy, "u:chan", make(chan int), 0); err == nil ||
		!strings.Contains(err.Error(), c.Key("u:chan")) {
		t.Errorf("序列化失败 = %v, 期望含完整 key", err)
	}
}

// ──────────────────────────────────────────
//  Pub/Sub
// ──────────────────────────────────────────

// publishUntilReceived 周期发布直到消费方收到（Pub/Sub 是 at-most-once，
// 订阅生效前发布的消息会丢，轮询发布消除时序依赖）。
func publishUntilReceived(t *testing.T, c *Client, channel string, received <-chan string) string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case got := <-received:
			return got
		case <-deadline:
			t.Fatal("等待 Pub/Sub 消息超时")
			return ""
		case <-tick.C:
			if err := c.Publish(t.Context(), channel, "hello").Err(); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
	}
}

func TestIntegrationConsume(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	received := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		done <- c.Consume(ctx, func(m *redis.Message) { received <- m.Payload }, "events")
	}()

	if got := publishUntilReceived(t, c, "events", received); got != "hello" {
		t.Errorf("收到 %q, want hello", got)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Consume 退出错误 = %v, want context.Canceled", err)
	}
}

func TestIntegrationConsumePattern(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	received := make(chan string, 16)
	done := make(chan error, 1)
	go func() {
		done <- c.ConsumePattern(ctx, func(m *redis.Message) { received <- m.Payload }, "ev:*")
	}()

	if got := publishUntilReceived(t, c, "ev:user", received); got != "hello" {
		t.Errorf("收到 %q, want hello", got)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("ConsumePattern 退出错误 = %v, want context.Canceled", err)
	}
}

func TestIntegrationSubscribeRaw(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	sub := c.Subscribe(ctx, "raw")
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe 确认失败: %v", err)
	}

	psub := c.PSubscribe(ctx, "raw:*")
	t.Cleanup(func() { _ = psub.Close() })
	if _, err := psub.Receive(ctx); err != nil {
		t.Fatalf("PSubscribe 确认失败: %v", err)
	}
}

// ──────────────────────────────────────────
//  分布式锁
// ──────────────────────────────────────────

func TestIntegrationLockMutualExclusion(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	l1, err := c.TryLock(ctx, "mutex", time.Minute)
	if err != nil {
		t.Fatalf("首次 TryLock: %v", err)
	}

	if _, errAgain := c.TryLock(ctx, "mutex", time.Minute); !errors.Is(errAgain, ErrLockNotObtained) {
		t.Errorf("持有期间二次 TryLock = %v, want ErrLockNotObtained", errAgain)
	}

	if errRel := l1.Release(ctx); errRel != nil {
		t.Fatalf("Release: %v", errRel)
	}
	// 释放后可再次获取
	l2, err := c.TryLock(ctx, "mutex", time.Minute)
	if err != nil {
		t.Fatalf("释放后 TryLock: %v", err)
	}
	if err := l2.Release(ctx); err != nil {
		t.Errorf("l2 Release: %v", err)
	}
	// 已释放的锁再次释放 → ErrLockLost
	if err := l1.Release(ctx); !errors.Is(err, ErrLockLost) {
		t.Errorf("重复 Release = %v, want ErrLockLost", err)
	}
}

func TestIntegrationLockExpiryAndTokenSafety(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	l1, err := c.TryLock(ctx, "exp", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	time.Sleep(250 * time.Millisecond) // 等待锁过期

	// 他人重新获取
	l2, err := c.TryLock(ctx, "exp", time.Minute)
	if err != nil {
		t.Fatalf("过期后 TryLock: %v", err)
	}

	// 过期的旧持有者 Release/Refresh 均失败，且不影响新持有者
	if err := l1.Release(ctx); !errors.Is(err, ErrLockLost) {
		t.Errorf("过期后 Release = %v, want ErrLockLost", err)
	}
	if err := l1.Refresh(ctx, time.Minute); !errors.Is(err, ErrLockLost) {
		t.Errorf("过期后 Refresh = %v, want ErrLockLost", err)
	}
	if err := l2.Release(ctx); err != nil {
		t.Errorf("新持有者 Release = %v, want nil（旧持有者操作不得误删）", err)
	}
}

func TestIntegrationLockIntrospection(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	l, err := c.TryLock(ctx, "intro", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	if got, want := l.Key(), c.Key("intro"); got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}

	ttl, err := l.TTL(ctx)
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Errorf("TTL() = (%v, %v), 期望 (0, 1m] 且 nil", ttl, err)
	}

	// 冲突错误携带完整 key 且 errors.Is 链不变
	if _, errHeld := c.TryLock(ctx, "intro", time.Minute); !errors.Is(errHeld, ErrLockNotObtained) ||
		!strings.Contains(errHeld.Error(), c.Key("intro")) {
		t.Errorf("冲突错误 = %v, 期望包装 ErrLockNotObtained 且含完整 key %q", errHeld, c.Key("intro"))
	}

	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := l.TTL(ctx); !errors.Is(err, ErrLockLost) {
		t.Errorf("释放后 TTL() = %v, want ErrLockLost", err)
	}
}

func TestIntegrationLockRefresh(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	l, err := c.TryLock(ctx, "refresh", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if err := l.Refresh(ctx, 5*time.Second); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // 超过原 TTL，续期后应仍持有

	if err := l.Release(ctx); err != nil {
		t.Errorf("续期后 Release = %v, want nil（锁应仍被持有）", err)
	}
}

func TestIntegrationWithLock(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	t.Run("正常执行并释放", func(t *testing.T) {
		t.Parallel()
		ran := false
		if err := c.WithLock(ctx, "wl", time.Minute, func(context.Context) error {
			ran = true
			return nil
		}); err != nil || !ran {
			t.Fatalf("WithLock = %v, ran = %v, want (nil, true)", err, ran)
		}
		// 已释放：可立即再次获取
		l, err := c.TryLock(ctx, "wl", time.Minute)
		if err != nil {
			t.Fatalf("WithLock 后 TryLock: %v（锁未被释放）", err)
		}
		_ = l.Release(ctx)
	})

	t.Run("锁被持有时不执行fn", func(t *testing.T) {
		t.Parallel()
		l, err := c.TryLock(ctx, "held", time.Minute)
		if err != nil {
			t.Fatalf("TryLock: %v", err)
		}
		defer func() { _ = l.Release(ctx) }()

		ran := false
		err = c.WithLock(ctx, "held", time.Minute, func(context.Context) error {
			ran = true
			return nil
		})
		if !errors.Is(err, ErrLockNotObtained) || ran {
			t.Errorf("WithLock = (%v, ran=%v), want (ErrLockNotObtained, false)", err, ran)
		}
	})

	t.Run("fn错误原样传出", func(t *testing.T) {
		t.Parallel()
		want := errors.New("biz failed")
		if err := c.WithLock(ctx, "wlerr", time.Minute, func(context.Context) error {
			return want
		}); !errors.Is(err, want) {
			t.Errorf("WithLock = %v, want %v", err, want)
		}
	})

	t.Run("超TTL丢锁可感知", func(t *testing.T) {
		t.Parallel()
		err := c.WithLock(ctx, "wlexp", 100*time.Millisecond, func(context.Context) error {
			time.Sleep(250 * time.Millisecond) // 执行超过 ttl，锁过期
			return nil
		})
		if !errors.Is(err, ErrLockLost) {
			t.Errorf("超 TTL 后 WithLock = %v, want ErrLockLost", err)
		}
	})

	t.Run("panic仍释放锁", func(t *testing.T) {
		t.Parallel()
		func() {
			defer func() {
				if recover() == nil {
					t.Error("期望 panic 向上传播")
				}
			}()
			_ = c.WithLock(ctx, "wlpanic", time.Minute, func(context.Context) error {
				panic("boom")
			})
		}()
		// 锁已被释放
		l, err := c.TryLock(ctx, "wlpanic", time.Minute)
		if err != nil {
			t.Fatalf("panic 后 TryLock: %v（锁未被释放）", err)
		}
		_ = l.Release(ctx)
	})

	t.Run("nil fn报错", func(t *testing.T) {
		t.Parallel()
		if err := c.WithLock(ctx, "wlnil", time.Minute, nil); err == nil ||
			!strings.Contains(err.Error(), "fn is nil") {
			t.Errorf("nil fn = %v, 期望参数错误", err)
		}
	})
}

func TestIntegrationScanKeys(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	const total = 25
	for i := range total {
		if err := c.Set(ctx, fmt.Sprintf("user:%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := c.Set(ctx, "other:1", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var got []string
	for key, err := range c.ScanKeys(ctx, "user:*") {
		if err != nil {
			t.Fatalf("ScanKeys: %v", err)
		}
		got = append(got, key)
	}
	if len(got) != total {
		t.Fatalf("ScanKeys 产出 %d 个 key, want %d", len(got), total)
	}
	for _, k := range got {
		if strings.HasPrefix(k, testPrefix(t)) {
			t.Errorf("key %q 未剥前缀", k)
		}
		if !strings.HasPrefix(k, "user:") {
			t.Errorf("key %q 不匹配 pattern", k)
		}
	}
	// 产出的 key 可直接回传带前缀方法
	if got, err := c.Get(ctx, got[0]).Result(); err != nil || got != "v" {
		t.Errorf("回传 Get = (%q, %v), want (v, nil)", got, err)
	}

	// 提前 break 立即停止
	count := 0
	for _, err := range c.ScanKeys(ctx, "user:*") {
		if err != nil {
			t.Fatalf("ScanKeys: %v", err)
		}
		count++
		break
	}
	if count != 1 {
		t.Errorf("break 后仍产出 %d 个", count)
	}
}

func TestIntegrationXGroupManagement(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	xadd(t, c, "m0")
	if err := c.XGroupCreateMkStream(ctx, "st", "g1", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}

	// 消费者读取（不 ACK）后出现在 InfoConsumers，pending=1
	if _, err := c.DefaultClient().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g1", Consumer: "c1", Streams: []string{c.Key("st"), ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}

	groups, err := c.XInfoGroups(ctx, "st").Result()
	if err != nil || len(groups) != 1 || groups[0].Name != "g1" {
		t.Fatalf("XInfoGroups = (%v, %v), want 1 个 g1", groups, err)
	}
	consumers, err := c.XInfoConsumers(ctx, "st", "g1").Result()
	if err != nil || len(consumers) != 1 || consumers[0].Name != "c1" || consumers[0].Pending != 1 {
		t.Fatalf("XInfoConsumers = (%v, %v), want c1 pending=1", consumers, err)
	}

	// 清理消费者：返回被丢弃的 pending 数
	if n, err := c.XGroupDelConsumer(ctx, "st", "g1", "c1").Result(); err != nil || n != 1 {
		t.Errorf("XGroupDelConsumer = (%d, %v), want (1, nil)", n, err)
	}
	if consumers, _ := c.XInfoConsumers(ctx, "st", "g1").Result(); len(consumers) != 0 {
		t.Errorf("DelConsumer 后仍有消费者: %v", consumers)
	}

	if n, err := c.XGroupDestroy(ctx, "st", "g1").Result(); err != nil || n != 1 {
		t.Errorf("XGroupDestroy = (%d, %v), want (1, nil)", n, err)
	}
}

// ──────────────────────────────────────────
//  Stream 消费组
// ──────────────────────────────────────────

// streamCfg 返回低延迟的测试消费配置。
func streamCfg(consumer string) StreamConfig {
	return StreamConfig{
		Stream:    "st",
		Group:     "g1",
		Consumer:  consumer,
		BatchSize: 8,
		Block:     100 * time.Millisecond,
	}
}

// xadd 向测试流追加一条消息并返回 ID。
func xadd(t *testing.T, c *Client, val string) string {
	t.Helper()
	id, err := c.XAdd(t.Context(), &redis.XAddArgs{
		Stream: "st",
		Values: map[string]any{"v": val},
	}).Result()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	return id
}

// waitPending 轮询等待消费组 pending 数达到 want。
func waitPending(t *testing.T, c *Client, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p, err := c.XPending(t.Context(), "st", "g1").Result()
		if err == nil && p.Count == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	p, err := c.XPending(t.Context(), "st", "g1").Result()
	t.Fatalf("等待 pending=%d 超时, 当前 = (%+v, %v)", want, p, err)
}

func TestIntegrationStreamConsumeAndAck(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for i := range 3 {
		xadd(t, c, fmt.Sprintf("m%d", i))
	}

	received := make(chan redis.XMessage, 8)
	done := make(chan error, 1)
	go func() {
		done <- c.ConsumeStream(ctx, streamCfg("c1"), func(m redis.XMessage) error {
			received <- m
			return nil
		})
	}()

	for i := range 3 {
		select {
		case m := <-received:
			if m.Values["v"] != fmt.Sprintf("m%d", i) {
				t.Errorf("第 %d 条消息 = %v", i, m.Values)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("等待第 %d 条消息超时", i)
		}
	}

	waitPending(t, c, 0) // 全部 ACK
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("ConsumeStream 退出错误 = %v, want context.Canceled", err)
	}

	// 透传命令验证
	if n, _ := c.XLen(ctx2(t), "st").Result(); n != 3 {
		t.Errorf("XLen = %d, want 3", n)
	}
	msgs, _ := c.XRange(ctx2(t), "st", "-", "+").Result()
	if len(msgs) != 3 {
		t.Errorf("XRange 条数 = %d, want 3", len(msgs))
	}
	if len(msgs) > 0 {
		if n, _ := c.XDel(ctx2(t), "st", msgs[0].ID).Result(); n != 1 {
			t.Errorf("XDel = %d, want 1", n)
		}
	}
	if err := c.XTrimMaxLen(ctx2(t), "st", 1).Err(); err != nil {
		t.Errorf("XTrimMaxLen: %v", err)
	}
}

// ctx2 返回未被取消的测试 ctx（消费循环取消后透传命令仍需可用 ctx）。
func ctx2(t *testing.T) context.Context { return t.Context() }

func TestIntegrationStreamFailureKeepsPendingThenResume(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)

	id := xadd(t, c, "poison")

	// 阶段 1：handler 始终失败，消息留在 pending，OnError 收到明细
	ctx1, cancel1 := context.WithCancel(t.Context())
	defer cancel1()
	onErrCh := make(chan error, 8)
	seen := make(chan struct{}, 8)
	done1 := make(chan error, 1)
	cfg := streamCfg("c1")
	cfg.OnError = func(_ redis.XMessage, err error) { onErrCh <- err }
	go func() {
		done1 <- c.ConsumeStream(ctx1, cfg, func(redis.XMessage) error {
			seen <- struct{}{}
			return errors.New("biz boom")
		})
	}()

	select {
	case <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("等待 handler 执行超时")
	}
	select {
	case err := <-onErrCh:
		if !strings.Contains(err.Error(), "biz boom") {
			t.Errorf("OnError 收到 %v, want biz boom", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待 OnError 超时")
	}
	cancel1()
	<-done1
	waitPending(t, c, 1)

	// 验证 XPendingExt 明细
	ext, err := c.XPendingExt(t.Context(), &redis.XPendingExtArgs{
		Stream: "st", Group: "g1", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil || len(ext) != 1 || ext[0].ID != id {
		t.Errorf("XPendingExt = (%v, %v), want 1 条 id=%s", ext, err, id)
	}

	// 阶段 2：同一消费者重启，handler 成功 → 启动时续传 pending 并 ACK
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	resumed := make(chan redis.XMessage, 8)
	done2 := make(chan error, 1)
	go func() {
		done2 <- c.ConsumeStream(ctx2, streamCfg("c1"), func(m redis.XMessage) error {
			resumed <- m
			return nil
		})
	}()

	select {
	case m := <-resumed:
		if m.ID != id {
			t.Errorf("续传消息 ID = %s, want %s", m.ID, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待 pending 续传超时")
	}
	waitPending(t, c, 0)
	cancel2()
	<-done2
}

func TestIntegrationStreamAutoClaim(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	id := xadd(t, c, "orphan")

	// 模拟死亡消费者：读取但不 ACK
	stream := c.Key("st")
	if err := c.DefaultClient().XGroupCreateMkStream(ctx, stream, "g1", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
	if _, err := c.DefaultClient().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g1", Consumer: "dead", Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("dead 消费者读取: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // 让 pending 消息闲置超过 MinIdle

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	claimed := make(chan redis.XMessage, 8)
	done := make(chan error, 1)
	cfg := streamCfg("alive")
	cfg.AutoClaimMinIdle = 50 * time.Millisecond
	go func() {
		done <- c.ConsumeStream(cctx, cfg, func(m redis.XMessage) error {
			claimed <- m
			return nil
		})
	}()

	select {
	case m := <-claimed:
		if m.ID != id {
			t.Errorf("AutoClaim 接管消息 ID = %s, want %s", m.ID, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待 AutoClaim 接管超时")
	}
	waitPending(t, c, 0)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("退出错误 = %v, want context.Canceled", err)
	}
}

// TestIntegrationStreamAutoClaimCursorAdvance 验证 XAUTOCLAIM 游标周期间
// 推进：BatchSize=1 时一次只接管一条，扫描未完成游标停在中途（非 "0"/"0-0"），
// 下一周期续扫直至全部接管、游标回绕。
func TestIntegrationStreamAutoClaimCursorAdvance(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	const total = 3
	wantIDs := make(map[string]bool, total)
	for i := range total {
		wantIDs[xadd(t, c, fmt.Sprintf("m%d", i))] = true
	}

	// 死亡消费者读取全部消息后不 ACK
	stream := c.Key("st")
	if err := c.DefaultClient().XGroupCreateMkStream(ctx, stream, "g1", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
	if _, err := c.DefaultClient().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g1", Consumer: "dead", Streams: []string{stream, ">"}, Count: total,
	}).Result(); err != nil {
		t.Fatalf("dead 消费者读取: %v", err)
	}

	const minIdle = 50 * time.Millisecond
	time.Sleep(minIdle + 30*time.Millisecond)

	var claimed []string
	s := &streamConsumer{
		rdb: c.DefaultClient(),
		cfg: StreamConfig{
			Stream: "st", Group: "g1", Consumer: "alive",
			BatchSize: 1, AutoClaimMinIdle: minIdle,
		},
		stream: stream,
		handler: func(m redis.XMessage) error {
			claimed = append(claimed, m.ID)
			return nil
		},
	}

	// 第一个周期：只接管 1 条，扫描未完成 → 游标停在中途
	if err := s.maybeAutoClaim(ctx); err != nil {
		t.Fatalf("第 1 次 maybeAutoClaim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("第 1 次接管 %d 条, want 1", len(claimed))
	}
	if s.claimCursor == "" || s.claimCursor == "0" || s.claimCursor == "0-0" {
		t.Errorf("扫描未完成时 claimCursor = %q, 期望停在中途", s.claimCursor)
	}

	// 续扫直至接管完全部消息（每次重置 lastClaim 立即触发下一周期）
	for i := 0; len(claimed) < total && i < total+2; i++ {
		s.lastClaim = time.Time{}
		if err := s.maybeAutoClaim(ctx); err != nil {
			t.Fatalf("第 %d 次 maybeAutoClaim: %v", i+2, err)
		}
	}
	if len(claimed) != total {
		t.Fatalf("共接管 %d 条, want %d（游标未正确续扫）", len(claimed), total)
	}
	for _, id := range claimed {
		if !wantIDs[id] {
			t.Errorf("接管了意外消息 %s", id)
		}
		delete(wantIDs, id)
	}

	// 全部接管后再扫一轮空集，游标回绕也可正常续用
	s.lastClaim = time.Time{}
	if err := s.maybeAutoClaim(ctx); err != nil {
		t.Fatalf("回绕后 maybeAutoClaim: %v", err)
	}
	waitPending(t, c, 0)
}

// runConsumeUntil 启动 ConsumeStream 直到 cond 满足（轮询）或超时，随后取消并等待退出。
func runConsumeUntil(t *testing.T, c *Client, cfg StreamConfig, handler func(redis.XMessage) error, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.ConsumeStream(ctx, cfg, handler) }()

	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("等待消费条件超时")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("消费退出错误 = %v, want context.Canceled", err)
	}
}

// TestIntegrationStreamDeadLetter 验证毒消息生命周期：MaxDeliver=2 时
// handler 尝试 2 次（首投 + 续传重投），第三次投递在交给 handler 前被
// 原子隔离到死信流（含元数据），源流 pending 清零，OnError 收到可判别通知。
func TestIntegrationStreamDeadLetter(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	id := xadd(t, c, "poison")

	var attempts atomic.Int32
	var deadLettered atomic.Int32
	var notifyErr error
	cfg := streamCfg("c1")
	cfg.MaxDeliver = 2
	cfg.DeadLetterStream = "dlq"
	cfg.OnError = func(_ redis.XMessage, err error) {
		if errors.Is(err, ErrMessageDeadLettered) {
			notifyErr = err
			deadLettered.Add(1)
		}
	}
	failing := func(redis.XMessage) error {
		attempts.Add(1)
		return errors.New("biz boom")
	}

	// 第 1 次投递：新消息，handler 尝试并失败
	runConsumeUntil(t, c, cfg, failing, func() bool { return attempts.Load() >= 1 })
	// 第 2 次投递：重启续传 pending（投递计数=2，未超限），handler 再次尝试
	runConsumeUntil(t, c, cfg, failing, func() bool { return attempts.Load() >= 2 })
	// 第 3 次投递：计数=3 > MaxDeliver=2，进死信流，handler 不再被调用
	runConsumeUntil(t, c, cfg, failing, func() bool { return deadLettered.Load() >= 1 })

	if got := attempts.Load(); got != 2 {
		t.Errorf("handler 尝试次数 = %d, want 2（MaxDeliver 次）", got)
	}
	if notifyErr == nil || !strings.Contains(notifyErr.Error(), id) {
		t.Errorf("死信通知 = %v, 期望含原消息 ID %s", notifyErr, id)
	}

	// 源流 pending 清零（原消息已 ACK）
	waitPending(t, c, 0)

	// 死信流恰有 1 条，字段含原值与全部元数据
	dlMsgs, err := c.XRange(ctx, "dlq", "-", "+").Result()
	if err != nil || len(dlMsgs) != 1 {
		t.Fatalf("死信流 XRange = (%v, %v), want 1 条", dlMsgs, err)
	}
	dl := dlMsgs[0].Values
	if dl["v"] != "poison" {
		t.Errorf("死信消息原字段 v = %v, want poison", dl["v"])
	}
	if dl["_redisx_origin_id"] != id {
		t.Errorf("_redisx_origin_id = %v, want %s", dl["_redisx_origin_id"], id)
	}
	if dl["_redisx_origin_stream"] != c.Key("st") {
		t.Errorf("_redisx_origin_stream = %v, want %s", dl["_redisx_origin_stream"], c.Key("st"))
	}
	if dl["_redisx_deliveries"] != "3" {
		t.Errorf("_redisx_deliveries = %v, want 3", dl["_redisx_deliveries"])
	}
	if dl["_redisx_dead_at"] == nil || dl["_redisx_dead_at"] == "" {
		t.Error("_redisx_dead_at 缺失")
	}

	// 死信化后正常消息不受影响
	var okCount atomic.Int32
	xadd(t, c, "normal")
	runConsumeUntil(t, c, cfg, func(redis.XMessage) error {
		okCount.Add(1)
		return nil
	}, func() bool { return okCount.Load() >= 1 })
	waitPending(t, c, 0)
}

// TestIntegrationStreamDeadLetterViaAutoClaim 验证 AutoClaim 接管路径同样
// 检查投递次数：死亡消费者反复接管导致计数超限的消息直接进死信流。
func TestIntegrationStreamDeadLetterViaAutoClaim(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	id := xadd(t, c, "orphan")
	stream := c.Key("st")
	if err := c.XGroupCreateMkStream(ctx, "st", "g1", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
	// 死亡消费者读取 1 次（计数=1），随后用 XCLAIM 反复接管推高计数至 3
	if _, err := c.DefaultClient().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g1", Consumer: "dead", Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	for range 2 {
		if err := c.DefaultClient().XClaim(ctx, &redis.XClaimArgs{
			Stream: stream, Group: "g1", Consumer: "dead", MinIdle: 0, Messages: []string{id},
		}).Err(); err != nil {
			t.Fatalf("XClaim: %v", err)
		}
	}
	time.Sleep(80 * time.Millisecond) // 闲置超过 MinIdle

	var attempts atomic.Int32
	var deadLettered atomic.Int32
	cfg := streamCfg("alive")
	cfg.AutoClaimMinIdle = 50 * time.Millisecond
	cfg.MaxDeliver = 3
	cfg.DeadLetterStream = "dlq"
	cfg.OnError = func(_ redis.XMessage, err error) {
		if errors.Is(err, ErrMessageDeadLettered) {
			deadLettered.Add(1)
		}
	}
	// AutoClaim 接管自增计数至 4 > 3 → 直接死信，handler 不被调用
	runConsumeUntil(t, c, cfg, func(redis.XMessage) error {
		attempts.Add(1)
		return nil
	}, func() bool { return deadLettered.Load() >= 1 })

	if got := attempts.Load(); got != 0 {
		t.Errorf("超限消息仍被投递 handler %d 次, want 0", got)
	}
	if n, _ := c.XLen(ctx, "dlq").Result(); n != 1 {
		t.Errorf("死信流长度 = %d, want 1", n)
	}
	waitPending(t, c, 0)
}

func TestIntegrationStreamCtxCancelWhileBlocked(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	cfg := streamCfg("c1")
	cfg.Block = 3 * time.Second // 长阻塞中取消
	go func() {
		done <- c.ConsumeStream(ctx, cfg, func(redis.XMessage) error { return nil })
	}()

	time.Sleep(200 * time.Millisecond) // 让消费循环进入阻塞读
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("阻塞中取消退出错误 = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("阻塞读未响应 ctx 取消")
	}
}

func TestIntegrationStreamXAckPassthrough(t *testing.T) {
	t.Parallel()

	c := newTestClient(t)
	ctx := t.Context()

	id := xadd(t, c, "ackme")
	stream := c.Key("st")
	if err := c.DefaultClient().XGroupCreateMkStream(ctx, stream, "g1", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream: %v", err)
	}
	if _, err := c.DefaultClient().XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g1", Consumer: "c1", Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	if n, err := c.XAck(ctx, "st", "g1", id).Result(); err != nil || n != 1 {
		t.Errorf("XAck = (%d, %v), want (1, nil)", n, err)
	}
}
