package redisx

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 以下方法是 Client 级别的快捷方式，均委托给默认 DB 的缓存 [Proxy] 执行。
// 等价于 c.MustSelectDB(defaultDB).Method(...)。

// ──────────────────────────────────────────
//  String shortcuts
// ──────────────────────────────────────────

// Get 在默认 DB 上获取 key 的值。key 自动拼接前缀。
func (c *Client) Get(ctx context.Context, key string) *goredis.StringCmd {
	return c.defProxy.Get(ctx, key)
}

// Set 在默认 DB 上设置 key-value。key 自动拼接前缀。
func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) *goredis.StatusCmd {
	return c.defProxy.Set(ctx, key, value, expiration)
}

// SetEX 在默认 DB 上设置 key-value 并指定过期时间。key 自动拼接前缀。
func (c *Client) SetEX(ctx context.Context, key string, value any, expiration time.Duration) *goredis.StatusCmd {
	return c.defProxy.SetEX(ctx, key, value, expiration)
}

// SetNX 在默认 DB 上仅当 key 不存在时设置。key 自动拼接前缀。
func (c *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *goredis.BoolCmd {
	return c.defProxy.SetNX(ctx, key, value, expiration)
}

// Incr 在默认 DB 上将 key 的值加 1。key 自动拼接前缀。
func (c *Client) Incr(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.Incr(ctx, key)
}

// IncrBy 在默认 DB 上将 key 的值加指定增量。key 自动拼接前缀。
func (c *Client) IncrBy(ctx context.Context, key string, value int64) *goredis.IntCmd {
	return c.defProxy.IncrBy(ctx, key, value)
}

// Decr 在默认 DB 上将 key 的值减 1。key 自动拼接前缀。
func (c *Client) Decr(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.Decr(ctx, key)
}

// DecrBy 在默认 DB 上将 key 的值减指定减量。key 自动拼接前缀。
func (c *Client) DecrBy(ctx context.Context, key string, value int64) *goredis.IntCmd {
	return c.defProxy.DecrBy(ctx, key, value)
}

// MGet 在默认 DB 上批量获取多个 key 的值。key 自动拼接前缀。
func (c *Client) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
	return c.defProxy.MGet(ctx, keys...)
}

// MSet 在默认 DB 上批量设置 key-value 对。key 自动拼接前缀。
func (c *Client) MSet(ctx context.Context, values ...any) *goredis.StatusCmd {
	return c.defProxy.MSet(ctx, values...)
}

// ──────────────────────────────────────────
//  Key shortcuts
// ──────────────────────────────────────────

// Del 在默认 DB 上删除 key。key 自动拼接前缀。
func (c *Client) Del(ctx context.Context, keys ...string) *goredis.IntCmd {
	return c.defProxy.Del(ctx, keys...)
}

// Exists 在默认 DB 上检查 key 是否存在。key 自动拼接前缀。
func (c *Client) Exists(ctx context.Context, keys ...string) *goredis.IntCmd {
	return c.defProxy.Exists(ctx, keys...)
}

// Expire 在默认 DB 上设置 key 的过期时间。key 自动拼接前缀。
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	return c.defProxy.Expire(ctx, key, expiration)
}

// TTL 在默认 DB 上返回 key 的剩余生存时间。key 自动拼接前缀。
func (c *Client) TTL(ctx context.Context, key string) *goredis.DurationCmd {
	return c.defProxy.TTL(ctx, key)
}

// ──────────────────────────────────────────
//  Hash shortcuts
// ──────────────────────────────────────────

// HSet 在默认 DB 上设置 hash 字段。key 自动拼接前缀。
func (c *Client) HSet(ctx context.Context, key string, values ...any) *goredis.IntCmd {
	return c.defProxy.HSet(ctx, key, values...)
}

// HGet 在默认 DB 上获取 hash 字段值。key 自动拼接前缀。
func (c *Client) HGet(ctx context.Context, key, field string) *goredis.StringCmd {
	return c.defProxy.HGet(ctx, key, field)
}

// HGetAll 在默认 DB 上获取 hash 所有字段和值。key 自动拼接前缀。
func (c *Client) HGetAll(ctx context.Context, key string) *goredis.MapStringStringCmd {
	return c.defProxy.HGetAll(ctx, key)
}

// HMSet 在默认 DB 上批量设置 hash 字段。key 自动拼接前缀。
func (c *Client) HMSet(ctx context.Context, key string, values ...any) *goredis.BoolCmd {
	return c.defProxy.HMSet(ctx, key, values...)
}

// HDel 在默认 DB 上删除 hash 字段。key 自动拼接前缀。
func (c *Client) HDel(ctx context.Context, key string, fields ...string) *goredis.IntCmd {
	return c.defProxy.HDel(ctx, key, fields...)
}

// HExists 在默认 DB 上检查 hash 字段是否存在。key 自动拼接前缀。
func (c *Client) HExists(ctx context.Context, key, field string) *goredis.BoolCmd {
	return c.defProxy.HExists(ctx, key, field)
}

// HLen 在默认 DB 上返回 hash 字段数量。key 自动拼接前缀。
func (c *Client) HLen(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.HLen(ctx, key)
}

// HIncrBy 在默认 DB 上将 hash 字段值加指定增量。key 自动拼接前缀。
func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) *goredis.IntCmd {
	return c.defProxy.HIncrBy(ctx, key, field, incr)
}

// ──────────────────────────────────────────
//  List shortcuts
// ──────────────────────────────────────────

// LPush 在默认 DB 上从列表左侧推入元素。key 自动拼接前缀。
func (c *Client) LPush(ctx context.Context, key string, values ...any) *goredis.IntCmd {
	return c.defProxy.LPush(ctx, key, values...)
}

// RPush 在默认 DB 上从列表右侧推入元素。key 自动拼接前缀。
func (c *Client) RPush(ctx context.Context, key string, values ...any) *goredis.IntCmd {
	return c.defProxy.RPush(ctx, key, values...)
}

// LPop 在默认 DB 上从列表左侧弹出元素。key 自动拼接前缀。
func (c *Client) LPop(ctx context.Context, key string) *goredis.StringCmd {
	return c.defProxy.LPop(ctx, key)
}

// RPop 在默认 DB 上从列表右侧弹出元素。key 自动拼接前缀。
func (c *Client) RPop(ctx context.Context, key string) *goredis.StringCmd {
	return c.defProxy.RPop(ctx, key)
}

// LRange 在默认 DB 上返回列表指定范围元素。key 自动拼接前缀。
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) *goredis.StringSliceCmd {
	return c.defProxy.LRange(ctx, key, start, stop)
}

// LLen 在默认 DB 上返回列表长度。key 自动拼接前缀。
func (c *Client) LLen(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.LLen(ctx, key)
}

// ──────────────────────────────────────────
//  Set shortcuts
// ──────────────────────────────────────────

// SAdd 在默认 DB 上向集合添加成员。key 自动拼接前缀。
func (c *Client) SAdd(ctx context.Context, key string, members ...any) *goredis.IntCmd {
	return c.defProxy.SAdd(ctx, key, members...)
}

// SMembers 在默认 DB 上返回集合所有成员。key 自动拼接前缀。
func (c *Client) SMembers(ctx context.Context, key string) *goredis.StringSliceCmd {
	return c.defProxy.SMembers(ctx, key)
}

// SIsMember 在默认 DB 上判断成员是否在集合中。key 自动拼接前缀。
func (c *Client) SIsMember(ctx context.Context, key string, member any) *goredis.BoolCmd {
	return c.defProxy.SIsMember(ctx, key, member)
}

// SRem 在默认 DB 上移除集合成员。key 自动拼接前缀。
func (c *Client) SRem(ctx context.Context, key string, members ...any) *goredis.IntCmd {
	return c.defProxy.SRem(ctx, key, members...)
}

// SCard 在默认 DB 上返回集合成员数量。key 自动拼接前缀。
func (c *Client) SCard(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.SCard(ctx, key)
}

// ──────────────────────────────────────────
//  Sorted Set shortcuts
// ──────────────────────────────────────────

// ZAdd 在默认 DB 上向有序集合添加成员。key 自动拼接前缀。
func (c *Client) ZAdd(ctx context.Context, key string, members ...goredis.Z) *goredis.IntCmd {
	return c.defProxy.ZAdd(ctx, key, members...)
}

// ZRem 在默认 DB 上移除有序集合成员。key 自动拼接前缀。
func (c *Client) ZRem(ctx context.Context, key string, members ...any) *goredis.IntCmd {
	return c.defProxy.ZRem(ctx, key, members...)
}

// ZCard 在默认 DB 上返回有序集合成员数量。key 自动拼接前缀。
func (c *Client) ZCard(ctx context.Context, key string) *goredis.IntCmd {
	return c.defProxy.ZCard(ctx, key)
}

// ZScore 在默认 DB 上返回有序集合成员的分值。key 自动拼接前缀。
func (c *Client) ZScore(ctx context.Context, key, member string) *goredis.FloatCmd {
	return c.defProxy.ZScore(ctx, key, member)
}

// ZRange 在默认 DB 上按排名范围返回有序集合成员。key 自动拼接前缀。
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) *goredis.StringSliceCmd {
	return c.defProxy.ZRange(ctx, key, start, stop)
}

// ZRangeByScore 在默认 DB 上按分值范围返回有序集合成员。key 自动拼接前缀。
func (c *Client) ZRangeByScore(ctx context.Context, key string, opt *goredis.ZRangeBy) *goredis.StringSliceCmd {
	return c.defProxy.ZRangeByScore(ctx, key, opt)
}

// ──────────────────────────────────────────
//  Pub/Sub shortcuts
// ──────────────────────────────────────────

// Publish 在默认 DB 上发布消息。channel 自动拼接前缀。
func (c *Client) Publish(ctx context.Context, channel string, message any) *goredis.IntCmd {
	return c.defProxy.Publish(ctx, channel, message)
}

// Subscribe 在默认 DB 上订阅 channel。channel 自动拼接前缀。
func (c *Client) Subscribe(ctx context.Context, channels ...string) *goredis.PubSub {
	return c.defProxy.Subscribe(ctx, channels...)
}

// PSubscribe 在默认 DB 上按模式订阅。模式自动拼接前缀。
func (c *Client) PSubscribe(ctx context.Context, patterns ...string) *goredis.PubSub {
	return c.defProxy.PSubscribe(ctx, patterns...)
}

// Consume 在默认 DB 上以受管方式订阅并消费消息。语义见 [Proxy.Consume]。
func (c *Client) Consume(ctx context.Context, handler func(*goredis.Message), channels ...string) error {
	return c.defProxy.Consume(ctx, handler, channels...)
}

// ──────────────────────────────────────────
//  Scan & batch deletion shortcuts
// ──────────────────────────────────────────

// TryLock 在默认 DB 上尝试获取分布式锁。语义见 [Proxy.TryLock]。
func (c *Client) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	return c.defProxy.TryLock(ctx, key, ttl)
}

// XAdd 在默认 DB 上追加消息到流。args.Stream 自动拼接前缀。
func (c *Client) XAdd(ctx context.Context, args *goredis.XAddArgs) *goredis.StringCmd {
	return c.defProxy.XAdd(ctx, args)
}

// XAck 在默认 DB 上确认消费组内消息。stream 自动拼接前缀。
func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) *goredis.IntCmd {
	return c.defProxy.XAck(ctx, stream, group, ids...)
}

// ConsumeStream 在默认 DB 上以消费组方式受管消费流消息。语义见 [Proxy.ConsumeStream]。
func (c *Client) ConsumeStream(ctx context.Context, cfg StreamConfig, handler func(goredis.XMessage) error) error {
	return c.defProxy.ConsumeStream(ctx, cfg, handler)
}

// DelByPattern 在默认 DB 上安全批量删除匹配 pattern 的 key。
//
// 使用 SCAN + UNLINK（需要 Redis >= 4.0），pattern 自动拼接前缀。返回成功删除的 key 总数。
func (c *Client) DelByPattern(ctx context.Context, pattern string) (int64, error) {
	return c.defProxy.DelByPattern(ctx, pattern)
}

// ──────────────────────────────────────────
//  Lua scripting shortcuts
// ──────────────────────────────────────────

// Eval 在默认 DB 上执行 Lua 脚本。keys 自动拼接前缀。
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	return c.defProxy.Eval(ctx, script, keys, args...)
}

// EvalSha 在默认 DB 上执行已缓存的 Lua 脚本。keys 自动拼接前缀。
func (c *Client) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *goredis.Cmd {
	return c.defProxy.EvalSha(ctx, sha1, keys, args...)
}

// EvalScript 在默认 DB 上执行 [*redis.Script]。keys 自动拼接前缀。
func (c *Client) EvalScript(ctx context.Context, script *goredis.Script, keys []string, args ...any) *goredis.Cmd {
	return c.defProxy.EvalScript(ctx, script, keys, args...)
}
