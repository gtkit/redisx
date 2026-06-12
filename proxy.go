package redisx

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Proxy 是对单个 DB 的 [*redis.Client] 命令代理。
//
// 所有带 key 参数的方法会自动拼接前缀（全局或 per-DB），对业务层透明。
// 通过 [Client.SelectDB] / [Client.MustSelectDB] 获取。
//
// Proxy 在 [NewClient] 中按 DB 缓存，不会重复创建。
type Proxy struct {
	rdb             *redis.Client
	prefix          string
	prefixSeparator string
}

// key 为原始 key 拼接前缀。
func (p *Proxy) key(k string) string {
	return prefixKey(p.prefix, p.prefixSeparator, k)
}

// keys 为多个 key 批量拼接前缀。
func (p *Proxy) keys(ks []string) []string {
	if len(ks) == 0 {
		return ks
	}
	result := make([]string, len(ks))
	for i, k := range ks {
		result[i] = p.key(k)
	}
	return result
}

// RawClient 返回底层 [*redis.Client]。
//
// 注意：通过 RawClient 执行的命令不会自动添加 key 前缀。
func (p *Proxy) RawClient() *redis.Client {
	return p.rdb
}

// Key 返回拼接了前缀的完整 key。
//
// 用于在 Pipeline 等需要手动拼接前缀的场景。
func (p *Proxy) Key(k string) string {
	return p.key(k)
}

// Keys 返回逐个拼接了前缀的完整 key 列表。
//
// 用于在 Pipeline 等需要手动拼接前缀的场景批量处理多个 key，
// 例如 pipe.Del(ctx, proxy.Keys("k1", "k2")...)。
func (p *Proxy) Keys(ks ...string) []string {
	return p.keys(ks)
}

// ──────────────────────────────────────────
//  String commands
// ──────────────────────────────────────────

// Get 获取 key 的值。key 自动拼接前缀。
//
// key 不存在返回 [redis.Nil]，可通过 errors.Is(err, redis.Nil) 判断。
func (p *Proxy) Get(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.Get(ctx, p.key(key))
}

// Set 设置 key-value。expiration 为 0 表示不设置过期时间。key 自动拼接前缀。
func (p *Proxy) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return p.rdb.Set(ctx, p.key(key), value, expiration)
}

// SetEX 设置 key-value 并指定过期时间。key 自动拼接前缀。
//
// 内部使用 SET 带过期实现（SETEX 的现代等价形式）；
// expiration 为 0 时等同于无过期时间的 SET。
func (p *Proxy) SetEX(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return p.rdb.Set(ctx, p.key(key), value, expiration)
}

// SetNX 仅当 key 不存在时设置。key 自动拼接前缀。
//
// 返回 true 表示设置成功，false 表示 key 已存在。
func (p *Proxy) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return p.rdb.SetNX(ctx, p.key(key), value, expiration)
}

// GetSet 设置新值并返回旧值。key 自动拼接前缀。
//
// 内部使用 SET ... GET 实现（GETSET 的现代等价形式，需要 Redis >= 6.2）；
// 旧值不存在时返回 redis.Nil，新值仍会写入。
func (p *Proxy) GetSet(ctx context.Context, key string, value any) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "set", p.key(key), value, "get")
	_ = p.rdb.Process(ctx, cmd)
	return cmd
}

// GetDel 获取 key 的值并删除该 key。key 自动拼接前缀。
func (p *Proxy) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.GetDel(ctx, p.key(key))
}

// Incr 将 key 的整数值加 1。key 自动拼接前缀。
func (p *Proxy) Incr(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.Incr(ctx, p.key(key))
}

// IncrBy 将 key 的整数值增加指定增量。key 自动拼接前缀。
func (p *Proxy) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return p.rdb.IncrBy(ctx, p.key(key), value)
}

// IncrByFloat 将 key 的浮点数值增加指定增量。key 自动拼接前缀。
func (p *Proxy) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	return p.rdb.IncrByFloat(ctx, p.key(key), value)
}

// Decr 将 key 的整数值减 1。key 自动拼接前缀。
func (p *Proxy) Decr(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.Decr(ctx, p.key(key))
}

// DecrBy 将 key 的整数值减少指定减量。key 自动拼接前缀。
func (p *Proxy) DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return p.rdb.DecrBy(ctx, p.key(key), value)
}

// MGet 批量获取多个 key 的值。所有 key 自动拼接前缀。
func (p *Proxy) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return p.rdb.MGet(ctx, p.keys(keys)...)
}

// MSet 批量设置 key-value 对。
//
// values 为交替的 key-value 序列: MSet(ctx, "k1", "v1", "k2", "v2")。
// 偶数位（0, 2, 4...）必须为 string，作为 key 自动拼接前缀。
//
// values 长度必须为偶数且 key 位必须为 string，否则返回错误。
func (p *Proxy) MSet(ctx context.Context, values ...any) *redis.StatusCmd {
	if len(values)%2 != 0 {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(fmt.Errorf("redisx: MSet requires even number of arguments, got %d", len(values)))
		return cmd
	}
	prefixed := make([]any, len(values))
	copy(prefixed, values)
	for i := 0; i < len(prefixed); i += 2 {
		k, ok := prefixed[i].(string)
		if !ok {
			// 静默跳过前缀会让 key 落入无前缀命名空间且不可发现，必须报错
			cmd := redis.NewStatusCmd(ctx)
			cmd.SetErr(fmt.Errorf("redisx: MSet key at index %d must be string, got %T", i, prefixed[i]))
			return cmd
		}
		prefixed[i] = p.key(k)
	}
	return p.rdb.MSet(ctx, prefixed...)
}

// ──────────────────────────────────────────
//  Key commands
// ──────────────────────────────────────────

// Del 删除一个或多个 key。所有 key 自动拼接前缀。返回成功删除的数量。
func (p *Proxy) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return p.rdb.Del(ctx, p.keys(keys)...)
}

// Exists 检查一个或多个 key 是否存在。所有 key 自动拼接前缀。返回存在的数量。
func (p *Proxy) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return p.rdb.Exists(ctx, p.keys(keys)...)
}

// Expire 设置 key 的过期时间。key 自动拼接前缀。
func (p *Proxy) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return p.rdb.Expire(ctx, p.key(key), expiration)
}

// ExpireAt 设置 key 在指定时间点过期。key 自动拼接前缀。
func (p *Proxy) ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	return p.rdb.ExpireAt(ctx, p.key(key), tm)
}

// Persist 移除 key 的过期时间。key 自动拼接前缀。
func (p *Proxy) Persist(ctx context.Context, key string) *redis.BoolCmd {
	return p.rdb.Persist(ctx, p.key(key))
}

// TTL 返回 key 的剩余生存时间。key 自动拼接前缀。
//
// key 不存在返回 -2，key 无过期时间返回 -1。
func (p *Proxy) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return p.rdb.TTL(ctx, p.key(key))
}

// PTTL 返回 key 的剩余生存时间（毫秒精度）。key 自动拼接前缀。
func (p *Proxy) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return p.rdb.PTTL(ctx, p.key(key))
}

// Type 返回 key 存储的值的类型。key 自动拼接前缀。
func (p *Proxy) Type(ctx context.Context, key string) *redis.StatusCmd {
	return p.rdb.Type(ctx, p.key(key))
}

// Rename 重命名 key。两个 key 均自动拼接前缀。
func (p *Proxy) Rename(ctx context.Context, key, newkey string) *redis.StatusCmd {
	return p.rdb.Rename(ctx, p.key(key), p.key(newkey))
}

// ──────────────────────────────────────────
//  Hash commands
// ──────────────────────────────────────────

// HSet 设置 hash 中一个或多个字段。key 自动拼接前缀。
//
// values 接受 field-value 对: HSet(ctx, "user:1", "name", "alice", "age", 18).
func (p *Proxy) HSet(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.rdb.HSet(ctx, p.key(key), values...)
}

// HGet 获取 hash 中指定字段的值。key 自动拼接前缀。
func (p *Proxy) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return p.rdb.HGet(ctx, p.key(key), field)
}

// HGetAll 获取 hash 中所有字段和值。key 自动拼接前缀。
func (p *Proxy) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	return p.rdb.HGetAll(ctx, p.key(key))
}

// HMSet 批量设置 hash 字段。key 自动拼接前缀。
//
// Deprecated: Redis 官方建议使用 [Proxy.HSet]，HMSet 保留用于兼容。
func (p *Proxy) HMSet(ctx context.Context, key string, values ...any) *redis.BoolCmd {
	return p.rdb.HMSet(ctx, p.key(key), values...)
}

// HMGet 批量获取 hash 中多个字段的值。key 自动拼接前缀。
func (p *Proxy) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	return p.rdb.HMGet(ctx, p.key(key), fields...)
}

// HDel 删除 hash 中一个或多个字段。key 自动拼接前缀。
func (p *Proxy) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return p.rdb.HDel(ctx, p.key(key), fields...)
}

// HExists 检查 hash 中指定字段是否存在。key 自动拼接前缀。
func (p *Proxy) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	return p.rdb.HExists(ctx, p.key(key), field)
}

// HLen 返回 hash 中字段数量。key 自动拼接前缀。
func (p *Proxy) HLen(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.HLen(ctx, p.key(key))
}

// HIncrBy 将 hash 中指定字段的整数值增加增量。key 自动拼接前缀。
func (p *Proxy) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	return p.rdb.HIncrBy(ctx, p.key(key), field, incr)
}

// HIncrByFloat 将 hash 中指定字段的浮点数值增加增量。key 自动拼接前缀。
func (p *Proxy) HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd {
	return p.rdb.HIncrByFloat(ctx, p.key(key), field, incr)
}

// ──────────────────────────────────────────
//  List commands
// ──────────────────────────────────────────

// LPush 从列表左侧推入一个或多个元素。key 自动拼接前缀。
func (p *Proxy) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.rdb.LPush(ctx, p.key(key), values...)
}

// RPush 从列表右侧推入一个或多个元素。key 自动拼接前缀。
func (p *Proxy) RPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.rdb.RPush(ctx, p.key(key), values...)
}

// LPop 从列表左侧弹出一个元素。key 自动拼接前缀。
func (p *Proxy) LPop(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.LPop(ctx, p.key(key))
}

// RPop 从列表右侧弹出一个元素。key 自动拼接前缀。
func (p *Proxy) RPop(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.RPop(ctx, p.key(key))
}

// LRange 返回列表中指定范围的元素。key 自动拼接前缀。
//
// start 和 stop 为从零开始的索引，-1 表示最后一个元素。
func (p *Proxy) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return p.rdb.LRange(ctx, p.key(key), start, stop)
}

// LLen 返回列表长度。key 自动拼接前缀。
func (p *Proxy) LLen(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.LLen(ctx, p.key(key))
}

// LRem 移除列表中与 value 相等的元素。key 自动拼接前缀。
func (p *Proxy) LRem(ctx context.Context, key string, count int64, value any) *redis.IntCmd {
	return p.rdb.LRem(ctx, p.key(key), count, value)
}

// LIndex 返回列表中指定索引的元素。key 自动拼接前缀。
func (p *Proxy) LIndex(ctx context.Context, key string, index int64) *redis.StringCmd {
	return p.rdb.LIndex(ctx, p.key(key), index)
}

// LTrim 只保留列表中指定范围的元素。key 自动拼接前缀。
func (p *Proxy) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	return p.rdb.LTrim(ctx, p.key(key), start, stop)
}

// ──────────────────────────────────────────
//  Set commands
// ──────────────────────────────────────────

// SAdd 向集合添加一个或多个成员。key 自动拼接前缀。
func (p *Proxy) SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.rdb.SAdd(ctx, p.key(key), members...)
}

// SMembers 返回集合中所有成员。key 自动拼接前缀。
func (p *Proxy) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return p.rdb.SMembers(ctx, p.key(key))
}

// SIsMember 判断 member 是否为集合的成员。key 自动拼接前缀。
func (p *Proxy) SIsMember(ctx context.Context, key string, member any) *redis.BoolCmd {
	return p.rdb.SIsMember(ctx, p.key(key), member)
}

// SRem 移除集合中一个或多个成员。key 自动拼接前缀。
func (p *Proxy) SRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.rdb.SRem(ctx, p.key(key), members...)
}

// SCard 返回集合中成员的数量。key 自动拼接前缀。
func (p *Proxy) SCard(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.SCard(ctx, p.key(key))
}

// SRandMember 随机返回集合中一个成员。key 自动拼接前缀。
func (p *Proxy) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.SRandMember(ctx, p.key(key))
}

// SPop 随机移除并返回集合中一个成员。key 自动拼接前缀。
func (p *Proxy) SPop(ctx context.Context, key string) *redis.StringCmd {
	return p.rdb.SPop(ctx, p.key(key))
}

// ──────────────────────────────────────────
//  Sorted Set commands
// ──────────────────────────────────────────

// ZAdd 向有序集合添加一个或多个成员。key 自动拼接前缀。
func (p *Proxy) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return p.rdb.ZAdd(ctx, p.key(key), members...)
}

// ZScore 返回有序集合中指定成员的分值。key 自动拼接前缀。
func (p *Proxy) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	return p.rdb.ZScore(ctx, p.key(key), member)
}

// ZRank 返回有序集合中指定成员的排名（升序，从 0 开始）。key 自动拼接前缀。
func (p *Proxy) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	return p.rdb.ZRank(ctx, p.key(key), member)
}

// ZRange 按排名范围返回有序集合成员。key 自动拼接前缀。
func (p *Proxy) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return p.rdb.ZRange(ctx, p.key(key), start, stop)
}

// ZRevRange 按排名范围逆序返回有序集合成员。key 自动拼接前缀。
//
// 内部使用 ZRANGE ... REV 实现（ZREVRANGE 的现代等价形式，需要 Redis >= 6.2）。
func (p *Proxy) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return p.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:   p.key(key),
		Start: start,
		Stop:  stop,
		Rev:   true,
	})
}

// ZRangeByScore 按分值范围返回有序集合成员。key 自动拼接前缀。
//
// 内部使用 ZRANGE ... BYSCORE 实现（ZRANGEBYSCORE 的现代等价形式，需要 Redis >= 6.2）。
func (p *Proxy) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	return p.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     p.key(key),
		Start:   opt.Min,
		Stop:    opt.Max,
		ByScore: true,
		Offset:  opt.Offset,
		Count:   opt.Count,
	})
}

// ZRem 移除有序集合中一个或多个成员。key 自动拼接前缀。
func (p *Proxy) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.rdb.ZRem(ctx, p.key(key), members...)
}

// ZRemRangeByScore 按分值范围移除有序集合成员。key 自动拼接前缀。
func (p *Proxy) ZRemRangeByScore(ctx context.Context, key, minScore, maxScore string) *redis.IntCmd {
	return p.rdb.ZRemRangeByScore(ctx, p.key(key), minScore, maxScore)
}

// ZCard 返回有序集合的成员数量。key 自动拼接前缀。
func (p *Proxy) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return p.rdb.ZCard(ctx, p.key(key))
}

// ZCount 返回分值在 minScore 和 maxScore 之间的成员数量。key 自动拼接前缀。
func (p *Proxy) ZCount(ctx context.Context, key, minScore, maxScore string) *redis.IntCmd {
	return p.rdb.ZCount(ctx, p.key(key), minScore, maxScore)
}

// ZIncrBy 为有序集合中指定成员的分值增加增量。key 自动拼接前缀。
func (p *Proxy) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	return p.rdb.ZIncrBy(ctx, p.key(key), increment, member)
}

// ──────────────────────────────────────────
//  Pub/Sub commands
// ──────────────────────────────────────────

// Publish 向指定 channel 发布消息。channel 自动拼接前缀。
func (p *Proxy) Publish(ctx context.Context, channel string, message any) *redis.IntCmd {
	return p.rdb.Publish(ctx, p.key(channel), message)
}

// Subscribe 订阅一个或多个 channel。所有 channel 自动拼接前缀。
//
// 返回 [*redis.PubSub]，调用方负责关闭；如需自动管理订阅生命周期，
// 请使用 [Proxy.Consume]。
func (p *Proxy) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return p.rdb.Subscribe(ctx, p.keys(channels)...)
}

// PSubscribe 按模式订阅一个或多个 channel 模式（如 "events:*"）。
// 所有模式自动拼接前缀。
//
// 返回 [*redis.PubSub]，调用方负责关闭。
func (p *Proxy) PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	return p.rdb.PSubscribe(ctx, p.keys(patterns)...)
}

// Consume 以受管方式订阅频道并串行消费消息，阻塞直到 ctx 取消或出错。
//
// 订阅生命周期由库内管理：订阅确认失败立即返回错误，退出时自动关闭订阅；
// ctx 取消时返回 ctx 的错误（可用 errors.Is(err, context.Canceled) 判断优雅退出）；
// handler panic 会被恢复，消费终止并以 error 返回。
//
// handler 串行执行以保证单频道消息顺序，耗时处理请在业务侧自行分发。
// 注意 Redis Pub/Sub 为 at-most-once，断线期间的消息会丢失；
// 需要可靠投递请使用 Stream。所有 channel 自动拼接前缀。
func (p *Proxy) Consume(ctx context.Context, handler func(*redis.Message), channels ...string) error {
	if handler == nil {
		return errors.New("redisx: consume handler is nil")
	}
	if len(channels) == 0 {
		return errors.New("redisx: consume requires at least one channel")
	}
	return consumeSub(ctx, p.rdb.Subscribe(ctx, p.keys(channels)...), handler, channels)
}

// ConsumePattern 以受管方式按模式订阅（PSUBSCRIBE）并串行消费消息，
// 生命周期语义与 [Proxy.Consume] 完全一致。所有模式自动拼接前缀
// （如 "events:*" 实际订阅 "{prefix}:events:*"）。
func (p *Proxy) ConsumePattern(ctx context.Context, handler func(*redis.Message), patterns ...string) error {
	if handler == nil {
		return errors.New("redisx: consume handler is nil")
	}
	if len(patterns) == 0 {
		return errors.New("redisx: consume requires at least one pattern")
	}
	return consumeSub(ctx, p.rdb.PSubscribe(ctx, p.keys(patterns)...), handler, patterns)
}

// consumeSub 是 Consume / ConsumePattern 共用的受管消费循环：
// 同步确认订阅、退出关闭订阅、ctx 取消返回其错误、handler panic 转为 error。
func consumeSub(ctx context.Context, sub *redis.PubSub, handler func(*redis.Message), names []string) error {
	defer sub.Close()

	// 同步确认订阅成功，连接不可用时立即返回而非静默空转
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("redisx: subscribe %v: %w", names, err)
	}

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("redisx: consume: %w", ctx.Err())
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := safeHandle(handler, msg); err != nil {
				return err
			}
		}
	}
}

// safeHandle 执行 handler 并将 panic 转换为 error 返回。
func safeHandle(handler func(*redis.Message), msg *redis.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("redisx: consume handler panic: %v", r)
		}
	}()
	handler(msg)
	return nil
}

// ──────────────────────────────────────────
//  Scan & batch deletion
// ──────────────────────────────────────────

// DelByPattern 使用 SCAN 安全批量删除匹配 pattern 的 key。
//
// pattern 自动拼接前缀：如 pattern="user:*"，实际匹配 "{prefix}:user:*"。
// 内部使用 SCAN + UNLINK 批量删除：UNLINK 在后台异步释放内存，
// 大 value 场景不会阻塞 Redis 主线程（需要 Redis >= 4.0）。
//
// 建议调用方传入带超时的 ctx 控制执行时间。返回成功删除的 key 总数。
func (p *Proxy) DelByPattern(ctx context.Context, pattern string) (int64, error) {
	fullPattern := p.key(pattern)
	var (
		totalDeleted int64
		cursor       uint64
	)

	for {
		keys, nextCursor, err := p.rdb.Scan(ctx, cursor, fullPattern, scanBatchSize).Result()
		if err != nil {
			return totalDeleted, fmt.Errorf("redisx: scan pattern=%q: %w", fullPattern, err)
		}

		if len(keys) > 0 {
			deleted, err := p.rdb.Unlink(ctx, keys...).Result()
			if err != nil {
				return totalDeleted, fmt.Errorf("redisx: unlink batch pattern=%q: %w", fullPattern, err)
			}
			totalDeleted += deleted
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return totalDeleted, nil
}

// ScanKeys 返回自动翻页的 key 迭代器，match 自动拼接前缀，
// 产出的 key **已剥去前缀**，可直接回传本库其他带前缀方法：
//
//	for key, err := range p.ScanKeys(ctx, "user:*") {
//	    if err != nil {
//	        return err
//	    }
//	    p.Del(ctx, key) // key 不含前缀，不会二次拼接
//	}
//
// SCAN 出错时迭代产出一次非 nil error 后终止；提前 break 立即停止翻页。
// 一致性语义跟随 Redis SCAN：迭代期间新增/删除的 key 不保证快照视图。
func (p *Proxy) ScanKeys(ctx context.Context, match string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		fullMatch := p.key(match)
		trim := ""
		if p.prefix != "" {
			trim = prefixKey(p.prefix, p.prefixSeparator, "")
		}
		var cursor uint64
		for {
			keys, next, err := p.rdb.Scan(ctx, cursor, fullMatch, scanBatchSize).Result()
			if err != nil {
				yield("", fmt.Errorf("redisx: scan keys match=%q: %w", fullMatch, err))
				return
			}
			for _, k := range keys {
				if !yield(strings.TrimPrefix(k, trim), nil) {
					return
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	}
}

// Scan 包装 SCAN 命令，match pattern 自动拼接前缀。
//
// 注意：返回的 key 是已含前缀的完整 key，直接回传给本库其他带前缀方法
// （如 [Proxy.Del]）会造成二次拼接。如需对结果继续操作，请改用
// [Proxy.RawClient] 执行，或自行剥离前缀；批量删除场景请直接使用
// [Proxy.DelByPattern]。
func (p *Proxy) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return p.rdb.Scan(ctx, cursor, p.key(match), count)
}

// ──────────────────────────────────────────
//  Lua scripting
// ──────────────────────────────────────────

// Eval 执行 Lua 脚本。keys 列表中的每个 key 自动拼接前缀。
func (p *Proxy) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return p.rdb.Eval(ctx, script, p.keys(keys), args...)
}

// EvalSha 执行已缓存的 Lua 脚本。keys 列表中的每个 key 自动拼接前缀。
func (p *Proxy) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return p.rdb.EvalSha(ctx, sha1, p.keys(keys), args...)
}

// EvalScript 执行 [*redis.Script]。keys 列表中的每个 key 自动拼接前缀。
func (p *Proxy) EvalScript(ctx context.Context, script *redis.Script, keys []string, args ...any) *redis.Cmd {
	return script.Run(ctx, p.rdb, p.keys(keys), args...)
}

// ──────────────────────────────────────────
//  Pipeline
// ──────────────────────────────────────────

// Pipeline 返回 go-redis 原生 Pipeline。
//
// Pipeline 内的命令需要手动拼接前缀，可通过 [Proxy.Key] 获取带前缀的 key。
func (p *Proxy) Pipeline() redis.Pipeliner {
	return p.rdb.Pipeline()
}

// TxPipeline 返回 go-redis 事务 Pipeline (MULTI/EXEC)。
//
// Pipeline 内的命令需要手动拼接前缀，可通过 [Proxy.Key] 获取带前缀的 key。
func (p *Proxy) TxPipeline() redis.Pipeliner {
	return p.rdb.TxPipeline()
}
