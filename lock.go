package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLockNotObtained 表示锁正被他人持有，本次未获得。
var ErrLockNotObtained = errors.New("redisx: lock not obtained")

// ErrLockLost 表示锁已不再被当前持有者持有（已过期或被他人重新获取）。
var ErrLockLost = errors.New("redisx: lock lost")

// ErrLockNoExpiry 表示锁存在但没有设置过期时间（契约外的永久锁）。
//
// 本库自身的获取/续期总会带 TTL，出现此错误通常意味着该 key 被外部
// PERSIST 或以无过期方式写入，属死锁隐患，调用方应显式处理而非当作正常。
var ErrLockNoExpiry = errors.New("redisx: lock has no expiry")

// acquireScript 原子获取锁：SET NX PX 成功返回 1；否则当前值等于本次 token
// （吸收底层自动重试造成的"服务端已置锁但响应丢失"）同样返回 1；否则返回 0。
var acquireScript = redis.NewScript(`
if redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
	return 1
end
if redis.call("get", KEYS[1]) == ARGV[1] then
	return 1
end
return 0`)

// fenceKeySuffix 是 fencing 计数器 key 相对锁 key 的固定后缀。计数器持久存在
// （无 TTL、Release 不删除），以保证 fencing token 跨获取严格单调递增。
const fenceKeySuffix = ":__fence__"

// fencedAcquireScript 原子获取带 fencing token 的锁：SET NX PX 成功则 INCR 计数器
// 返回新 fence；自获取重试（当前值等于本次 token）返回已分配的同一 fence 不重复计数；
// 被他人持有返回 -1。KEYS[1]=锁 key，KEYS[2]=计数器 key，ARGV[1]=token，ARGV[2]=ttl_ms。
var fencedAcquireScript = redis.NewScript(`
if redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
	return redis.call("incr", KEYS[2])
end
if redis.call("get", KEYS[1]) == ARGV[1] then
	return tonumber(redis.call("get", KEYS[2]))
end
return -1`)

// unlockScript 校验 token 匹配后原子删除，杜绝误删他人锁。
var unlockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0`)

// refreshScript 校验 token 匹配后原子续期。
var refreshScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0`)

// ttlScript 校验 token 匹配后原子返回剩余 TTL（毫秒）；不匹配返回 -3 哨兵。
var ttlScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pttl", KEYS[1])
end
return -3`)

func validateLockTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("redisx: lock ttl must be positive, got %v", ttl)
	}
	if ttl < time.Millisecond {
		return fmt.Errorf("redisx: lock ttl must be at least %s, got %v", time.Millisecond, ttl)
	}
	return nil
}

// newLockToken 生成 16 字节随机十六进制 token，作为锁值用于防误删与自获取识别。
func newLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("redisx: generate lock token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// acquireLock 以指定 token 原子获取锁，返回是否获得。
//
// 抽出为非导出函数便于以固定 token 单测三分支（新获取 / 自获取重试幂等 /
// 被他人持有）；生产路径由 [Proxy.TryLock] 传入随机 token 调用。
func acquireLock(ctx context.Context, rdb *redis.Client, key, token string, ttl time.Duration) (bool, error) {
	n, err := acquireScript.Run(ctx, rdb, []string{key}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Lock 表示一把已持有的单实例分布式锁，由 [Proxy.TryLock] 获取。
//
// 注意语义边界：这是单 Redis 实例锁（非 RedLock），主从异步复制下
// 故障切换瞬间存在双持有的理论窗口，关键互斥请在业务层做幂等兜底。
type Lock struct {
	rdb   *redis.Client
	key   string // 已拼前缀的完整 key
	token string
}

// TryLock 尝试获取分布式锁，非阻塞。key 自动拼接前缀。
//
// 成功返回 [*Lock]；锁被他人持有返回 [ErrLockNotObtained]（可 errors.Is 判断）；
// ttl 必须至少为 1ms（Redis TTL 精度为毫秒；无 TTL 的锁等于死锁隐患）。
// 需要阻塞等待的场景由调用方按业务节奏循环 TryLock。
//
// 获取经单段 Lua 完成并对底层自动重试幂等：若 SET NX 因响应丢失被重发，
// 重发时锁值仍等于本次 token 亦视为获得，不会把自己已持有的锁误报为被他人持有。
func (p *Proxy) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	if err := validateLockTTL(ttl); err != nil {
		return nil, err
	}

	token, err := newLockToken()
	if err != nil {
		return nil, err
	}

	fullKey := p.key(key)
	ok, err := acquireLock(ctx, p.rdb, fullKey, token, ttl)
	if err != nil {
		return nil, fmt.Errorf("redisx: try lock key=%q: %w", fullKey, err)
	}
	if !ok {
		return nil, fmt.Errorf("redisx: lock key=%q held by another: %w", fullKey, ErrLockNotObtained)
	}
	return &Lock{rdb: p.rdb, key: fullKey, token: token}, nil
}

// WithLock 获取锁后执行 fn，并保证释放（含 fn panic 路径），消除手写
// TryLock + defer Release 样板与漏释放风险。key 自动拼接前缀。
//
// 锁被他人持有时返回 [ErrLockNotObtained]（errors.Is 判断），fn 不执行；
// fn 的错误原样传出；释放阶段发现锁已失去（fn 执行超过 ttl，互斥可能已被
// 破坏）时 [ErrLockLost] 经 [errors.Join] 并入返回错误——调用方应检查
// errors.Is(err, ErrLockLost) 并触发业务侧补偿。fn panic 时锁仍被释放，
// panic 继续向上传播。
//
// 不做自动续期：fn 预计耗时必须显著小于 ttl，长任务请自行分段或在 fn 内
// 通过 [Proxy.TryLock] 返回的 Lock 显式 Refresh。
func (p *Proxy) WithLock(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("redisx: with lock fn is nil")
	}
	lock, err := p.TryLock(ctx, key, ttl)
	if err != nil {
		return err
	}

	// 释放动作不随外层 ctx 取消：fn 因 ctx 取消失败时锁也应立即归还，
	// 而不是占到 ttl 自然过期
	releaseCtx := context.WithoutCancel(ctx)

	var fnErr error
	func() {
		defer func() {
			// panic 路径也要释放锁，避免 ttl 内死锁；随后继续传播 panic
			if r := recover(); r != nil {
				_ = lock.Release(releaseCtx)
				panic(r)
			}
		}()
		fnErr = fn(ctx)
	}()

	relErr := lock.Release(releaseCtx)
	if relErr == nil {
		return fnErr // 常规路径：fn 错误原样传出，不被 Join 包装
	}
	return errors.Join(fnErr, relErr)
}

// Key 返回已拼前缀的完整锁 key，供日志、打点等观测场景使用。
func (l *Lock) Key() string {
	return l.key
}

// TTL 返回锁的剩余存活时间（校验 token 后原子读取）。
//
// 返回 nil error 即表示锁仍被当前持有者持有；锁已过期或被他人重新获取
// 返回 [ErrLockLost]；token 匹配但锁无过期时间（契约外的永久锁）返回
// [ErrLockNoExpiry]。仅供观测与业务自检：查询与后续操作之间锁仍可能
// 过期，互斥正确性依赖 Release/Refresh 自身的 token 校验，而非本方法。
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	n, err := ttlScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if err != nil {
		return 0, fmt.Errorf("redisx: lock ttl key=%q: %w", l.key, err)
	}
	if n == -3 {
		return 0, ErrLockLost
	}
	if n == -1 {
		// key 存在但无过期时间（契约外的永久锁）：不伪装成 0,nil，
		// 返回专门错误让调用方感知死锁隐患
		return 0, ErrLockNoExpiry
	}
	if n < 0 {
		// 其余负值（如 -2 key 不存在）在 token 已匹配的前提下不会出现；防御性归零
		return 0, nil
	}
	return time.Duration(n) * time.Millisecond, nil
}

// Release 释放锁。仅当锁仍被当前持有者持有（token 匹配）时删除；
// 锁已过期或被他人重新获取时返回 [ErrLockLost]，且不会影响他人的锁。
func (l *Lock) Release(ctx context.Context) error {
	n, err := unlockScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("redisx: release lock key=%q: %w", l.key, err)
	}
	if n == 0 {
		return ErrLockLost
	}
	return nil
}

// Refresh 将锁的 TTL 重设为 ttl（校验 token 后原子执行）。
//
// 长任务在持有期间显式调用本方法续期；ttl 必须至少为 1ms；锁已失去返回 [ErrLockLost]。
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	if err := validateLockTTL(ttl); err != nil {
		return err
	}
	n, err := refreshScript.Run(ctx, l.rdb, []string{l.key}, l.token, ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("redisx: refresh lock key=%q: %w", l.key, err)
	}
	if n == 0 {
		return ErrLockLost
	}
	return nil
}

// FencedLock 表示一把已持有、带单调递增 fencing token 的分布式锁，由
// [Proxy.FencedLock] 获取。它嵌入 [Lock]，故 Key / Release / Refresh / TTL
// 语义与 Lock 完全一致（token 校验、防误删、过期返回 [ErrLockLost]）；额外通过
// [FencedLock.Fence] 暴露本次获取的 fencing token。
//
// 与 Lock 相同，这是单 Redis 实例锁（非 RedLock）。
type FencedLock struct {
	Lock
	fence int64
}

// Fence 返回本次获取的 fencing token。同一 key 上后获得者的 token 严格大于
// 先获得者（可能有间隙，但不回退、不重复），供下游被保护资源拒绝较旧持有者。
func (l *FencedLock) Fence() int64 {
	return l.fence
}

// FencedLock 尝试获取带 fencing token 的分布式锁，非阻塞。key 自动拼接前缀。
//
// 相比 [Proxy.TryLock]，额外返回一个在同一 key 上严格单调递增的 fencing token
// （见 [FencedLock.Fence]）：进程暂停 / GC / 网络阻塞导致旧持有者的锁在 TTL
// 过期、新持有者已拿到锁后，旧持有者仍可能继续操作外部资源；随机 token 只能防
// 误删他人锁，无法阻止这种"过期后仍写"。fencing token 的能力是——由下游被保护
// 资源记录见过的最大 token 并拒绝更小者，从而排除旧持有者。**仅当下游确实校验
// token 时栅栏才生效**，本库只负责原子生成与暴露 token。
//
// 成功返回 [*FencedLock]；锁被他人持有返回 [ErrLockNotObtained]（errors.Is 判断）；
// ttl 必须至少为 1ms。获取路径与 TryLock 一样对底层自动重试幂等。
//
// 实现说明：fencing token 由一个持久（无 TTL）的计数器 key 提供（锁 key 追加
// ":__fence__" 后缀），Release 不删除该计数器以保证跨获取单调。该计数器 key 会
// 长期存在，且可能被 [Proxy.DelByPattern] 的宽 pattern 扫到，请勿手动删除，
// 否则单调性重置。
func (p *Proxy) FencedLock(ctx context.Context, key string, ttl time.Duration) (*FencedLock, error) {
	if err := validateLockTTL(ttl); err != nil {
		return nil, err
	}

	token, err := newLockToken()
	if err != nil {
		return nil, err
	}

	fullKey := p.key(key)
	fenceKey := fullKey + fenceKeySuffix
	fence, err := fencedAcquireScript.Run(ctx, p.rdb, []string{fullKey, fenceKey}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return nil, fmt.Errorf("redisx: fenced lock key=%q: %w", fullKey, err)
	}
	if fence < 0 {
		return nil, fmt.Errorf("redisx: fenced lock key=%q held by another: %w", fullKey, ErrLockNotObtained)
	}
	return &FencedLock{Lock: Lock{rdb: p.rdb, key: fullKey, token: token}, fence: fence}, nil
}
