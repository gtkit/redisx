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
func (p *Proxy) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	if err := validateLockTTL(ttl); err != nil {
		return nil, err
	}

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("redisx: generate lock token: %w", err)
	}
	token := hex.EncodeToString(buf)

	fullKey := p.key(key)
	ok, err := p.rdb.SetNX(ctx, fullKey, token, ttl).Result()
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
// 返回 [ErrLockLost]。仅供观测与业务自检：查询与后续操作之间锁仍可能
// 过期，互斥正确性依赖 Release/Refresh 自身的 token 校验，而非本方法。
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	n, err := ttlScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if err != nil {
		return 0, fmt.Errorf("redisx: lock ttl key=%q: %w", l.key, err)
	}
	if n == -3 {
		return 0, ErrLockLost
	}
	if n < 0 {
		// -1：key 存在但无过期时间，本库的锁不会出现；防御性归零
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
