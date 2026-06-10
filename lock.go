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
// ttl 必须大于 0（无 TTL 的锁等于死锁隐患）。
// 需要阻塞等待的场景由调用方按业务节奏循环 TryLock。
func (p *Proxy) TryLock(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("redisx: lock ttl must be positive, got %v", ttl)
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
		return nil, ErrLockNotObtained
	}
	return &Lock{rdb: p.rdb, key: fullKey, token: token}, nil
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
// 长任务在持有期间显式调用本方法续期；锁已失去返回 [ErrLockLost]。
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("redisx: lock ttl must be positive, got %v", ttl)
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
