package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// GetJSON 读取 key 的值并 JSON 反序列化为 T。key 自动拼接前缀。
//
// key 不存在时返回 T 零值与 [redis.Nil]（errors.Is 判断）；值不是合法
// JSON 时返回携带 key 的错误。Go 方法不支持类型参数，因此为包级函数；
// [Client] 内嵌 Proxy，可直接作为 p 传入：
//
//	user, err := redisx.GetJSON[User](ctx, c.Proxy, "user:1")
//	if errors.Is(err, redis.Nil) { /* miss */ }
func GetJSON[T any](ctx context.Context, p *Proxy, key string) (T, error) {
	var v T
	data, err := p.Get(ctx, key).Bytes()
	if err != nil {
		// redis.Nil 经 %w 透传，调用方仍用 errors.Is 判断 miss
		return v, fmt.Errorf("redisx: get json key=%q: %w", p.key(key), err)
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("redisx: get json key=%q: %w", p.key(key), err)
	}
	return v, nil
}

// SetJSON 将 value JSON 序列化后写入 key。key 自动拼接前缀，
// expiration 为 0 表示不设置过期时间。
//
// 序列化失败返回携带 key 的错误且不发送命令。
func SetJSON(ctx context.Context, p *Proxy, key string, value any, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("redisx: set json key=%q: %w", p.key(key), err)
	}
	if err := p.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("redisx: set json key=%q: %w", p.key(key), err)
	}
	return nil
}
