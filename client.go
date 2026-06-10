package redisx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
)

// scanBatchSize 是 SCAN 命令每次迭代的 COUNT 参考值。
const scanBatchSize int64 = 200

// Client 是 Redis 多 DB 客户端封装。
//
// 并发安全：内部 clients / proxies map 在 [NewClient] 中一次性构建完成后即为只读，
// 不再修改，因此并发读取无需加锁——这是比 sync.RWMutex 更轻量的方案。
// 底层每个 *redis.Client 本身也是并发安全的（go-redis 原生连接池）。
type Client struct {
	defaultDB int                   // 默认 DB 编号
	keyPrefix string                // 全局 key 前缀
	clients   map[int]*redis.Client // 初始化后只读，无需锁保护
	proxies   map[int]*Proxy        // 初始化后只读，每个 DB 对应一个缓存的 Proxy
	defProxy  *Proxy                // 默认 DB 的 Proxy 缓存，避免热路径重复分配
}

// NewClient 使用 Functional Options 创建 Redis 客户端。
//
// 初始化时会对每个 DB 执行 Ping 检查连通性，任一失败则返回错误并清理所有已创建的连接。
// clients/proxies map 在构建完成后不再修改，后续并发读取无需加锁。
//
// 用法:
//
//	c, err := redisx.NewClient(
//	    redisx.WithAddr("127.0.0.1:6379"),
//	    redisx.WithPassword("secret"),
//	    redisx.WithKeyPrefix("myapp"),
//	    redisx.WithInitDBs(0, 1, 2),
//	)
func NewClient(opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	if cfg.Addr == "" {
		return nil, errors.New("redis: addr is required")
	}
	if cfg.DefaultDB < 0 {
		return nil, fmt.Errorf("redis: invalid default db %d, must be >= 0", cfg.DefaultDB)
	}

	// 收集并去重需要初始化的 DB，同时记录 per-DB 前缀
	// key=db号, value=该 DB 的前缀（空字符串表示使用全局前缀）
	dbPrefixes := make(map[int]string)
	dbPrefixes[cfg.DefaultDB] = "" // DefaultDB 始终使用全局前缀
	for _, dc := range cfg.InitDBs {
		if dc.DB < 0 {
			return nil, fmt.Errorf("redis: invalid db %d in init list, must be >= 0", dc.DB)
		}
		// 后设置的 per-DB 前缀优先；如果已有条目且新条目有前缀则覆盖
		if dc.Prefix != "" {
			dbPrefixes[dc.DB] = dc.Prefix
		} else if _, exists := dbPrefixes[dc.DB]; !exists {
			dbPrefixes[dc.DB] = ""
		}
	}

	clients := make(map[int]*redis.Client, len(dbPrefixes))

	// 初始化失败时负责清理已成功创建的连接
	cleanup := func() {
		for _, rdb := range clients {
			_ = rdb.Close()
		}
	}

	for db := range dbPrefixes {
		rdb := redis.NewClient(&redis.Options{
			Addr:            cfg.Addr,
			Username:        cfg.Username,
			Password:        cfg.Password,
			DB:              db,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxRetries:      cfg.MaxRetries,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			ConnMaxIdleTime: cfg.IdleTimeout,
			TLSConfig:       cfg.TLSConfig,
		})

		// 每个 DB 独立超时，避免多 DB 串行 Ping 共享一个总超时导致后续 DB 被误判
		pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.DialTimeout+2*time.Second)
		err := rdb.Ping(pingCtx).Err()
		pingCancel()
		if err != nil {
			_ = rdb.Close() // 关闭当前这个也要关
			cleanup()
			return nil, fmt.Errorf("redis: ping db=%d addr=%s: %w", db, cfg.Addr, err)
		}

		clients[db] = rdb
	}

	// 构建 Proxy 缓存——初始化后只读
	proxies := make(map[int]*Proxy, len(clients))
	for db, rdb := range clients {
		prefix := dbPrefixes[db]
		if prefix == "" {
			prefix = cfg.KeyPrefix // 使用全局前缀
		}
		proxies[db] = &Proxy{rdb: rdb, prefix: prefix}
	}

	return &Client{
		defaultDB: cfg.DefaultDB,
		keyPrefix: cfg.KeyPrefix,
		clients:   clients,
		proxies:   proxies,
		defProxy:  proxies[cfg.DefaultDB],
	}, nil
}

// Close 优雅关闭所有 DB 客户端连接。
//
// 如有多个 DB 关闭失败，使用 [errors.Join] 合并返回。
func (c *Client) Close() error {
	var errs []error
	for db, rdb := range c.clients {
		if err := rdb.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db=%d: %w", db, err))
		}
	}
	return errors.Join(errs...)
}

// HealthCheck 对所有已初始化的 DB 执行 PING 健康检查。
//
// 不会短路：即使某个 DB 失败也会继续检查其余 DB，最终返回所有失败的聚合错误。
// 返回 nil 表示全部健康。
func (c *Client) HealthCheck(ctx context.Context) error {
	var errs []error
	for db, rdb := range c.clients {
		if err := rdb.Ping(ctx).Err(); err != nil {
			errs = append(errs, fmt.Errorf("db=%d ping: %w", db, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("redis: health check failed: %w", errors.Join(errs...))
	}
	return nil
}

// SelectDB 返回指定 DB 编号上的命令代理 [Proxy]，支持链式调用。
//
// 如果指定的 DB 未在 [WithInitDBs] / [WithDBConfig] 中初始化，返回错误。
func (c *Client) SelectDB(db int) (*Proxy, error) {
	if p, ok := c.proxies[db]; ok {
		return p, nil
	}
	available := slices.Sorted(maps.Keys(c.proxies))
	return nil, fmt.Errorf("redis: db=%d not initialized, available: %v", db, available)
}

// MustSelectDB 返回指定 DB 编号上的命令代理 [Proxy]。
//
// 如果 DB 未初始化，直接 panic。仅用于程序启动阶段或确定 DB 存在的场景。
func (c *Client) MustSelectDB(db int) *Proxy {
	p, err := c.SelectDB(db)
	if err != nil {
		panic(err)
	}
	return p
}

// GetClient 安全获取指定 DB 的底层 [*redis.Client]。
//
// 返回 false 表示该 DB 未初始化。用于需要直接操作 go-redis 原生 API 的场景。
func (c *Client) GetClient(db int) (*redis.Client, bool) {
	rdb, ok := c.clients[db]
	return rdb, ok
}

// DefaultClient 返回默认 DB 的底层 [*redis.Client]。
func (c *Client) DefaultClient() *redis.Client {
	return c.clients[c.defaultDB]
}

// Prefix 返回当前配置的全局 key 前缀。
func (c *Client) Prefix() string {
	return c.keyPrefix
}

// prefixKey 为 key 拼接前缀。prefix 为空时直接返回原始 key。
func prefixKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + ":" + key
}
