package redisx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// scanBatchSize 是 SCAN 命令每次迭代的 COUNT 参考值。
const scanBatchSize int64 = 200

// Client 是 Redis 多 DB 客户端封装。
//
// 嵌入的 [*Proxy] 是默认 DB 的命令代理，其全部命令方法（Get/Set/HSet/
// ConsumeStream/TryLock 等）经方法提升直接在 Client 上可用，等价于
// MustSelectDB(defaultDB) 上的同名调用，key 自动拼接前缀。
//
// 并发安全：内部 clients / proxies map 在 [NewClient] 中一次性构建完成后即为只读，
// 不再修改，因此并发读取无需加锁——这是比 sync.RWMutex 更轻量的方案。
// 底层每个 *redis.Client 本身也是并发安全的（go-redis 原生连接池）。
type Client struct {
	*Proxy // 默认 DB 的命令代理，方法提升为 Client 级快捷方式

	defaultDB int                   // 默认 DB 编号
	keyPrefix string                // 全局 key 前缀
	clients   map[int]*redis.Client // 初始化后只读，无需锁保护
	proxies   map[int]*Proxy        // 初始化后只读，每个 DB 对应一个缓存的 Proxy
}

// NewClient 使用 Functional Options 创建 Redis 客户端。
//
// 等价于以 context.Background() 调用 [NewClientContext]；需要约束或取消
// 初始化耗时的场景请直接使用 [NewClientContext]。
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
	return NewClientContext(context.Background(), opts...)
}

// NewClientContext 与 [NewClient] 相同，但初始化拨号与 Ping 均受传入 ctx 约束：
// ctx 取消或超时立即中止初始化并回收已建连接。
//
// 初始化对每个 DB 执行 Ping 检查连通性：DefaultDB 优先拨号，失败立即整体失败且
// 不再拨号其余 DB；其余 DB 并发拨号以缩短启动时间。非降级模式下任一失败即返回
// 确定性错误（编号最小的失败 DB）并清理所有已创建的连接。启用 [WithAllowPartialInit]
// 后改为降级语义：失败的 DB 缺席集合，错误以 [*InitError] 返回（与可用的 Client
// 可同时非 nil，可经 errors.As 按 DB 提取失败原因，且与 DB 编号确定对应、不受并发
// 完成顺序影响），但 DefaultDB 仍必须初始化成功，否则整体失败返回 nil。
// clients/proxies map 在构建完成后不再修改，后续并发读取无需加锁。
func NewClientContext(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	if cfg.Addr == "" {
		return nil, errors.New("redisx: addr is required")
	}
	if cfg.DefaultDB < 0 {
		return nil, fmt.Errorf("redisx: invalid default db %d, must be >= 0", cfg.DefaultDB)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// 收集并去重需要初始化的 DB，同时记录 per-DB 前缀
	// key=db号, value=该 DB 的前缀（空字符串表示使用全局前缀）
	dbPrefixes := make(map[int]string)
	dbPrefixes[cfg.DefaultDB] = "" // DefaultDB 始终使用全局前缀
	for _, dc := range cfg.InitDBs {
		if dc.DB < 0 {
			return nil, fmt.Errorf("redisx: invalid db %d in init list, must be >= 0", dc.DB)
		}
		// 后设置的 per-DB 前缀优先；如果已有条目且新条目有前缀则覆盖
		if dc.Prefix != "" {
			dbPrefixes[dc.DB] = dc.Prefix
		} else if _, exists := dbPrefixes[dc.DB]; !exists {
			dbPrefixes[dc.DB] = ""
		}
	}

	clients, failed, err := dialAll(ctx, cfg, dbPrefixes)
	if err != nil {
		return nil, err
	}

	// 构建 Proxy 缓存——初始化后只读
	proxies := make(map[int]*Proxy, len(clients))
	for db, rdb := range clients {
		prefix := dbPrefixes[db]
		if prefix == "" {
			prefix = cfg.KeyPrefix // 使用全局前缀
		}
		proxies[db] = &Proxy{
			rdb:                    rdb,
			prefix:                 prefix,
			prefixSeparator:        cfg.KeyPrefixSeparator,
			channelPrefix:          cfg.ChannelPrefix,
			channelPrefixSeparator: cfg.ChannelPrefixSeparator,
		}
	}

	c := &Client{
		Proxy:     proxies[cfg.DefaultDB],
		defaultDB: cfg.DefaultDB,
		keyPrefix: cfg.KeyPrefix,
		clients:   clients,
		proxies:   proxies,
	}
	if len(failed) > 0 {
		return c, &InitError{Failed: failed}
	}
	return c, nil
}

// normalizeMaxRetries 映射本库语义到 go-redis：本库 MaxRetries=0 表示关闭重试，
// 而 go-redis 中 0 是"默认 3 次"、-1 才是关闭。
func normalizeMaxRetries(n int) int {
	if n == 0 {
		return -1
	}
	return n
}

// dialDB 为单个 DB 建立连接并在 ctx 约束下 Ping 验证；失败时关闭连接并返回错误。
//
// 每个 DB 从 ctx 派生独立超时上限，避免多 DB 共享一个总超时导致后续 DB 被误判。
// 启用 TLS 时使用 cfg.TLSConfig 的独立副本，避免多个 client 共享同一 *tls.Config
// 被 go-redis 拨号期写入字段而相互影响。
func dialDB(ctx context.Context, cfg *Config, db int) (*redis.Client, error) {
	var tlsCfg *tls.Config
	if cfg.TLSConfig != nil {
		tlsCfg = cfg.TLSConfig.Clone()
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              db,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      normalizeMaxRetries(cfg.MaxRetries),
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		ConnMaxIdleTime: cfg.IdleTimeout,
		TLSConfig:       tlsCfg,
	})

	pingCtx, pingCancel := context.WithTimeout(ctx, cfg.DialTimeout+2*time.Second)
	pingFail := rdb.Ping(pingCtx).Err()
	pingCancel()
	if pingFail != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redisx: ping db=%d addr=%s: %w", db, cfg.Addr, pingFail)
	}
	return rdb, nil
}

// dialAll 为每个 DB 建立连接并 Ping 验证。
//
// DefaultDB 优先同步拨号：它承载 Client 级快捷方法必须成功，失败时立即 fail-fast，
// 其余 DB 无需再拨号。其余 DB 并发拨号以缩短启动时间；结果按 DB 编号确定性聚合，
// 不依赖并发完成顺序。返回成功的连接集合与降级模式下各 DB 的失败原因；非降级模式
// 下任一非默认 DB 失败时关闭全部已建连接并返回编号最小失败 DB 的确定性错误。
func dialAll(ctx context.Context, cfg *Config, dbPrefixes map[int]string) (clients map[int]*redis.Client, failed map[int]error, err error) {
	clients = make(map[int]*redis.Client, len(dbPrefixes))
	cleanup := func() {
		for _, rdb := range clients {
			_ = rdb.Close()
		}
	}

	// DefaultDB 优先同步拨号，失败立即整体 fail-fast、不拨其余
	defaultRDB, derr := dialDB(ctx, cfg, cfg.DefaultDB)
	if derr != nil {
		return nil, nil, derr
	}
	clients[cfg.DefaultDB] = defaultRDB

	// 其余 DB 升序收集后并发拨号
	rest := make([]int, 0, len(dbPrefixes))
	for _, db := range slices.Sorted(maps.Keys(dbPrefixes)) {
		if db != cfg.DefaultDB {
			rest = append(rest, db)
		}
	}
	if len(rest) == 0 {
		return clients, nil, nil
	}

	type dialResult struct {
		db  int
		rdb *redis.Client
		err error
	}
	// 每个 goroutine 只写自己的结果槽（按 rest 升序索引），wg.Wait 后串行聚合，
	// 无共享写、无需锁；聚合顺序由 rest 决定，与完成顺序无关
	results := make([]dialResult, len(rest))
	var wg sync.WaitGroup
	for i, db := range rest {
		wg.Add(1)
		go func(i, db int) {
			defer wg.Done()
			rdb, e := dialDB(ctx, cfg, db)
			results[i] = dialResult{db: db, rdb: rdb, err: e}
		}(i, db)
	}
	wg.Wait()

	var firstErr error // rest 升序 → 首个错误即编号最小失败
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			// 降级模式下非默认 DB 失败只记录不中断
			if cfg.AllowPartialInit {
				if failed == nil {
					failed = make(map[int]error)
				}
				failed[r.db] = r.err
			}
			continue
		}
		clients[r.db] = r.rdb
	}

	// 非降级模式：任一非默认 DB 失败则整体失败，回收全部已建连接（含 DefaultDB 与
	// 已成功的非默认 DB），返回确定性错误
	if !cfg.AllowPartialInit && firstErr != nil {
		cleanup()
		return nil, nil, firstErr
	}

	return clients, failed, nil
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

// HealthCheck 对所有已初始化的 DB 并发执行 PING 健康检查。
//
// 不会短路：即使某个 DB 失败也会检查其余 DB，最终以 [errors.Join] 返回所有失败的
// 聚合错误（按 DB 编号确定有序，不受并发完成顺序影响）。返回 nil 表示全部健康。
func (c *Client) HealthCheck(ctx context.Context) error {
	dbs := slices.Sorted(maps.Keys(c.clients))
	// 每个 goroutine 只写自己的错误槽（按 dbs 升序索引），wg.Wait 后聚合，无共享写
	errs := make([]error, len(dbs))
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(i, db int) {
			defer wg.Done()
			if err := c.clients[db].Ping(ctx).Err(); err != nil {
				errs[i] = fmt.Errorf("db=%d ping: %w", db, err)
			}
		}(i, db)
	}
	wg.Wait()

	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("redisx: health check failed: %w", joined)
	}
	return nil
}

// SelectDB 返回指定 DB 编号上的命令代理 [Proxy]，支持链式调用。
//
// 如果指定的 DB 未在 [WithInitDBs] / [WithInitDBPrefix] 中初始化，返回错误。
func (c *Client) SelectDB(db int) (*Proxy, error) {
	if p, ok := c.proxies[db]; ok {
		return p, nil
	}
	available := slices.Sorted(maps.Keys(c.proxies))
	return nil, fmt.Errorf("redisx: db=%d not initialized, available: %v", db, available)
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

// AddHook 将 hook 安装到所有已初始化 DB 的底层 [*redis.Client] 上。
//
// 用于一次性挂接熔断、限流、metrics、tracing 等中间件，库本身不内置任何策略。
// hook 为 nil 时静默忽略。部分初始化（[WithAllowPartialInit]）模式下，
// 初始化失败而缺席的 DB 自然跳过。
//
// 调用时机：与 go-redis 原生 AddHook 的约束一致，必须在 [NewClient] 之后、
// 开始并发执行命令之前完成安装，运行中途添加不保证并发安全。
// 如需只对个别 DB 安装，请改用 [Client.GetClient] 自行处理。
func (c *Client) AddHook(hook redis.Hook) {
	if hook == nil {
		return
	}
	for _, rdb := range c.clients {
		rdb.AddHook(hook)
	}
}

// Prefix 返回当前配置的全局 key 前缀。
func (c *Client) Prefix() string {
	return c.keyPrefix
}

// PoolStats 返回每个已初始化 DB 的连接池统计，key 为 DB 编号。
//
// 透传 go-redis 的连接池统计（命中/未命中/超时/连接数等），供业务接入
// 监控拉取；库本身不做任何聚合、阈值或告警判断。
func (c *Client) PoolStats() map[int]*redis.PoolStats {
	stats := make(map[int]*redis.PoolStats, len(c.clients))
	for db, rdb := range c.clients {
		stats[db] = rdb.PoolStats()
	}
	return stats
}

// prefixKey 为 key 拼接前缀。prefix 为空时直接返回原始 key。
func prefixKey(prefix, separator, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + separator + key
}
