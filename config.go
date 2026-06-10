package redisx

import (
	"crypto/tls"
	"time"
)

// DBConfig 定义单个 DB 的配置（编号 + 可选独立前缀）。
//
// 当 Prefix 非空时，该 DB 使用独立前缀替代全局 KeyPrefix。
// 兼容 v1 的 WithDB(0, "myprefix") 用法。
type DBConfig struct {
	DB     int
	Prefix string // 可选，非空时覆盖全局 KeyPrefix
}

// Config 定义 Redis 客户端的完整配置。
// 所有字段均有合理默认值，仅 Addr 为必填项。
type Config struct {
	// Addr 是 Redis 服务器地址，格式为 host:port。必填。
	Addr string

	// Username 用于 Redis 6+ ACL 认证。空字符串表示不使用。
	Username string

	// Password 是 Redis 认证密码。空字符串表示无需认证。
	Password string

	// DefaultDB 是默认使用的数据库编号（0~15），默认 0。
	DefaultDB int

	// InitDBs 是初始化时需要创建连接的 DB 配置列表。
	// DefaultDB 会自动包含，无需重复添加。
	InitDBs []DBConfig

	// PoolSize 是每个 DB 客户端的最大连接池大小。默认 10。
	PoolSize int

	// MinIdleConns 是连接池中保持的最小空闲连接数。默认 3。
	MinIdleConns int

	// MaxRetries 是命令失败后最大重试次数。默认 3。
	MaxRetries int

	// DialTimeout 是建立 TCP 连接的超时时间。默认 5s。
	DialTimeout time.Duration

	// ReadTimeout 是 socket 读操作超时时间。默认 3s。
	ReadTimeout time.Duration

	// WriteTimeout 是 socket 写操作超时时间。默认 3s。
	WriteTimeout time.Duration

	// IdleTimeout 是空闲连接被回收前的最大存活时间。默认 5m。
	IdleTimeout time.Duration

	// KeyPrefix 是全局 key 前缀。
	// 设置后所有命令的 key 会自动拼接为 "{KeyPrefix}:{key}"。
	// 可被 DBConfig.Prefix 覆盖。空字符串表示不使用前缀。
	KeyPrefix string

	// TLSConfig 是连接 Redis 的 TLS 配置。nil 表示不启用 TLS。
	TLSConfig *tls.Config

	// AllowPartialInit 允许部分 DB 初始化失败（降级模式）。
	// 默认 false：任一 DB 失败则整体失败（全有或全无）。
	AllowPartialInit bool
}

// defaultConfig 返回包含生产合理默认值的 Config。
func defaultConfig() *Config {
	return &Config{
		DefaultDB:    0,
		PoolSize:     10,
		MinIdleConns: 3,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		IdleTimeout:  5 * time.Minute,
	}
}

// Option 是 Functional Options 模式的配置函数。
type Option func(*Config)

// WithAddr 设置 Redis 服务器地址（必填）。
//
// 格式: "host:port"，例如 "127.0.0.1:6379"。
func WithAddr(addr string) Option {
	return func(c *Config) { c.Addr = addr }
}

// WithUsername 设置 Redis 6+ ACL 认证用户名。
func WithUsername(username string) Option {
	return func(c *Config) { c.Username = username }
}

// WithPassword 设置 Redis 认证密码。
func WithPassword(password string) Option {
	return func(c *Config) { c.Password = password }
}

// WithDB 设置默认使用的数据库编号（0~15）。
func WithDB(db int) Option {
	return func(c *Config) { c.DefaultDB = db }
}

// WithInitDBs 设置需要初始化的多个 DB 编号。
//
// DefaultDB 会自动包含在列表中，无需重复添加。
// 所有 DB 共享全局 KeyPrefix。如需 per-DB 前缀，请使用 [WithDBConfig]。
//
// 示例: WithInitDBs(0, 1, 2) 将同时初始化 DB0、DB1、DB2。
func WithInitDBs(dbs ...int) Option {
	return func(c *Config) {
		for _, db := range dbs {
			c.InitDBs = append(c.InitDBs, DBConfig{DB: db})
		}
	}
}

// WithDBConfig 添加一个带独立前缀的 DB 配置。
//
// 当 prefix 非空时，该 DB 使用独立前缀替代全局 KeyPrefix。
// 兼容 v1 的 WithDB(db, "prefix") 语义。
//
// 示例: WithDBConfig(2, "session") 使 DB2 的 key 前缀为 "session:" 而非全局前缀。
func WithDBConfig(db int, prefix string) Option {
	return func(c *Config) {
		c.InitDBs = append(c.InitDBs, DBConfig{DB: db, Prefix: prefix})
	}
}

// WithPoolSize 设置每个 DB 连接池的最大连接数。
func WithPoolSize(size int) Option {
	return func(c *Config) { c.PoolSize = size }
}

// WithMinIdleConns 设置连接池中保持的最小空闲连接数。
func WithMinIdleConns(n int) Option {
	return func(c *Config) { c.MinIdleConns = n }
}

// WithMaxRetries 设置命令失败后最大重试次数。
//
// 注意：对非幂等命令（如 INCR、LPUSH），读超时后的自动重试可能导致
// 命令被重复执行。对此类命令敏感的场景请设置为 0 关闭重试，
// 或在业务层使用 Lua 脚本保证幂等。
func WithMaxRetries(n int) Option {
	return func(c *Config) { c.MaxRetries = n }
}

// WithDialTimeout 设置建立 TCP 连接的超时时间。
func WithDialTimeout(d time.Duration) Option {
	return func(c *Config) { c.DialTimeout = d }
}

// WithReadTimeout 设置 socket 读操作超时时间。
func WithReadTimeout(d time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = d }
}

// WithWriteTimeout 设置 socket 写操作超时时间。
func WithWriteTimeout(d time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = d }
}

// WithIdleTimeout 设置空闲连接被回收前的最大存活时间。
func WithIdleTimeout(d time.Duration) Option {
	return func(c *Config) { c.IdleTimeout = d }
}

// WithKeyPrefix 设置全局 key 前缀。
//
// 设置后所有带 key 参数的命令会自动拼接为 "{prefix}:{key}"，
// 对业务层完全透明。可被 per-DB 前缀覆盖（见 [WithDBConfig]）。
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.KeyPrefix = prefix }
}

// WithTLSConfig 设置连接 Redis 的 TLS 配置。
//
// 传入非 nil 配置后，所有 DB 的连接均通过 TLS 建立，
// 适用于云厂商强制加密的 Redis 实例。nil 表示不启用 TLS。
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *Config) { c.TLSConfig = tlsConfig }
}

// WithAllowPartialInit 允许部分 DB 初始化失败（降级模式）。
//
// 启用后，初始化失败的 DB 不进入集合，所有失败聚合后与可用的 Client
// 一同返回（两者可同时非 nil），对缺席 DB 调用 [Client.SelectDB] 返回错误；
// DefaultDB 承载 Client 级快捷方法，仍必须初始化成功，否则整体失败。
// 默认关闭，即任一 DB 失败则整体失败（全有或全无）。
func WithAllowPartialInit() Option {
	return func(c *Config) { c.AllowPartialInit = true }
}
