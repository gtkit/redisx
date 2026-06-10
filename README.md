# redisx

基于 [redis/go-redis v9](https://github.com/redis/go-redis) 的生产级 Redis 客户端封装。**错误全部通过返回值传递，库内不产生任何日志，外部直接依赖仅有 go-redis**。

## 特性

- 无全局变量，多实例（多服务器/多套配置）共存
- 全局 key 前缀透明拼接，业务层无感知；支持每库（per-DB）独立前缀
- 完整连接池 / 超时配置（PoolSize、MinIdleConns、DialTimeout、ReadTimeout 等）
- 支持 TLS 连接（`WithTLSConfig`，适配云厂商强制加密实例）
- `DelByPattern`：SCAN + UNLINK 批量删除，异步释放内存不阻塞 Redis 主线程
- `HealthCheck` 健康检查
- `Proxy` 命令代理覆盖 String / Hash / List / Set / ZSet / Pub-Sub / Lua / Pipeline，key 自动加前缀
- 包名 `redisx` 与 go-redis 的 `redis` 包不冲突，业务代码无需 import 别名

## 安装

```bash
go get github.com/gtkit/redisx@latest
```

要求 Go 1.26+；`DelByPattern` 依赖 UNLINK，要求 Redis >= 4.0。

## 快速开始

```go
import "github.com/gtkit/redisx"

c, err := redisx.NewClient(
    redisx.WithAddr("127.0.0.1:6379"),
    redisx.WithPassword(os.Getenv("REDIS_PASSWORD")), // 真实值从环境变量读取
    redisx.WithKeyPrefix("myapp"),                    // 全局前缀
    redisx.WithInitDBs(0, 1),                         // 初始化多个 DB
    redisx.WithDBConfig(2, "session"),                // DB2 使用独立前缀
)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

ctx := context.Background()

// 默认 DB 操作，实际 key 为 "myapp:user:1"
c.Set(ctx, "user:1", "hello", time.Hour)
val, err := c.Get(ctx, "user:1").Result()

// 切换 DB 链式调用，DB2 的 key 前缀为 "session:"
token, err := c.MustSelectDB(2).Get(ctx, "token:abc").Result()

// SCAN + UNLINK 安全批量删除
deleted, err := c.DelByPattern(ctx, "user:*")

// 健康检查
if err := c.HealthCheck(ctx); err != nil {
    log.Printf("redis unhealthy: %v", err)
}
```

### TLS 连接

```go
c, err := redisx.NewClient(
    redisx.WithAddr("redis.example.com:6380"),
    redisx.WithPassword(os.Getenv("REDIS_PASSWORD")),
    redisx.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
)
```

## 配置项（Functional Options）


| Option                     | 说明                                                           | 默认值             |
| -------------------------- | -------------------------------------------------------------- | ------------------ |
| `WithAddr(addr)`           | 服务器地址`host:port`（必填）                                  | —                 |
| `WithUsername(u)`          | Redis 6+ ACL 用户名                                            | 空（不使用）       |
| `WithPassword(p)`          | 认证密码                                                       | 空（不认证）       |
| `WithDB(db)`               | 默认 DB 编号                                                   | 0                  |
| `WithInitDBs(dbs...)`      | 初始化多个 DB（共享全局前缀）                                  | 仅默认 DB          |
| `WithDBConfig(db, prefix)` | 初始化单个 DB 并指定独立前缀                                   | —                 |
| `WithKeyPrefix(prefix)`    | 全局 key 前缀                                                  | 空（不加前缀）     |
| `WithPoolSize(n)`          | 每个 DB 的最大连接数                                           | 10                 |
| `WithMinIdleConns(n)`      | 最小空闲连接数                                                 | 3                  |
| `WithMaxRetries(n)`        | 命令失败重试次数（非幂等命令慎用，见 GoDoc）                   | 3                  |
| `WithDialTimeout(d)`       | 建连超时                                                       | 5s                 |
| `WithReadTimeout(d)`       | 读超时                                                         | 3s                 |
| `WithWriteTimeout(d)`      | 写超时                                                         | 3s                 |
| `WithIdleTimeout(d)`       | 空闲连接回收时间                                               | 5m                 |
| `WithTLSConfig(cfg)`       | TLS 配置                                                       | nil（不启用）      |
| `WithAllowPartialInit()`   | 降级模式：失败 DB 缺席集合，错误聚合返回（DefaultDB 仍须成功） | 关闭（全有或全无） |

## 多 DB 与前缀语义

- 初始化默认为**全有或全无**：任一 DB Ping 失败，整体返回错误并回收已建连接。
- 启用 `WithAllowPartialInit()` 后改为**降级模式**：失败的 DB 缺席集合，错误以 `*InitError` 与可用的 Client 一同返回（两者可同时非 nil），`errors.As` 提取后可按 DB 编号编程决策（`ie.Failed[db]`）；但 `DefaultDB` 承载 Client 级快捷方法，仍必须成功，否则整体失败。
- `SelectDB(db)` 返回错误版代理；`MustSelectDB(db)` 未初始化时 panic，仅用于启动阶段。
- 前缀优先级：`WithDBConfig` 的 per-DB 前缀 > 全局 `WithKeyPrefix` > 不加前缀。
- `GetClient(db)` / `DefaultClient()` / `Proxy.RawClient()` 返回原生 `*redis.Client`，**不带前缀拼接**。
- Pipeline 内命令需手动拼前缀：单 key 用 `Proxy.Key(k)`，多 key 用 `Proxy.Keys(k1, k2, ...)`（见 GoDoc Example）。

## Pub/Sub

```go
// 发布（channel 自动拼前缀）
c.Publish(ctx, "events:user", "user_created")

// 受管消费（推荐）：订阅确认、连接关闭、ctx 退出、panic 恢复均由库内管理
err := c.Consume(ctx, func(m *redis.Message) {
    fmt.Println(m.Channel, m.Payload)
}, "events:user")
// ctx 取消时返回 context.Canceled，可据此判断优雅退出

// 裸订阅 / 模式订阅：返回 *redis.PubSub，调用方负责 Close
sub := c.PSubscribe(ctx, "events:*")
defer sub.Close()
```

注意：Redis Pub/Sub 为 at-most-once，断线期间消息会丢失；`Consume` 的 handler 串行执行以保证单频道顺序，耗时处理请投递到业务自有 worker。

## 分布式锁

单 Redis 实例锁：SET NX + 随机 token，Lua 校验 token 后原子释放/续期，杜绝误删他人锁。

```go
lock, err := c.TryLock(ctx, "job:daily-report", 30*time.Second)
if errors.Is(err, redisx.ErrLockNotObtained) {
    return // 他人持有，按业务节奏稍后重试
}
if err != nil {
    return err
}
defer lock.Release(ctx)

// 长任务期间显式续期；锁已失去返回 ErrLockLost
if err := lock.Refresh(ctx, 30*time.Second); errors.Is(err, redisx.ErrLockLost) {
    return // 锁已过期被他人接管，停止当前工作
}
```

语义边界：非 RedLock，主从故障切换瞬间存在双持有的理论窗口，关键互斥请在业务层做幂等兜底；不提供 watchdog 自动续期（生命周期由业务显式管理）。

## Stream 消费组（可靠消息）

Pub/Sub 是 at-most-once；需要可靠投递时使用 Stream 消费组（at-least-once）：

```go
// 生产
c.XAdd(ctx, &redis.XAddArgs{Stream: "orders", Values: map[string]any{"id": 1001}})

// 受管消费：自动建组（MKSTREAM）、崩溃重启续传 pending、handler 返回 nil 即 XACK
err := c.ConsumeStream(ctx, redisx.StreamConfig{
    Stream:   "orders",
    Group:    "billing",
    Consumer: "worker-1",                  // 组内唯一（如实例 ID）
    AutoClaimMinIdle: 30 * time.Second,    // 可选：接管死消费者闲置超时的消息
}, func(m redis.XMessage) error {
    return process(m.Values) // 返回 error 则不确认，留在 pending 等待重投
})
```

语义要点：handler 返回 error 的消息留在 pending（重启续传或被 AutoClaim 接管时重投），持续失败的消息请用 `XPENDING` 监控；handler 串行执行保证顺序；panic 恢复后终止消费并以 error 返回；ctx 取消优雅退出。

## 从 gtkit/redis 迁移

### 从 v2（`github.com/gtkit/redis/v2`）迁移

API 完全同构，仅改导入路径与包选择器：

```go
// 旧
import "github.com/gtkit/redis/v2"
c, err := redis.NewClient(redis.WithAddr("127.0.0.1:6379"))

// 新
import "github.com/gtkit/redisx"
c, err := redisx.NewClient(redisx.WithAddr("127.0.0.1:6379"))
```

### 从 v1（`github.com/gtkit/redis`）迁移

```go
// v1：全局单例 + 手动拼前缀
conn, err := redis.NewCollection(
    redis.WithAddr("127.0.0.1:6379"),
    redis.WithDB(0, "test"),
    redis.WithDB(2, "prefix:test2"),
)
rdb := redis.Select(2)
rdb.Client().Set(ctx, rdb.Prefix()+"key:2", "v", 0)

// redisx：实例化 + 前缀自动拼接
c, err := redisx.NewClient(
    redisx.WithAddr("127.0.0.1:6379"),
    redisx.WithDBConfig(0, "test"),
    redisx.WithDBConfig(2, "prefix:test2"),
)
c.MustSelectDB(2).Set(ctx, "key:2", "v", 0) // 实际 key: "prefix:test2:key:2"
```


| v1                            | redisx                              |
| ----------------------------- | ----------------------------------- |
| `NewCollection(opts...)`      | `NewClient(opts...)`                |
| `WithDB(db, prefix)`          | `WithDBConfig(db, prefix)`          |
| `Select(db)`                  | `SelectDB(db)` / `MustSelectDB(db)` |
| `Client(db)`                  | `GetClient(db)`                     |
| `rdb.Prefix() + key` 手动拼接 | 自动拼接                            |
| `BatchDel(ctx, pattern)`      | `DelByPattern(ctx, pattern)`        |

注意两处行为差异：

- v1 多库初始化是**部分成功**语义（失败的库缺席集合）；redisx 默认**全有或全无**，需要 v1 语义时启用 `WithAllowPartialInit()`。
- v1 `BatchDel` 用逐 key DEL；redisx `DelByPattern` 用批量 UNLINK，返回值多了删除计数。

## 并发安全

`Client` 与 `Proxy` 在 `NewClient` 返回后内部状态只读，可在任意 goroutine 并发使用；底层 `*redis.Client` 由 go-redis 连接池保证并发安全。

## License

与 gtkit 其他包保持一致。
