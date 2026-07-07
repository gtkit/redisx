# redisx

基于 [redis/go-redis v9](https://github.com/redis/go-redis) 的生产级 Redis 客户端封装。

**设计原则**：错误全部通过返回值传递，库内不产生任何日志；不内置熔断、降级、重试编排等策略，只提供扩展点；核心 Redis 能力基于 go-redis，JSON 助手使用 gtkit/json/v2。

## 特性

- 无全局变量，多实例（多服务器/多套配置）共存
- 全局 key 前缀透明拼接，业务层无感知；支持每库（per-DB）独立前缀和自定义前缀连接符
- 完整连接池 / 超时配置（PoolSize、MinIdleConns、DialTimeout、ReadTimeout 等）
- 支持 TLS 连接（`WithTLSConfig`，适配云厂商强制加密实例）
- 可选降级初始化（`WithAllowPartialInit`），失败 DB 经 `InitError` 按编号提取
- `Proxy` 代理 String / Hash / List / Set / ZSet / Pub-Sub / Stream / Lua / Pipeline 的**常用命令子集**（非全量 Redis 命令），key 自动加前缀；完整命令面经 `RawClient()` / `GetClient(db)` 直接使用 go-redis；`Client` 内嵌默认 DB 代理，全部代理命令直接可用
- `Consume` / `ConsumePattern` 受管订阅消费，`ConsumeStream` 消费组可靠消费（at-least-once）
- 单实例分布式锁：`TryLock` / `Release` / `Refresh` / `TTL`，Lua 校验 token 杜绝误删
- `DelByPattern`：SCAN + UNLINK 批量删除，异步释放内存不阻塞 Redis 主线程
- `HealthCheck` 健康检查、`PoolStats` 连接池统计透传，监控出口齐备
- `AddHook` 一次调用为所有 DB 挂接 go-redis Hook，外接熔断 / 限流 / metrics / tracing
- 包名 `redisx` 与 go-redis 的 `redis` 包不冲突，业务代码无需 import 别名

## 安装

```bash
go get github.com/gtkit/redisx@latest
```

要求 Go 1.26+、Redis >= 4.0（`DelByPattern` 依赖 UNLINK）；`GetSet`、`ZRevRange`、`ZRangeByScore`、`XAUTOCLAIM` 等现代命令实现需要 Redis >= 6.2。

## 快速开始

```go
import "github.com/gtkit/redisx"

c, err := redisx.NewClient(
    redisx.WithAddr("127.0.0.1:6379"),
    redisx.WithPassword(os.Getenv("REDIS_PASSWORD")), // 真实值从环境变量读取
    redisx.WithKeyPrefix("myapp"),                    // 全局前缀
    redisx.WithInitDBs(0, 1),                         // 初始化多个 DB
    redisx.WithInitDBPrefix(2, "session"),            // DB2 使用独立前缀
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
```

---

## 初始化与配置

### 全部配置项（Functional Options）

| Option                     | 说明                                                           | 默认值             |
| -------------------------- | -------------------------------------------------------------- | ------------------ |
| `WithAddr(addr)`           | 服务器地址`host:port`（必填）                                  | —                 |
| `WithUsername(u)`          | Redis 6+ ACL 用户名                                            | 空（不使用）       |
| `WithPassword(p)`          | 认证密码                                                       | 空（不认证）       |
| `WithDB(db)`               | 默认 DB 编号                                                   | 0                  |
| `WithInitDBs(dbs...)`      | 初始化多个 DB（共享全局前缀）                                  | 仅默认 DB          |
| `WithInitDBPrefix(db, prefix)` | 初始化单个 DB 并指定独立前缀                              | —                 |
| `WithDBConfig(db, prefix)` | 已废弃兼容别名，等价于 `WithInitDBPrefix`                       | —                 |
| `WithKeyPrefix(prefix)`    | 全局 key 前缀                                                  | 空（不加前缀）     |
| `WithKeyPrefixSeparator(s)` | key 前缀与原始 key 之间的连接符                                | `:`                |
| `WithChannelPrefix(prefix)` | Pub/Sub channel 前缀（独立于 key 前缀）                         | 空（不加前缀）     |
| `WithChannelPrefixSeparator(s)` | channel 前缀与原始 channel 之间的连接符                    | `:`                |
| `WithPoolSize(n)`          | 每个 DB 的最大连接数                                           | 10                 |
| `WithMinIdleConns(n)`      | 最小空闲连接数                                                 | 3                  |
| `WithMaxRetries(n)`        | 命令失败重试次数（非幂等命令慎用，见下文）                     | 3                  |
| `WithDialTimeout(d)`       | 建连超时                                                       | 5s                 |
| `WithReadTimeout(d)`       | 读超时                                                         | 3s                 |
| `WithWriteTimeout(d)`      | 写超时                                                         | 3s                 |
| `WithIdleTimeout(d)`       | 空闲连接回收时间                                               | 5m                 |
| `WithTLSConfig(cfg)`       | TLS 配置                                                       | nil（不启用）      |
| `WithAllowPartialInit()`   | 降级模式：失败 DB 缺席集合，错误聚合返回（DefaultDB 仍须成功） | 关闭（全有或全无） |

**关于 `WithMaxRetries`**：对非幂等命令（如 `INCR`、`LPUSH`），读超时后的自动重试可能导致命令被重复执行。对此敏感的场景请设置为 0 关闭重试，或在业务层用 Lua 脚本保证幂等。

### 初始化语义

- `NewClient` 会对每个声明的 DB 建立独立连接池并执行 PING 验证，**DefaultDB 优先拨号**：它承载 Client 级快捷方法，失败时立即整体失败，其余 DB 不再拨号；其余 DB 按编号升序拨号，错误信息确定有序。
- 默认**全有或全无**：任一 DB 验证失败，整体返回错误并回收已建连接。
- `clients` / `proxies` 集合在构建完成后只读，后续并发使用无需加锁。

### 降级初始化与 InitError

启用 `WithAllowPartialInit()` 后，非默认 DB 初始化失败不再阻断整体：失败的 DB 缺席集合，错误以 `*InitError` 与**可用的** Client 一同返回（两者可同时非 nil）：

```go
c, err := redisx.NewClient(
    redisx.WithAddr("127.0.0.1:6379"),
    redisx.WithInitDBs(0, 1, 2),
    redisx.WithAllowPartialInit(),
)
// ⚠️ 降级模式下 c 与 err 可同时非 nil！
// 不要写惯用的 `if err != nil { return nil, err }`——那会把可用的 Client 丢掉。
// 先用 errors.As 判断是否为部分失败，再决定降级使用还是拒绝启动。
var ie *redisx.InitError
if errors.As(err, &ie) {
    for db, cause := range ie.Failed {
        log.Printf("db=%d 初始化失败: %v", db, cause) // 按业务决定降级还是拒绝启动
    }
}
if c == nil {
    log.Fatal(err) // DefaultDB 失败时 Client 为 nil，整体不可用
}
```

`InitError.Unwrap()` 返回各 DB 的底层错误，`errors.Is` 可穿透到网络层错误。对缺席 DB 调用 `SelectDB` 返回错误。

### TLS 连接

```go
c, err := redisx.NewClient(
    redisx.WithAddr("redis.example.com:6380"),
    redisx.WithPassword(os.Getenv("REDIS_PASSWORD")),
    redisx.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
)
```

---

## Key 前缀机制

设置 `WithKeyPrefix("myapp")` 后，所有带 key 参数的命令默认自动拼接为 `myapp:{key}`，业务层无感知。可通过 `WithKeyPrefixSeparator(".")` 改为 `myapp.{key}` 等自定义格式。

**前缀优先级**：`WithInitDBPrefix` 的 per-DB 前缀 > 全局 `WithKeyPrefix` > 不加前缀。

Pub/Sub channel 是 Redis 实例级全局命名空间，不随 DB 切换；`WithKeyPrefix` 不影响 channel。如需隔离 topic，请使用 `WithChannelPrefix("myapp")`。

**不拼前缀的出口**（需要自己负责完整 key）：

- `GetClient(db)` / `DefaultClient()` / `Proxy.RawClient()` 返回原生 `*redis.Client`；
- `Pipeline()` / `TxPipeline()` 内的命令（见 [Pipeline 与事务](#pipeline-与事务)）；
- Lua 脚本体内自行构造的 key（`Eval` 系列只对 keys 参数列表拼前缀）。

**陷阱**：`Scan` 返回的 key **已含前缀**，直接回传给本库其他带前缀方法（如 `Del`）会二次拼接。**遍历 key 请优先用 `ScanKeys`**——自动翻页且产出已剥前缀的 key，可直接回传：

```go
for key, err := range c.ScanKeys(ctx, "user:*") {
    if err != nil {
        return err
    }
    c.Del(ctx, key) // key 不含前缀，不会二次拼接
}
```

批量删除直接用 `DelByPattern`。

手动拼接工具（Pipeline 等场景）：

```go
p := c.MustSelectDB(0)
full := p.Key("user:1")           // "myapp:user:1"
fulls := p.Keys("k1", "k2")       // ["myapp:k1", "myapp:k2"]
prefix := c.Prefix()              // "myapp"
```

### 同一 DB 多命名空间

同一个 DB 里可以复用同一连接池派生多套 key 命名空间；Pub/Sub channel 仍按独立 channel 前缀处理：

```go
db2 := c.MustSelectDB(2)

cache, err := db2.WithPrefix("cache")
if err != nil {
    return err
}
triggers, err := db2.WithPrefix("trigger")
if err != nil {
    return err
}

cache.Set(ctx, "user:42", payload, time.Minute)      // Redis key: cache:user:42
triggers.Set(ctx, "user:42", "refresh", time.Minute) // Redis key: trigger:user:42

// channel 不随 key prefix 变化；需要 topic 隔离请在 NewClient 时配置 WithChannelPrefix。
db2.Publish(ctx, "events:user", "changed")
```

---

## 多 DB 使用

```go
p, err := c.SelectDB(1)   // DB 未初始化时返回错误（错误信息列出可用 DB）
p := c.MustSelectDB(1)    // 未初始化时 panic，仅用于启动阶段或确定存在的场景

rdb, ok := c.GetClient(1) // 原生 *redis.Client，不拼前缀
rdb := c.DefaultClient()  // 默认 DB 的原生客户端
```

`Client` 内嵌默认 DB 的 `Proxy`，**`Proxy` 的全部命令方法在 Client 上直接可用**，等价于 `c.MustSelectDB(defaultDB).Xxx(...)`。每个 DB 的 `Proxy` 在初始化时缓存，重复获取无额外分配。

需要连接**多个 Redis 服务器**时，分别 `NewClient` 即可，实例间完全独立（无全局变量）。

---

## 常用命令

所有命令返回 go-redis 原生的 `*redis.XxxCmd`，用 `.Result()` / `.Val()` / `.Err()` 取值；key 一律自动拼前缀。

### 错误处理

key 不存在时 `Get`、`HGet` 等返回 `redis.Nil`，这是"未命中"而非故障，务必区分：

```go
val, err := c.Get(ctx, "user:1").Result()
switch {
case errors.Is(err, redis.Nil):
    // 缓存未命中，回源
case err != nil:
    // 真实错误（网络/超时），按业务决定降级或上报
default:
    // 命中
}
```

库内不产生日志，所有失败都经返回值传出，日志策略完全由调用方决定。

### String / 计数器

```go
c.Set(ctx, "k", "v", time.Hour)        // expiration 为 0 表示不过期
c.SetEX(ctx, "k", "v", time.Hour)      // SET 带过期（SETEX 现代等价）
ok, _ := c.SetNX(ctx, "k", "v", ttl).Result() // 不存在才写，true=写入成功
old, _ := c.GetSet(ctx, "k", "new").Result()  // 写新值返回旧值（SET..GET，Redis>=6.2）
v, _ := c.GetDel(ctx, "k").Result()    // 取值并删除

c.Incr(ctx, "counter")                 // +1
c.IncrBy(ctx, "counter", 10)
c.IncrByFloat(ctx, "price", 0.5)
c.Decr(ctx, "counter")
c.DecrBy(ctx, "counter", 10)

vals, _ := c.MGet(ctx, "k1", "k2").Result()       // 批量取，缺失项为 nil
c.MSet(ctx, "k1", "v1", "k2", "v2")               // 交替 key-value，长度必须为偶数且 key 位必须为 string
```

结构体值用 JSON 泛型助手（包级函数，`Client` 内嵌 `Proxy` 直接传）：

```go
redisx.SetJSON(ctx, c.Proxy, "user:1", User{Name: "alice"}, time.Hour)

u, err := redisx.GetJSON[User](ctx, c.Proxy, "user:1")
if errors.Is(err, redis.Nil) { /* key 不存在，u 为零值 */ }
```

非 JSON 编码可显式存取 bytes，或提供业务自己的 Codec（例如 protobuf/gob/msgpack 适配器；redisx 不引入这些依赖）：

```go
_ = redisx.SetBytes(ctx, c.Proxy, "payload:1", data, time.Hour)
data, err := redisx.GetBytes(ctx, c.Proxy, "payload:1")

_ = redisx.SetCodec(ctx, c.Proxy, "user:pb:1", protoCodec, user, time.Hour)
user, err := redisx.GetCodec[User](ctx, c.Proxy, "user:pb:1", protoCodec)
```

### Key 管理

```go
c.Del(ctx, "k1", "k2")                  // 返回删除数量
n, _ := c.Exists(ctx, "k1", "k2").Result() // 返回存在数量
c.Expire(ctx, "k", time.Hour)
c.ExpireAt(ctx, "k", deadline)
c.Persist(ctx, "k")                     // 移除过期时间
d, _ := c.TTL(ctx, "k").Result()        // -2=不存在 -1=无过期
c.PTTL(ctx, "k")                        // 毫秒精度
c.Type(ctx, "k")
c.Rename(ctx, "old", "new")             // 两个 key 都拼前缀
```

### Hash

```go
c.HSet(ctx, "user:1", "name", "alice", "age", 18) // field-value 对
v, _ := c.HGet(ctx, "user:1", "name").Result()
all, _ := c.HGetAll(ctx, "user:1").Result()        // map[string]string
vals, _ := c.HMGet(ctx, "user:1", "name", "age").Result()
c.HDel(ctx, "user:1", "age")
ok, _ := c.HExists(ctx, "user:1", "name").Result()
n, _ := c.HLen(ctx, "user:1").Result()
c.HIncrBy(ctx, "user:1", "score", 10)
c.HIncrByFloat(ctx, "user:1", "balance", 1.5)
```

### List

```go
c.LPush(ctx, "list", "a", "b")
c.RPush(ctx, "list", "c")
v, _ := c.LPop(ctx, "list").Result()
v, _ := c.RPop(ctx, "list").Result()
items, _ := c.LRange(ctx, "list", 0, -1).Result() // -1 表示最后一个
n, _ := c.LLen(ctx, "list").Result()
c.LRem(ctx, "list", 1, "a")            // 移除 1 个等于 "a" 的元素
v, _ := c.LIndex(ctx, "list", 0).Result()
c.LTrim(ctx, "list", 0, 99)            // 只保留前 100 个
```

### Set

```go
c.SAdd(ctx, "tags", "go", "redis")
members, _ := c.SMembers(ctx, "tags").Result()
ok, _ := c.SIsMember(ctx, "tags", "go").Result()
c.SRem(ctx, "tags", "redis")
n, _ := c.SCard(ctx, "tags").Result()
v, _ := c.SRandMember(ctx, "tags").Result() // 随机取（不删）
v, _ := c.SPop(ctx, "tags").Result()        // 随机弹出（删）
```

### Sorted Set

```go
c.ZAdd(ctx, "rank", redis.Z{Score: 100, Member: "alice"})
score, _ := c.ZScore(ctx, "rank", "alice").Result()
i, _ := c.ZRank(ctx, "rank", "alice").Result()       // 升序排名，从 0 开始
top, _ := c.ZRange(ctx, "rank", 0, 9).Result()       // 按排名升序
top, _ = c.ZRevRange(ctx, "rank", 0, 9).Result()     // 逆序（ZRANGE REV，Redis>=6.2）
hits, _ := c.ZRangeByScore(ctx, "rank", &redis.ZRangeBy{
    Min: "60", Max: "100", Offset: 0, Count: 10,     // 按分值范围（Redis>=6.2）
}).Result()
c.ZRem(ctx, "rank", "alice")
c.ZRemRangeByScore(ctx, "rank", "0", "59")
n, _ := c.ZCard(ctx, "rank").Result()
n, _ = c.ZCount(ctx, "rank", "60", "100").Result()
c.ZIncrBy(ctx, "rank", 5, "alice")
```

### 批量删除与遍历

```go
// SCAN + UNLINK 批量删除：UNLINK 后台异步释放内存，大 value 不阻塞主线程。
// pattern 自动拼前缀（"user:*" 实际匹配 "myapp:user:*"），返回删除总数。
// 建议传带超时的 ctx 控制执行时间。
deleted, err := c.DelByPattern(ctx, "user:*")

// 裸 SCAN：match 拼前缀；注意返回的 key 已含前缀（见前缀机制一节）
keys, cursor, err := c.Scan(ctx, 0, "user:*", 100).Result()
```

---

## Pipeline 与事务

`Pipeline()`（批量打包）与 `TxPipeline()`（MULTI/EXEC 事务）返回 go-redis 原生 `redis.Pipeliner`。**Pipeline 内的命令不会自动拼前缀**，用 `Key` / `Keys` 手动拼：

```go
p := c.MustSelectDB(0)
pipe := p.Pipeline()
get := pipe.Get(ctx, p.Key("user:1"))
pipe.Del(ctx, p.Keys("k1", "k2")...)
if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
    return err
}
val, _ := get.Result()
```

---

## Lua 脚本

keys 参数列表中的每个 key 自动拼前缀；脚本体内自行构造的 key 不受管：

```go
// 直接执行
n, err := c.Eval(ctx, `return redis.call("incrby", KEYS[1], ARGV[1])`,
    []string{"counter"}, 10).Int64()

// 按 SHA 执行已缓存脚本
n, err = c.EvalSha(ctx, sha1, []string{"counter"}, 10).Int64()

// 推荐：*redis.Script 自动处理 NOSCRIPT 回退
var script = redis.NewScript(`return redis.call("get", KEYS[1])`)
v, err := c.EvalScript(ctx, script, []string{"k"}).Result()
```

---

## Pub/Sub

Redis Pub/Sub 为 **at-most-once**：不落盘、不重投，订阅者断线期间的消息永久丢失。适合在线通知、缓存失效广播等"丢了无所谓"的场景；需要可靠投递请用 [Stream](#stream-消费组可靠消息)。

Pub/Sub channel 默认不使用 key 前缀。需要 topic 命名空间隔离时，在初始化时显式设置 `WithChannelPrefix("myapp")`，或配合 `WithChannelPrefixSeparator(".")` 自定义拼接格式。

```go
// 发布（默认使用原始 channel；不会套用 WithKeyPrefix）
c.Publish(ctx, "events:user", "user_created")

// 受管消费（推荐）：订阅确认、连接关闭、ctx 退出、panic 恢复均由库内管理
err := c.Consume(ctx, func(m *redis.Message) {
    fmt.Println(m.Channel, m.Payload)
}, "events:user")
// ctx 取消时返回 context.Canceled，可据此判断优雅退出

// 模式订阅的受管消费，生命周期语义与 Consume 一致
err = c.ConsumePattern(ctx, func(m *redis.Message) {
    fmt.Println(m.Pattern, m.Channel, m.Payload)
}, "events:*")

// 裸订阅 / 模式订阅：返回 *redis.PubSub，调用方负责 Close
sub := c.Subscribe(ctx, "events:user")
defer sub.Close()
sub = c.PSubscribe(ctx, "events:*")
defer sub.Close()
```

`Consume` / `ConsumePattern` 的 handler **串行执行**以保证单频道顺序，耗时处理请投递到业务自有 worker；handler panic 会被恢复，消费终止并以 error 返回。

如果配置了 `WithChannelPrefix("myapp")`，上面的 `"events:user"` 实际发布 / 订阅到 `"myapp:events:user"`；模式 `"events:*"` 实际订阅到 `"myapp:events:*"`。

---

## 分布式锁

单 Redis 实例锁：SET NX + 随机 token，Lua 校验 token 后原子释放/续期，杜绝误删他人锁。

```go
lock, err := c.TryLock(ctx, "job:daily-report", 30*time.Second)
if errors.Is(err, redisx.ErrLockNotObtained) {
    return nil // 他人持有，按业务节奏稍后重试
}
if err != nil {
    return err
}
defer lock.Release(ctx)

// 长任务期间显式续期；锁已失去返回 ErrLockLost
if err := lock.Refresh(ctx, 30*time.Second); errors.Is(err, redisx.ErrLockLost) {
    return nil // 锁已过期被他人接管，停止当前工作
}
```

更推荐的闭包模式——`WithLock` 封装"拿锁—执行—必释放"（含 panic 路径），消除漏 `Release` 风险：

```go
err := c.WithLock(ctx, "job:daily-report", 30*time.Second, func(ctx context.Context) error {
    return doReport(ctx) // 预计耗时须显著小于 ttl
})
switch {
case errors.Is(err, redisx.ErrLockNotObtained):
    return nil // 他人持有，跳过本轮
case errors.Is(err, redisx.ErrLockLost):
    // fn 执行超过 ttl，互斥可能已被破坏——触发业务侧补偿/告警
}
```

要点与边界：

- `TryLock` 非阻塞；需要阻塞等待时由调用方按业务节奏循环重试（库不内置轮询策略）。冲突错误用 `errors.Is(err, ErrLockNotObtained)` 判断，错误消息携带完整 key 便于排障。
- `ttl` 必须至少为 1ms——Redis TTL 精度为毫秒，无 TTL 的锁等于死锁隐患。
- `Release` / `Refresh` 在锁已过期或被他人重新获取时返回 `ErrLockLost`，且**不会影响他人的锁**。
- 观测自检：`lock.Key()` 返回完整锁 key（供日志/打点）；`lock.TTL(ctx)` 校验 token 后原子返回剩余时长，锁已失去返回 `ErrLockLost`。注意 TTL 仅供观测——查询与后续操作之间锁仍可能过期，互斥正确性依赖 `Release`/`Refresh` 自身的 token 校验。
- 非 RedLock：主从异步复制下故障切换瞬间存在双持有的理论窗口，关键互斥请在业务层做幂等兜底。
- 不提供 watchdog 自动续期，生命周期由业务显式管理。

---

## Stream 消费组（可靠消息）

Stream 是 Redis 内置的可靠消息队列（**at-least-once**）：消息持久化、消费组分摊、ACK 确认、pending 重投、死消费者接管。

### 生产

```go
id, err := c.XAdd(ctx, &redis.XAddArgs{
    Stream: "orders",                          // 自动拼前缀（不修改传入的 args）
    MaxLen: 100000, Approx: true,              // 建议设置，控制流长度
    Values: map[string]any{"order_id": 1001},
}).Result()
```

### 受管消费

```go
err := c.ConsumeStream(ctx, redisx.StreamConfig{
    Stream:   "orders",
    Group:    "billing",
    Consumer: "worker-1",                  // 组内唯一（如实例 ID）
    BatchSize: 16,                         // 单次最多读取数，默认 16
    Block:    5 * time.Second,             // 无消息时阻塞等待，默认 5s
    AutoClaimMinIdle: 30 * time.Second,    // 可选：接管死消费者闲置超时的消息
    OnError: func(m redis.XMessage, err error) {
        log.Printf("msg %s failed: %v", m.ID, err) // 库内无日志，失败感知交给业务
    },
}, func(m redis.XMessage) error {
    return process(m.Values) // 返回 nil 即 XACK；返回 error 则留在 pending 等待重投
})
```

`StreamConfig` 字段：

| 字段               | 说明                                                                 | 默认    |
| ------------------ | -------------------------------------------------------------------- | ------- |
| `Stream`           | 流名，自动拼前缀（必填）                                             | —      |
| `Group`            | 消费组名，不存在时自动创建（MKSTREAM）（必填）                       | —      |
| `Consumer`         | 消费者名，组内应唯一（必填）                                         | —      |
| `BatchSize`        | 单次 XREADGROUP 最多读取数                                           | 16      |
| `Block`            | 无消息时阻塞等待时长（0 或负值取默认，无法表达无限阻塞）             | 5s      |
| `AutoClaimMinIdle` | >0 时周期接管闲置超过该时长的他人 pending 消息（Redis>=6.2）；实际接管周期下限受 Block 钳制 | 0（关） |
| `MaxDeliver`       | >0 时启用死信策略：投递次数超过该值的消息转入死信流（即 handler 最多尝试 MaxDeliver 次） | 0（关） |
| `DeadLetterStream` | 死信流名，自动拼前缀；`MaxDeliver > 0` 时必填且不得与 Stream 同名     | —      |
| `OnError`          | handler 业务失败时的同步回调，应保持轻量；回调 panic 终止消费        | nil     |

生命周期语义：

- 消费组不存在时自动创建；启动时先续传本消费者的 pending（崩溃重启不丢已读未确认的消息），再消费新消息。
- handler **串行执行**保证顺序；panic 被恢复，消费终止并以 error 返回。
- ctx 取消时返回 ctx 的错误（`errors.Is(err, context.Canceled)` 判断优雅退出）。

### 死信队列（毒消息隔离）

默认失败消息无限重投。配置 `MaxDeliver` + `DeadLetterStream` 后，投递次数超限的毒消息被**原子**（Lua 内 XADD + XACK，无丢失/重复窗口）转入死信流，不再阻塞消费：

```go
err := c.ConsumeStream(ctx, redisx.StreamConfig{
    Stream: "orders", Group: "billing", Consumer: "worker-1",
    MaxDeliver:       5,            // handler 最多尝试 5 次
    DeadLetterStream: "orders:dlq", // 第 6 次投递前转入死信流
    OnError: func(m redis.XMessage, err error) {
        if errors.Is(err, redisx.ErrMessageDeadLettered) {
            alert("毒消息已隔离", m.ID) // 死信化事件经 OnError 通知
        }
    },
}, handler)
```

死信消息保留原字段，并附加元数据：`_redisx_origin_stream`（原流）、`_redisx_origin_id`（原消息 ID）、`_redisx_deliveries`（投递次数）、`_redisx_dead_at`（死信时间，RFC3339）。

边界：

- 投递次数来自 Redis pending entry 的 delivery counter（重投/接管自增）；仅 pending 续传与 AutoClaim 路径检查，新消息热路径零额外开销。
- 死信流的裁剪、监控、重放由业务负责，库内不做 MAXLEN 限制；重放时可按 `_redisx_origin_id` 幂等。
- 业务侧手工 `XCLAIM ... JUSTID` 不自增计数，会让该次接管不计入 MaxDeliver。

### 失败处理与监控

handler 返回 error 的消息**不会 ACK**，留在 pending：重启续传或被 AutoClaim 接管时重投（启用死信策略时超限即隔离）。持续失败的消息会堆积，务必监控：

```go
// pending 摘要：总数、最小/最大 ID、各消费者的数量
summary, err := c.XPending(ctx, "orders", "billing").Result()

// pending 明细：按条件过滤（不修改传入的 args）
details, err := c.XPendingExt(ctx, &redis.XPendingExtArgs{
    Stream: "orders", Group: "billing",
    Start: "-", End: "+", Count: 10,
    Idle: time.Minute, // 只看闲置超过 1 分钟的
}).Result()
```

### 管理命令

```go
n, _ := c.XLen(ctx, "orders").Result()                  // 流长度
msgs, _ := c.XRange(ctx, "orders", "-", "+").Result()   // 按 ID 区间读取
c.XAck(ctx, "orders", "billing", "1-0")                 // 手动确认
c.XDel(ctx, "orders", "1-0")                            // 删除消息
c.XTrimMaxLen(ctx, "orders", 100000)                    // 裁剪流长度

// 组管理
c.XGroupCreateMkStream(ctx, "orders", "billing", "$")   // 提前建组（只消费新消息）
groups, _ := c.XInfoGroups(ctx, "orders").Result()      // 组状态
cs, _ := c.XInfoConsumers(ctx, "orders", "billing").Result() // 消费者状态（发现死条目）
c.XGroupDelConsumer(ctx, "orders", "billing", "pod-old") // 清理死亡消费者
c.XGroupDestroy(ctx, "orders", "billing")               // 销毁组
```

**消费者条目运维**：以实例 ID 做 Consumer 名时，滚动发布会在组内累积死亡消费者条目（AutoClaim 只接管消息、不删条目）。请在实例下线钩子或定期任务中用 `XInfoConsumers` 找出长期闲置者、`XGroupDelConsumer` 清理；其未确认消息会被丢弃，清理前确认 pending 已被接管。

---

## 监控与运维

```go
// 健康检查：PING 所有已初始化 DB，不短路，聚合返回全部失败；nil 表示全部健康
if err := c.HealthCheck(ctx); err != nil {
    log.Printf("redis unhealthy: %v", err)
}

// 连接池统计：按 DB 编号透传 go-redis PoolStats（命中/未命中/超时/连接数）
for db, s := range c.PoolStats() {
    metrics.Report(db, s.Hits, s.Misses, s.Timeouts, s.TotalConns, s.IdleConns)
}

// 优雅关闭：关闭所有 DB 连接池，多 DB 失败用 errors.Join 合并返回
defer c.Close()
```

当前库版本经 `redisx.Version` 常量获取。

---

## Hook 扩展点（外接熔断 / 限流 / 观测）

库本身不内置熔断 / 降级策略——这类决策属于业务层（回源数据库、返回兜底值还是直接报错，只有调用方知道）。库提供的是扩展点：`AddHook` 把任意 go-redis `redis.Hook` 一次性安装到**所有已初始化 DB** 的底层客户端上，可用于接入 gobreaker、sentinel-golang 等熔断库，或挂接限流、metrics、tracing 中间件。

```go
// breakerHook 用任意熔断器包装 Redis 命令执行链（以 gobreaker 风格为例）
type breakerHook struct {
    cb *gobreaker.CircuitBreaker // 业务自选的熔断器实现
}

func (h *breakerHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *breakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
    return func(ctx context.Context, cmd redis.Cmder) error {
        _, err := h.cb.Execute(func() (any, error) {
            return nil, next(ctx, cmd) // 熔断打开时直接快速失败，不再打到 Redis
        })
        return err
    }
}

func (h *breakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
    return func(ctx context.Context, cmds []redis.Cmder) error {
        _, err := h.cb.Execute(func() (any, error) {
            return nil, next(ctx, cmds)
        })
        return err
    }
}

// 初始化阶段安装，对所有 DB 生效
c, err := redisx.NewClient(redisx.WithAddr("127.0.0.1:6379"), redisx.WithInitDBs(0, 1))
if err != nil {
    log.Fatal(err)
}
c.AddHook(&breakerHook{cb: newBreaker()})
```

注意事项：

- **调用时机**：与 go-redis 原生 `AddHook` 约束一致，必须在 `NewClient` 之后、开始并发执行命令之前安装完成，运行中途添加不保证并发安全。
- 只想对个别 DB 安装时，改用 `GetClient(db)` 拿到原生客户端后自行 `AddHook`。
- 熔断快速失败时业务拿到的是熔断器返回的错误（如 gobreaker 的 `ErrOpenState`），可据此走降级分支（回源、兜底值等）。

---

## 并发安全

`Client` 与 `Proxy` 在 `NewClient` 返回后内部状态只读，可在任意 goroutine 并发使用；底层 `*redis.Client` 由 go-redis 连接池保证并发安全。

## 已知边界

- 仅支持单实例 Redis，不支持 Cluster / Sentinel 拓扑（Cluster 协议无多 DB 概念，如有需求属独立特性）。
- Pub/Sub 为 at-most-once；可靠投递请用 Stream。
- 分布式锁为单实例语义（非 RedLock）。

## License

与 gtkit 其他包保持一致。
