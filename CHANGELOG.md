# Changelog

本文件遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [SemVer 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

- 新增 `WithInitDBPrefix(db, prefix)` 作为 per-DB 独立前缀的首选 Option；`WithDBConfig` 保留为兼容别名并标记废弃
- 新增 `WithChannelPrefix(prefix)` / `WithChannelPrefixSeparator(s)`，Pub/Sub channel 前缀与 key 前缀解耦
- 新增 `Proxy.WithPrefix(prefix)`，可在同一底层 Redis client 上派生不同 key 命名空间的 Proxy
- 新增 `WrapClient(rdb, prefix, separator)`，便于将已有 `*redis.Client` 包装为 Proxy 用于测试 / DI

### Changed

- **行为变化（边缘 BREAKING）**：Pub/Sub `Publish` / `Subscribe` / `PSubscribe` / `Consume` / `ConsumePattern` 默认不再复用 key 前缀；需要保持旧 channel 名称隔离的调用方请显式配置 `WithChannelPrefix`

## [1.2.0] - 2026-06-12

### Added

- 新增 JSON 泛型助手 `GetJSON[T]` / `SetJSON`：消除结构体值读写的 Marshal/Unmarshal 样板，key 不存在透传 `redis.Nil`
- 新增 bytes / codec 泛型助手：`SetBytes` / `GetBytes` 显式存取二进制 payload，`Codec` + `SetCodec` / `GetCodec[T]` 支持调用方自带 protobuf/gob/msgpack 等编解码器，不新增这些格式的依赖

- Stream 消费组新增死信策略：`StreamConfig.MaxDeliver` + `DeadLetterStream`，投递超限的毒消息经 Lua 原子（XADD + XACK 无丢失/重复窗口）转入死信流并附 `_redisx_*` 元数据，死信化事件经 `OnError` 以 `ErrMessageDeadLettered` 通知；默认关闭，行为不变

- 新增 `Proxy.WithLock(ctx, key, ttl, fn)`：闭包持锁执行，保证释放（含 panic 与 ctx 取消路径），fn 超 TTL 丢锁时 `ErrLockLost` 并入返回错误可感知
- 新增 `Proxy.ScanKeys(ctx, match)`：自动翻页的 `iter.Seq2[string, error]` 迭代器，产出已剥前缀的 key，结构性消除 `Scan` 的双重拼接陷阱
- 新增消费组管理命令透传（stream 自动拼前缀）：`XGroupCreateMkStream` / `XGroupDestroy` / `XGroupDelConsumer` / `XInfoGroups` / `XInfoConsumers`，README 补充死亡消费者清理运维指引

- 新增 GitHub Actions CI：真实 Redis service container 下跑 `go vet` + `go test -race` + 覆盖率 ≥ 80% 门槛 + benchmark 冒烟 + golangci-lint，质量门自动化执行

- 新增 `Lock.Key()`（返回已拼前缀的完整锁 key，供日志/打点）与 `Lock.TTL(ctx)`（Lua 校验 token 后原子返回剩余 TTL，锁已失去返回 `ErrLockLost`；仅供观测自检，互斥正确性仍依赖 Release/Refresh 的 token 校验）

- `NewClient` 增加配置防御校验（fail-fast）：`PoolSize` 必须 > 0、`MinIdleConns` 必须在 [0, PoolSize] 内、`MaxRetries` 必须 >= 0、四个超时（Dial/Read/Write/Idle）必须 > 0，非法值在拨号前报错并包含字段名与实际值；go-redis 的 0/-1/-2 哨兵语义不再透传
- 新增真实 Redis 集成测试（`REDISX_TEST_ADDR` 可覆盖地址，默认 127.0.0.1:6379，不可达时 skip）：覆盖 Client 生命周期、全部命令代理与前缀拼接、分布式锁互斥 / 过期 / 续期 / token 防误删、Stream 消费组创建 / ACK / pending 续传 / AutoClaim 接管 / ctx 取消，整体覆盖率约 95%
- 新增 `benchmark_test.go`：prefixKey / Keys 纯函数、Set / Get 命令包装、DelByPattern、Stream XAdd 与生产消费链路基准

### Fixed

- 修复 `WithMaxRetries(0)` 与文档承诺不符的问题：此前 0 被 go-redis 解释为"默认 3 次重试"，现库内映射为 -1，0 真正表示关闭自动重试
- 修复 `ConsumeStream` 的 AutoClaim 每个周期都从 "0" 全量重扫 pending 的低效问题：现保存 XAUTOCLAIM 返回的游标周期间续扫，扫完一轮自动回绕

### Changed

- **行为变化（边缘 BREAKING）**：此前传入非法配置（如 `WithPoolSize(-1)`、`WithReadTimeout(0)`）仍能创建客户端，升级后 `NewClient` 返回配置校验错误
- **行为变化（边缘 BREAKING）**：`KeyPrefix` / per-DB 前缀含 glob 元字符（`*?[]\`）时 `NewClient` 报错——前缀会拼入 `DelByPattern`/`Scan` 的 MATCH 模式，含元字符存在批量误删风险，此前是静默隐患
- **行为变化（边缘 BREAKING）**：`MSet` 偶数位（key 位）传非 string 参数现在返回错误——此前静默跳过前缀拼接，key 落入无前缀命名空间且不可发现
- 包内错误消息前缀统一为 `redisx:`（此前 `client.go`/`config.go`/`proxy.go` 用 `redis:`，与 go-redis 自身错误难以区分）；`TryLock` 冲突错误现包装完整 key 上下文——错误链（`errors.Is/As`）均不受影响，仅依赖子串匹配错误文本的调用方需注意
- 文档修正：DB 编号上限说明改为"取决于服务端 databases 配置"（不再写死 0~15）；README 明确 `Proxy` 为常用命令子集、完整命令面走 `RawClient`；`ConsumeStream` 文档补充 handler 超时与 `OnError` 上下文的业务侧指引

## [1.1.1] - 2026-06-11

### Changed

- README 重写为全面使用文档：初始化与配置全表、Key 前缀机制、多 DB、各命令族示例、Pipeline / Lua、Pub/Sub、分布式锁、Stream 消费组、监控运维、Hook 扩展点、已知边界；移除 gtkit/redis 迁移章节（迁移说明保留在 1.0.0 版本记录中）

## [1.1.0] - 2026-06-10

### Added

- 新增 `Client.AddHook`：将 go-redis Hook 一次性安装到所有已初始化 DB，用于外接熔断 / 限流 / metrics / tracing（库内不内置策略）
- 新增 `StreamConfig.OnError` 可选回调：Stream handler 业务失败时携带原始消息与错误同步通知业务侧（库内无日志设计下的失败感知口子）
- 新增 Stream 监控与管理命令透传：`XPending` / `XPendingExt` / `XLen` / `XRange` / `XDel` / `XTrimMaxLen`，stream 自动拼前缀
- 新增 `Client.PoolStats`：按 DB 编号透传 go-redis 连接池统计，供业务接入监控
- 新增 `Proxy.ConsumePattern`：模式订阅（PSUBSCRIBE）的受管消费，生命周期语义与 `Consume` 一致

### Changed

- `Client` 内嵌默认 DB 的 `Proxy`，全部命令方法经提升直接可用：原快捷方法源码完全兼容，并自动补齐此前缺失的命令子集（`PTTL`、`LRem`、`SPop` 等）；删除约 300 行手抄委托样板
- 初始化改为 DefaultDB 优先拨号：DefaultDB 失败立即整体失败且不再拨号其余 DB（fail-fast），其余 DB 按编号升序，错误信息确定有序
- `Proxy.Scan` / `StreamConfig.Block` / `AutoClaimMinIdle` 文档补充使用陷阱说明（Scan 返回 key 已含前缀；Block 0 值取默认；AutoClaim 实际周期下限受 Block 钳制）

### Fixed

- 修复降级初始化（`WithAllowPartialInit`）下 DefaultDB 失败路径将 `*InitError` 摊平、导致 `errors.As` 无法提取的问题（DefaultDB 优先拨号后该路径不复存在）

## [1.0.0] - 2026-06-10

redisx 首个正式版本，为 `github.com/gtkit/redis`（v1/v2）的后继包，能力完整覆盖两者；代码迁移自 `github.com/gtkit/redis/v2` 主干并改名为 `redisx`。

### Added

- 多 DB 客户端 `NewClient`，Functional Options 配置，初始化全有或全无并自动回收失败连接
- 全局 key 前缀透明拼接，支持 per-DB 独立前缀（`WithKeyPrefix` / `WithDBConfig`）
- 完整连接池与超时配置（`WithPoolSize`、`WithMinIdleConns`、`WithDialTimeout`、`WithReadTimeout`、`WithWriteTimeout`、`WithIdleTimeout`、`WithMaxRetries`）
- 新增 `WithTLSConfig`，支持 TLS 连接 Redis（gtkit/redis v1/v2 均不支持）
- 新增 `WithAllowPartialInit`，可选降级初始化：失败 DB 缺席集合、错误聚合返回，对齐 gtkit/redis v1 的部分成功语义（DefaultDB 仍须成功）
- 新增 `InitError` 结构化错误：降级初始化部分失败时经 `errors.As` 提取，按 DB 编号获取失败原因做编程决策
- 新增 `Proxy.Keys`，Pipeline 场景批量拼接多个 key 的前缀
- 新增 `Consume` 受管消费：订阅确认、连接关闭、ctx 取消退出、handler panic 恢复均由库内管理，封掉裸 PubSub 忘 Close 的连接泄漏点
- 新增 `PSubscribe` 模式订阅，模式自动拼接前缀
- 新增分布式锁：`TryLock` 获取（SET NX + 随机 token），`Lock.Release`/`Lock.Refresh` 经 Lua 校验 token 原子执行，杜绝误删他人锁；哨兵错误 `ErrLockNotObtained`/`ErrLockLost` 可 `errors.Is` 判断
- 新增 Stream 消费组受管消费（at-least-once）：`ConsumeStream` 自动建组、崩溃重启续传 pending、handler 成功即 XACK、可选 `AutoClaimMinIdle` 接管死消费者消息；配套 `XAdd`/`XAck` 前缀透明透传
- `Proxy` 命令代理：String / Hash / List / Set / ZSet / Pub-Sub / Lua / Pipeline，key 自动加前缀
- `DelByPattern`：SCAN + UNLINK 批量删除，异步释放内存不阻塞 Redis 主线程
- `HealthCheck` 健康检查，聚合返回所有 DB 的失败信息

### Changed

- 包名由 `redis` 改为 `redisx`，与 go-redis 的 `redis` 包不再冲突，业务代码无需 import 别名
- 底层 go-redis 升级至 v9.20.0
- 透传层不再调用 go-redis 已弃用方法：`SetEX`/`GetSet`/`ZRevRange`/`ZRangeByScore` 改用现代等价命令实现（SET 带过期、SET..GET、ZRANGE REV/BYSCORE），导出签名与语义不变；`GetSet`/`ZRevRange`/`ZRangeByScore` 需要 Redis >= 6.2
