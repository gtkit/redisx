# Changelog

本文件遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [SemVer 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

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
