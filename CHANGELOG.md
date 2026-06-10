# Changelog

本文件遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [SemVer 2.0.0](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [1.0.0] - 2026-06-10

redisx 首个正式版本，为 `github.com/gtkit/redis`（v1/v2）的后继包，能力完整覆盖两者；代码迁移自 `github.com/gtkit/redis/v2` 主干并改名为 `redisx`。

### Added

- 多 DB 客户端 `NewClient`，Functional Options 配置，初始化全有或全无并自动回收失败连接
- 全局 key 前缀透明拼接，支持 per-DB 独立前缀（`WithKeyPrefix` / `WithDBConfig`）
- 完整连接池与超时配置（`WithPoolSize`、`WithMinIdleConns`、`WithDialTimeout`、`WithReadTimeout`、`WithWriteTimeout`、`WithIdleTimeout`、`WithMaxRetries`）
- 新增 `WithTLSConfig`，支持 TLS 连接 Redis（gtkit/redis v1/v2 均不支持）
- 新增 `WithAllowPartialInit`，可选降级初始化：失败 DB 缺席集合、错误聚合返回，对齐 gtkit/redis v1 的部分成功语义（DefaultDB 仍须成功）
- 新增 `Proxy.Keys`，Pipeline 场景批量拼接多个 key 的前缀
- 新增 `Consume` 受管消费：订阅确认、连接关闭、ctx 取消退出、handler panic 恢复均由库内管理，封掉裸 PubSub 忘 Close 的连接泄漏点
- 新增 `PSubscribe` 模式订阅，模式自动拼接前缀
- `Proxy` 命令代理：String / Hash / List / Set / ZSet / Pub-Sub / Lua / Pipeline，key 自动加前缀
- `DelByPattern`：SCAN + UNLINK 批量删除，异步释放内存不阻塞 Redis 主线程
- `HealthCheck` 健康检查，聚合返回所有 DB 的失败信息

### Changed

- 包名由 `redis` 改为 `redisx`，与 go-redis 的 `redis` 包不再冲突，业务代码无需 import 别名
- 底层 go-redis 升级至 v9.20.0
- 透传层不再调用 go-redis 已弃用方法：`SetEX`/`GetSet`/`ZRevRange`/`ZRangeByScore` 改用现代等价命令实现（SET 带过期、SET..GET、ZRANGE REV/BYSCORE），导出签名与语义不变；`GetSet`/`ZRevRange`/`ZRangeByScore` 需要 Redis >= 6.2
