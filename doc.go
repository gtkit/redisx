// Package redisx 提供基于 github.com/redis/go-redis/v9 的生产级 Redis 客户端封装。
//
// redisx 是 github.com/gtkit/redis（v1/v2）的后继包，能力完整覆盖两者，
// 新项目请直接使用本包。主要特性:
//   - 无全局变量，支持多实例（多服务器/多配置）共存
//   - 全局 Key 前缀透明封装，业务层无感知
//   - 支持 per-DB 独立前缀
//   - 完整连接池/超时配置（PoolSize、DialTimeout、ReadTimeout 等）
//   - 支持 TLS 连接（WithTLSConfig）
//   - 可选降级初始化（WithAllowPartialInit，失败 DB 缺席集合、错误聚合返回）
//   - SCAN + UNLINK 批量删除，异步释放内存不阻塞 Redis 主线程
//   - 无内部日志：所有失败经返回值传递，由调用方决定日志策略；零日志框架依赖
//   - HealthCheck 健康检查 API
//   - 包名 redisx 与 go-redis 的 redis 包不冲突，业务层无需 import 别名
//
// 快速使用:
//
//	c, err := redisx.NewClient(
//	    redisx.WithAddr("127.0.0.1:6379"),
//	    redisx.WithPassword("123456"),
//	    redisx.WithKeyPrefix("app:demo"),
//	    redisx.WithInitDBs(0, 1, 2),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer c.Close()
//
//	ctx := context.Background()
//
//	// 默认 DB 操作，key 自动添加前缀 "app:demo:user:1"
//	c.Set(ctx, "user:1", "hello", 0)
//
//	// 切换 DB1 链式调用
//	val, _ := c.MustSelectDB(1).Get(ctx, "config").Result()
//
//	// SCAN 安全批量删除
//	deleted, _ := c.DelByPattern(ctx, "user:*")
package redisx
