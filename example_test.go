package redisx_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gtkit/redisx"

	goredis "github.com/redis/go-redis/v9"
)

func Example() {
	// ─── 1. 极简用法（仅 Addr 必填）───
	c, err := redisx.NewClient(
		redisx.WithAddr("127.0.0.1:6379"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	_ = ctx

	// ─── 2. 完整配置 ───
	c2, err := redisx.NewClient(
		redisx.WithAddr("127.0.0.1:6379"),
		redisx.WithUsername("default"), // Redis 6+ ACL
		redisx.WithPassword("123456"),
		redisx.WithDB(0),                  // 默认 DB
		redisx.WithKeyPrefix("app:demo"),  // 全局前缀
		redisx.WithInitDBs(0, 1, 2),       // 初始化多个 DB（共享全局前缀）
		redisx.WithDBConfig(3, "session"), // DB3 使用独立前缀 "session"
		redisx.WithPoolSize(20),
		redisx.WithMinIdleConns(5),
		redisx.WithMaxRetries(3),
		redisx.WithDialTimeout(5*time.Second),
		redisx.WithReadTimeout(3*time.Second),
		redisx.WithWriteTimeout(3*time.Second),
		redisx.WithIdleTimeout(5*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c2.Close()

	// ─── 3. 默认 DB 操作（key 自动加前缀 "app:demo:user:1"）───
	c2.Set(ctx, "user:1", "hello", time.Hour)
	val, _ := c2.Get(ctx, "user:1").Result()
	fmt.Println(val) // "hello"

	// ─── 4. 多 DB 切换（链式调用）───
	db1 := c2.MustSelectDB(1)
	db1.Set(ctx, "config:timeout", "30s", 0)
	cfg, _ := db1.Get(ctx, "config:timeout").Result()
	fmt.Println(cfg) // "30s"

	// DB3 使用独立前缀 "session"：实际 key 为 "session:token:abc"
	db3 := c2.MustSelectDB(3)
	db3.Set(ctx, "token:abc", "user_123", 30*time.Minute)

	// 安全获取（带错误处理）
	db2, err := c2.SelectDB(2)
	if err != nil {
		log.Fatal(err)
	}
	db2.Set(ctx, "cache:hot", "data", 10*time.Minute)

	// GetClient 获取原生 *redis.Client（不带前缀）
	if rdb, ok := c2.GetClient(1); ok {
		rdb.Set(ctx, "raw:key", "value", 0)
	}

	// ─── 5. Hash 操作 ───
	c2.HSet(ctx, "user:100", "name", "alice", "age", 18)
	name, _ := c2.HGet(ctx, "user:100", "name").Result()
	fmt.Println(name) // "alice"

	all, _ := c2.HGetAll(ctx, "user:100").Result()
	fmt.Println(all) // map[name:alice age:18]

	// ─── 6. List 操作 ───
	c2.LPush(ctx, "queue:tasks", "task1", "task2", "task3")
	task, _ := c2.RPop(ctx, "queue:tasks").Result()
	fmt.Println(task) // "task1"

	// ─── 7. Set 操作 ───
	c2.SAdd(ctx, "tags:user:1", "go", "redis", "docker")
	isMember, _ := c2.SIsMember(ctx, "tags:user:1", "go").Result()
	fmt.Println(isMember) // true

	// ─── 8. Sorted Set 操作 ───
	c2.ZAdd(ctx, "leaderboard",
		goredis.Z{Score: 100, Member: "alice"},
		goredis.Z{Score: 200, Member: "bob"},
	)
	score, _ := c2.ZScore(ctx, "leaderboard", "bob").Result()
	fmt.Println(score) // 200

	// ─── 9. SCAN 安全批量删除 ───
	deleted, _ := c2.DelByPattern(ctx, "user:*")
	fmt.Printf("deleted %d keys\n", deleted)

	// ─── 10. 健康检查 ───
	if err := c2.HealthCheck(ctx); err != nil {
		log.Printf("redis unhealthy: %v", err)
	}

	// ─── 11. Lua 脚本（keys 自动加前缀）───
	script := `return redis.call("GET", KEYS[1])`
	result, _ := c2.Eval(ctx, script, []string{"user:1"}).Result()
	fmt.Println(result)

	// ─── 12. Pipeline（需通过 Proxy.Key 手动拼前缀）───
	proxy := c2.MustSelectDB(0)
	pipe := proxy.Pipeline()
	pipe.Set(ctx, proxy.Key("batch:1"), "v1", time.Hour)
	pipe.Set(ctx, proxy.Key("batch:2"), "v2", time.Hour)
	pipe.Get(ctx, proxy.Key("batch:1"))
	pipe.Del(ctx, proxy.Keys("batch:1", "batch:2")...) // 多 key 批量拼前缀
	_, _ = pipe.Exec(ctx)

	// ─── 13. Pub/Sub ───
	pubsub := c2.Subscribe(ctx, "events:user")
	defer pubsub.Close()

	c2.Publish(ctx, "events:user", "user_created")
}

// Example_migration 展示从 github.com/gtkit/redis（v1）迁移的等价用法。
//
// v1 用法:
//
//	conn, err := redis.NewCollection(
//	    redis.WithAddr("127.0.0.1:6379"),
//	    redis.WithDB(0, "test"),
//	    redis.WithDB(1),
//	    redis.WithDB(2, "prefix:test2"),
//	)
//	rdb := redis.Select(2)
//	rdb.Client().Set(ctx, rdb.Prefix()+"key:2", "value:2", 0)  // 手动拼前缀
//
// redisx 等价:.
func Example_migration() {
	c, err := redisx.NewClient(
		redisx.WithAddr("127.0.0.1:6379"),
		redisx.WithDBConfig(0, "test"),
		redisx.WithInitDBs(1),
		redisx.WithDBConfig(2, "prefix:test2"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// redisx: 前缀自动拼接，业务层无感知
	db2 := c.MustSelectDB(2)
	db2.Set(ctx, "key:2", "value:2", 0) // 实际 key: "prefix:test2:key:2"
}

// ExampleGetJSON 演示结构体值的 JSON 读写助手与新增的人体工学 API。
func ExampleGetJSON() {
	c, err := redisx.NewClient(redisx.WithAddr("127.0.0.1:6379"), redisx.WithKeyPrefix("app"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	type User struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	// 结构体读写：Client 内嵌 Proxy，直接传 c.Proxy
	_ = redisx.SetJSON(ctx, c.Proxy, "user:1", User{Name: "alice", Age: 18}, time.Hour)
	u, err := redisx.GetJSON[User](ctx, c.Proxy, "user:1")
	if errors.Is(err, goredis.Nil) {
		fmt.Println("user not found")
	}
	_ = u

	// 闭包持锁执行：拿锁—执行—必释放
	err = c.WithLock(ctx, "job:report", 30*time.Second, func(context.Context) error {
		return nil // 业务逻辑
	})
	if errors.Is(err, redisx.ErrLockNotObtained) {
		fmt.Println("他人持有，跳过本轮")
	}

	// 遍历 key：产出已剥前缀，可直接回传
	for key, err := range c.ScanKeys(ctx, "user:*") {
		if err != nil {
			log.Fatal(err)
		}
		c.Del(ctx, key)
	}
}
