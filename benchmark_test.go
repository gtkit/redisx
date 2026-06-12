package redisx

// 基准测试。纯函数 benchmark 无外部依赖；命令路径 benchmark 依赖真实 Redis
// （REDISX_TEST_ADDR 覆盖地址，默认 127.0.0.1:6379，不可达时 skip）。

import (
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ──────────────────────────────────────────
//  纯函数：前缀拼接
// ──────────────────────────────────────────

func BenchmarkPrefixKey(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = prefixKey("app:service", defaultKeyPrefixSeparator, "user:12345")
	}
}

func BenchmarkPrefixKeyEmpty(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = prefixKey("", defaultKeyPrefixSeparator, "user:12345")
	}
}

func BenchmarkProxyKeys(b *testing.B) {
	p := &Proxy{prefix: "app:service", prefixSeparator: defaultKeyPrefixSeparator}
	ks := []string{"user:1", "user:2", "user:3", "user:4", "user:5", "user:6", "user:7", "user:8"}

	b.ReportAllocs()
	for b.Loop() {
		_ = p.keys(ks)
	}
}

// ──────────────────────────────────────────
//  命令包装（真实 Redis）
// ──────────────────────────────────────────

// newBenchClient 创建指向真实 Redis 的客户端，注册清理。
func newBenchClient(b *testing.B) *Client {
	b.Helper()
	c := newTestClient(b)
	return c
}

func BenchmarkProxySet(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		if err := c.Set(ctx, "bench:key", "value", time.Minute).Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProxyGet(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()
	if err := c.Set(ctx, "bench:key", "value", time.Minute).Err(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := c.Get(ctx, "bench:key").Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProxyGet_Parallel(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()
	if err := c.Set(ctx, "bench:key", "value", time.Minute).Err(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Get(ctx, "bench:key").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDelByPattern(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()
	const keysPerRound = 50

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		pipe := c.Pipeline()
		for i := range keysPerRound {
			pipe.Set(ctx, c.Key(fmt.Sprintf("bench:del:%d", i)), "v", time.Minute)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		deleted, err := c.DelByPattern(ctx, "bench:del:*")
		if err != nil {
			b.Fatal(err)
		}
		if deleted != keysPerRound {
			b.Fatalf("deleted = %d, want %d", deleted, keysPerRound)
		}
	}
}

// ──────────────────────────────────────────
//  Stream 生产与消费（真实 Redis）
// ──────────────────────────────────────────

func BenchmarkStreamXAdd(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()
	args := &redis.XAddArgs{Stream: "bench:st", MaxLen: 1000, Approx: true, Values: map[string]any{"v": "payload"}}

	b.ReportAllocs()
	for b.Loop() {
		if err := c.XAdd(ctx, args).Err(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamProduceConsume 度量单条消息的完整链路：
// XADD → XREADGROUP → handler → XACK（复用库内消费循环的 read/handle 路径）。
func BenchmarkStreamProduceConsume(b *testing.B) {
	c := newBenchClient(b)
	ctx := b.Context()

	stream := c.Key("bench:st")
	if err := c.DefaultClient().XGroupCreateMkStream(ctx, stream, "g", "0").Err(); err != nil {
		b.Fatal(err)
	}
	sc := &streamConsumer{
		rdb:     c.DefaultClient(),
		cfg:     StreamConfig{Stream: "bench:st", Group: "g", Consumer: "c1", BatchSize: 1, Block: time.Second},
		stream:  stream,
		handler: func(redis.XMessage) error { return nil },
	}
	args := &redis.XAddArgs{Stream: "bench:st", MaxLen: 1000, Approx: true, Values: map[string]any{"v": "payload"}}

	b.ReportAllocs()
	for b.Loop() {
		if err := c.XAdd(ctx, args).Err(); err != nil {
			b.Fatal(err)
		}
		msgs, err := sc.read(ctx, ">")
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := sc.handle(ctx, m); err != nil {
				b.Fatal(err)
			}
		}
	}
}
