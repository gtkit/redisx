package redisx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultStreamBatch = int64(16)
	defaultStreamBlock = 5 * time.Second
)

// StreamConfig 定义 [Proxy.ConsumeStream] 受管消费组的配置。
type StreamConfig struct {
	// Stream 是流名，自动拼接前缀。必填。
	Stream string

	// Group 是消费组名。必填。不存在时自动创建（MKSTREAM）。
	Group string

	// Consumer 是消费者名，组内应唯一（如实例 ID）。必填。
	Consumer string

	// BatchSize 是单次 XREADGROUP 最多读取的消息数。默认 16。
	BatchSize int64

	// Block 是无新消息时 XREADGROUP 的阻塞等待时长（占用一个连接）。
	// 0 或负值取默认 5s，无法表达 Redis 的"无限阻塞"语义。
	Block time.Duration

	// AutoClaimMinIdle 大于 0 时，周期性以 XAUTOCLAIM 接管组内其他消费者
	// 闲置超过该时长的 pending 消息（需要 Redis >= 6.2）。0 表示关闭。
	//
	// 注意：无新消息时消费循环先阻塞 Block 时长才检查接管，
	// 实际接管周期下限受 Block 钳制，设置小于 Block 的值没有意义。
	AutoClaimMinIdle time.Duration

	// OnError 可选：handler 返回业务 error 时被同步调用（此时消息不 ACK，
	// 留在 pending 等待重投）。库内不产生日志，这是业务感知"哪条消息为何
	// 失败"的口子；nil 表示静默（仅靠 XPENDING 监控）。
	//
	// 回调在消费 goroutine 内执行，应保持轻量（记日志/打点）；
	// 回调内 panic 会终止消费并以 error 返回。
	OnError func(msg redis.XMessage, err error)
}

func (cfg *StreamConfig) validate() error {
	if cfg.Stream == "" {
		return errors.New("redisx: stream config: Stream is required")
	}
	if cfg.Group == "" {
		return errors.New("redisx: stream config: Group is required")
	}
	if cfg.Consumer == "" {
		return errors.New("redisx: stream config: Consumer is required")
	}
	return nil
}

// XAdd 追加消息到流。args.Stream 自动拼接前缀（不修改传入的 args）。
func (p *Proxy) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	if args == nil {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(errors.New("redisx: XAdd args is nil"))
		return cmd
	}
	prefixed := *args
	prefixed.Stream = p.key(args.Stream)
	return p.rdb.XAdd(ctx, &prefixed)
}

// XAck 确认消费组内一条或多条消息。stream 自动拼接前缀。
func (p *Proxy) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	return p.rdb.XAck(ctx, p.key(stream), group, ids...)
}

// XLen 返回流的消息数量。stream 自动拼接前缀。
func (p *Proxy) XLen(ctx context.Context, stream string) *redis.IntCmd {
	return p.rdb.XLen(ctx, p.key(stream))
}

// XRange 返回流中 ID 区间内的消息（"-" / "+" 表示最小 / 最大 ID）。
// stream 自动拼接前缀。
func (p *Proxy) XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	return p.rdb.XRange(ctx, p.key(stream), start, stop)
}

// XDel 从流中删除一条或多条消息。stream 自动拼接前缀。
func (p *Proxy) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	return p.rdb.XDel(ctx, p.key(stream), ids...)
}

// XTrimMaxLen 将流裁剪到最多保留 maxLen 条消息。stream 自动拼接前缀。
func (p *Proxy) XTrimMaxLen(ctx context.Context, stream string, maxLen int64) *redis.IntCmd {
	return p.rdb.XTrimMaxLen(ctx, p.key(stream), maxLen)
}

// XPending 返回消费组的 pending 摘要（总数、最小/最大 ID、各消费者数量）。
// stream 自动拼接前缀。用于监控 handler 持续失败导致的消息堆积。
func (p *Proxy) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	return p.rdb.XPending(ctx, p.key(stream), group)
}

// XPendingExt 按条件返回消费组 pending 消息明细。
// args.Stream 自动拼接前缀（不修改传入的 args）。
func (p *Proxy) XPendingExt(ctx context.Context, args *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	if args == nil {
		cmd := redis.NewXPendingExtCmd(ctx)
		cmd.SetErr(errors.New("redisx: XPendingExt args is nil"))
		return cmd
	}
	prefixed := *args
	prefixed.Stream = p.key(args.Stream)
	return p.rdb.XPendingExt(ctx, &prefixed)
}

// ConsumeStream 以消费组方式受管消费流消息（at-least-once），
// 阻塞直到 ctx 取消或发生不可恢复错误。
//
// 生命周期由库内管理：消费组不存在时自动创建（MKSTREAM）；启动时先续传
// 本消费者的 pending 消息（崩溃重启不丢已读未确认的消息），再消费新消息。
//
// handler 返回 nil 即 XACK；返回 error 时消息留在 pending（重启续传或被
// AutoClaim 接管时重投），消费继续，可经 [StreamConfig.OnError] 感知失败明细；
// handler panic 会被恢复，消费终止并以 error 返回。handler 串行执行以保证消息顺序。
// ctx 取消时返回 ctx 的错误（errors.Is(err, context.Canceled) 判断优雅退出）。
//
// 持续失败的消息会堆积在 pending，请业务侧用 [Proxy.XPending] 监控。
func (p *Proxy) ConsumeStream(ctx context.Context, cfg StreamConfig, handler func(redis.XMessage) error) error {
	if handler == nil {
		return errors.New("redisx: stream handler is nil")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultStreamBatch
	}
	if cfg.Block <= 0 {
		cfg.Block = defaultStreamBlock
	}

	stream := p.key(cfg.Stream)
	if err := p.rdb.XGroupCreateMkStream(ctx, stream, cfg.Group, "0").Err(); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("redisx: create group %q on stream %q: %w", cfg.Group, stream, err)
	}

	sc := &streamConsumer{rdb: p.rdb, cfg: cfg, stream: stream, handler: handler}
	return sc.run(ctx)
}

// streamConsumer 承载一次 ConsumeStream 的消费循环状态。
type streamConsumer struct {
	rdb       *redis.Client
	cfg       StreamConfig
	stream    string
	handler   func(redis.XMessage) error
	lastClaim time.Time
}

func (s *streamConsumer) run(ctx context.Context) error {
	readID := "0" // 先续传本消费者的 pending，drained 后切换 ">"
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("redisx: consume stream: %w", ctx.Err())
		}

		msgs, err := s.read(ctx, readID)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("redisx: consume stream: %w", ctx.Err())
			}
			return err
		}

		if readID != ">" {
			if len(msgs) == 0 {
				readID = ">" // pending 已续传完毕
				continue
			}
			// 显式 ID 推进，避免对持续失败的消息无限重读
			readID = msgs[len(msgs)-1].ID
		}

		for _, m := range msgs {
			if err := s.handle(ctx, m); err != nil {
				return err
			}
		}

		if err := s.maybeAutoClaim(ctx); err != nil {
			return err
		}
	}
}

// read 读取一批消息；无新消息（阻塞超时）返回空批次。
func (s *streamConsumer) read(ctx context.Context, readID string) ([]redis.XMessage, error) {
	block := s.cfg.Block
	if readID != ">" {
		block = -1 // 历史 pending 读取不阻塞（go-redis 负值省略 BLOCK）
	}
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.cfg.Group,
		Consumer: s.cfg.Consumer,
		Streams:  []string{s.stream, readID},
		Count:    s.cfg.BatchSize,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("redisx: xreadgroup stream=%q: %w", s.stream, err)
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

// handle 执行 handler：成功即 XACK；业务失败留在 pending 并通知 OnError；
// panic（含 OnError 内的）转为终止错误。
func (s *streamConsumer) handle(ctx context.Context, m redis.XMessage) error {
	bizErr, panicErr := safeStreamHandle(s.handler, m)
	if panicErr != nil {
		return panicErr
	}
	if bizErr != nil {
		// 业务失败：不 ACK，留在 pending 等待重投
		if s.cfg.OnError != nil {
			if pe := safeOnError(s.cfg.OnError, m, bizErr); pe != nil {
				return pe
			}
		}
		return nil
	}
	if err := s.rdb.XAck(ctx, s.stream, s.cfg.Group, m.ID).Err(); err != nil {
		return fmt.Errorf("redisx: xack stream=%q id=%s: %w", s.stream, m.ID, err)
	}
	return nil
}

// maybeAutoClaim 按 AutoClaimMinIdle 周期接管其他消费者的超时 pending 消息。
func (s *streamConsumer) maybeAutoClaim(ctx context.Context) error {
	if s.cfg.AutoClaimMinIdle <= 0 || time.Since(s.lastClaim) < s.cfg.AutoClaimMinIdle {
		return nil
	}
	s.lastClaim = time.Now()

	msgs, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    s.cfg.Group,
		Consumer: s.cfg.Consumer,
		MinIdle:  s.cfg.AutoClaimMinIdle,
		Start:    "0",
		Count:    s.cfg.BatchSize,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redisx: xautoclaim stream=%q: %w", s.stream, err)
	}
	for _, m := range msgs {
		if err := s.handle(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// safeStreamHandle 执行 handler 并返回其业务 error（nil 表示成功，应当 ACK）；
// panic 被恢复并转换为 panicErr。
func safeStreamHandle(handler func(redis.XMessage) error, m redis.XMessage) (bizErr, panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("redisx: stream handler panic: %v", r)
		}
	}()
	return handler(m), nil
}

// safeOnError 执行 OnError 回调，panic 被恢复并转换为终止错误。
func safeOnError(onError func(redis.XMessage, error), m redis.XMessage, bizErr error) (panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			panicErr = fmt.Errorf("redisx: stream OnError panic: %v", r)
		}
	}()
	onError(m, bizErr)
	return nil
}
