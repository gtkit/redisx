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

// ErrMessageDeadLettered 表示消息投递次数超过 MaxDeliver，已被转入死信流。
// 经 [StreamConfig.OnError] 通知业务时可 errors.Is 判别。
var ErrMessageDeadLettered = errors.New("redisx: message dead-lettered")

// 死信消息附加的元数据字段名（下划线前缀避让业务字段）。
const (
	deadLetterFieldOriginStream = "_redisx_origin_stream"
	deadLetterFieldOriginID     = "_redisx_origin_id"
	deadLetterFieldDeliveries   = "_redisx_deliveries"
	deadLetterFieldDeadAt       = "_redisx_dead_at"
)

// deadLetterScript 原子完成"写死信流 + ACK 原消息"，消除两步间崩溃导致的
// 死信丢失或重复窗口。KEYS[1]=源流，KEYS[2]=死信流；ARGV[1]=group，
// ARGV[2]=原消息 ID，ARGV[3..]=死信消息的 field-value 序列。
var deadLetterScript = redis.NewScript(`
local fields = {}
for i = 3, #ARGV do
	fields[#fields+1] = ARGV[i]
end
redis.call("xadd", KEYS[2], "*", unpack(fields))
return redis.call("xack", KEYS[1], ARGV[1], ARGV[2])`)

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

	// MaxDeliver 大于 0 时启用死信策略：投递次数（Redis pending entry 的
	// delivery counter，每次投递/接管自增）超过该值的消息不再交给 handler，
	// 原子转入 DeadLetterStream 并 ACK。0 表示关闭（失败消息无限重投）。
	//
	// 语义即"handler 最多尝试 MaxDeliver 次"。仅 pending 续传与 AutoClaim
	// 路径检查（新消息首次投递必然未超限），新消息热路径零额外开销。
	MaxDeliver int64

	// DeadLetterStream 是死信流名，自动拼接前缀。MaxDeliver > 0 时必填，
	// 且不得与 Stream 同名。死信消息保留原消息全部字段，并附加
	// _redisx_origin_stream / _redisx_origin_id / _redisx_deliveries /
	// _redisx_dead_at 元数据。死信流的裁剪、监控与重放由业务负责，
	// 库内不做 MAXLEN 限制——无人消费时请业务侧自行 XTrimMaxLen。
	DeadLetterStream string

	// OnError 可选：handler 返回业务 error 时被同步调用（此时消息不 ACK，
	// 留在 pending 等待重投）。库内不产生日志，这是业务感知"哪条消息为何
	// 失败"的口子；nil 表示静默（仅靠 XPENDING 监控）。
	//
	// 消息被死信化时同样经本回调通知，err 满足
	// errors.Is(err, ErrMessageDeadLettered)。
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
	if cfg.MaxDeliver < 0 {
		return fmt.Errorf("redisx: stream config: MaxDeliver must be >= 0, got %d", cfg.MaxDeliver)
	}
	if cfg.MaxDeliver > 0 {
		if cfg.DeadLetterStream == "" {
			return errors.New("redisx: stream config: DeadLetterStream is required when MaxDeliver > 0")
		}
		if cfg.DeadLetterStream == cfg.Stream {
			return fmt.Errorf("redisx: stream config: DeadLetterStream must differ from Stream %q", cfg.Stream)
		}
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

// XGroupCreateMkStream 创建消费组，流不存在时一并创建（MKSTREAM）。
// stream 自动拼接前缀。start 为起始 ID（"0" 从头，"$" 只消费新消息）。
//
// [Proxy.ConsumeStream] 会自动建组，本方法用于需要提前建组或自定义
// 起始位置的场景。组已存在时 Redis 返回 BUSYGROUP 错误。
func (p *Proxy) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	return p.rdb.XGroupCreateMkStream(ctx, p.key(stream), group, start)
}

// XGroupDestroy 销毁消费组（含全部 pending 状态）。stream 自动拼接前缀。
func (p *Proxy) XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd {
	return p.rdb.XGroupDestroy(ctx, p.key(stream), group)
}

// XGroupDelConsumer 从消费组中删除指定消费者，返回其被丢弃的 pending 数。
// stream 自动拼接前缀。
//
// 以实例 ID 做 Consumer 名时，滚动发布会在组内累积死亡消费者条目，
// 请在实例下线或定期任务中调用本方法清理；其未确认的消息会被丢弃，
// 清理前请确认 pending 已被 AutoClaim 接管或确认完毕（见 [Proxy.XPendingExt]）。
func (p *Proxy) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	return p.rdb.XGroupDelConsumer(ctx, p.key(stream), group, consumer)
}

// XInfoGroups 返回流上全部消费组的状态（consumer 数、pending 数、last-delivered-id 等）。
// stream 自动拼接前缀。
func (p *Proxy) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	return p.rdb.XInfoGroups(ctx, p.key(stream))
}

// XInfoConsumers 返回消费组内全部消费者的状态（pending 数、闲置时长等），
// 用于发现待清理的死亡消费者。stream 自动拼接前缀。
func (p *Proxy) XInfoConsumers(ctx context.Context, stream, group string) *redis.XInfoConsumersCmd {
	return p.rdb.XInfoConsumers(ctx, p.key(stream), group)
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
// 持续失败的消息默认堆积在 pending（请业务侧用 [Proxy.XPending] 监控）；
// 配置 [StreamConfig.MaxDeliver] + [StreamConfig.DeadLetterStream] 可在
// 投递超限后将毒消息原子隔离到死信流，不再阻塞重投。
//
// 库内不提供 handler 超时控制（Go 无法安全中断不配合的函数），长耗时处理
// 请在 handler 内自行用 context 控制；[StreamConfig.OnError] 回调中如需
// stream/group/consumer 上下文，闭包捕获自己构造的 StreamConfig 即可，
// 消息 ID 在回调参数 msg.ID 中。
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
	if cfg.MaxDeliver > 0 {
		sc.dlStream = p.key(cfg.DeadLetterStream)
	}
	return sc.run(ctx)
}

// streamConsumer 承载一次 ConsumeStream 的消费循环状态。
type streamConsumer struct {
	rdb         *redis.Client
	cfg         StreamConfig
	stream      string
	handler     func(redis.XMessage) error
	lastClaim   time.Time
	claimCursor string // XAUTOCLAIM 游标，周期间续扫；空值等价 "0"（从头）
	dlStream    string // 已拼前缀的死信流名；空表示死信策略关闭
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
			// 续传的是重投消息，检查投递次数并隔离超限者
			if msgs, err = s.filterDeadLetters(ctx, msgs); err != nil {
				return err
			}
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
//
// 周期间使用 XAUTOCLAIM 返回的游标续扫，避免大量 pending 时每次全量重扫；
// 扫完一轮 Redis 返回 "0-0"，作为下次 Start 等价于从头开始新一轮。
func (s *streamConsumer) maybeAutoClaim(ctx context.Context) error {
	if s.cfg.AutoClaimMinIdle <= 0 || time.Since(s.lastClaim) < s.cfg.AutoClaimMinIdle {
		return nil
	}
	s.lastClaim = time.Now()

	start := s.claimCursor
	if start == "" {
		start = "0"
	}
	msgs, next, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    s.cfg.Group,
		Consumer: s.cfg.Consumer,
		MinIdle:  s.cfg.AutoClaimMinIdle,
		Start:    start,
		Count:    s.cfg.BatchSize,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redisx: xautoclaim stream=%q: %w", s.stream, err)
	}
	s.claimCursor = next
	// 接管的是重投消息，检查投递次数并隔离超限者
	if msgs, err = s.filterDeadLetters(ctx, msgs); err != nil {
		return err
	}
	for _, m := range msgs {
		if err := s.handle(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// filterDeadLetters 按 MaxDeliver 过滤一批重投消息：投递次数超限者原子转入
// 死信流并 ACK（不再交给 handler），其余原样返回。策略关闭或空批次时为 no-op。
//
// 投递次数来自 pending entry 的 delivery counter（XPENDING 明细），按本批
// ID 区间一次查询摊薄成本；新消息（">"）路径不调用本方法，热路径零开销。
func (s *streamConsumer) filterDeadLetters(ctx context.Context, msgs []redis.XMessage) ([]redis.XMessage, error) {
	if s.dlStream == "" || len(msgs) == 0 {
		return msgs, nil
	}

	pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   s.stream,
		Group:    s.cfg.Group,
		Start:    msgs[0].ID,
		End:      msgs[len(msgs)-1].ID,
		Count:    int64(len(msgs)),
		Consumer: s.cfg.Consumer,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redisx: xpending stream=%q: %w", s.stream, err)
	}
	deliveries := make(map[string]int64, len(pending))
	for _, pe := range pending {
		deliveries[pe.ID] = pe.RetryCount
	}

	kept := msgs[:0]
	for _, m := range msgs {
		n := deliveries[m.ID]
		if n <= s.cfg.MaxDeliver {
			kept = append(kept, m)
			continue
		}
		if err := s.deadLetter(ctx, m, n); err != nil {
			return nil, err
		}
	}
	return kept, nil
}

// deadLetter 将消息原子转入死信流并 ACK 原消息，随后经 OnError 通知业务。
func (s *streamConsumer) deadLetter(ctx context.Context, m redis.XMessage, deliveries int64) error {
	args := make([]any, 0, 2+2*4+2*len(m.Values))
	args = append(args,
		s.cfg.Group, m.ID,
		deadLetterFieldOriginStream, s.stream,
		deadLetterFieldOriginID, m.ID,
		deadLetterFieldDeliveries, deliveries,
		deadLetterFieldDeadAt, time.Now().UTC().Format(time.RFC3339),
	)
	for k, v := range m.Values {
		args = append(args, k, v)
	}

	if err := deadLetterScript.Run(ctx, s.rdb, []string{s.stream, s.dlStream}, args...).Err(); err != nil {
		return fmt.Errorf("redisx: dead-letter stream=%q id=%s: %w", s.stream, m.ID, err)
	}
	if s.cfg.OnError != nil {
		notify := fmt.Errorf("redisx: message id=%s dead-lettered to %q after %d deliveries: %w",
			m.ID, s.dlStream, deliveries, ErrMessageDeadLettered)
		if pe := safeOnError(s.cfg.OnError, m, notify); pe != nil {
			return pe
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
