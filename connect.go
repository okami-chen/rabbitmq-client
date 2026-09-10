package rabbitmq_client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/util/grand"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	reconnectMinInterval = 3
	reconnectMaxInterval = 15
	orphanLookahead      = 8
)

func Connect(ctx context.Context, app *ApplicationContext, cfg *RabbitInfo, info *ConsumerInfo) {
	for ctx.Err() == nil {
		instance := app.Clone()
		if err := NewInstance(ctx, cfg, info, instance); err != nil {
			g.Log().Warningf(ctx, "%s@%s-%d -> failed to create instance: %+v", info.Queue, app.Name, app.Index, err)
			if !sleepOrDone(ctx, reconnectInterval()) {
				return
			}
			continue
		}

		instance.consume(ctx)
		instance.close(ctx)
		if ctx.Err() != nil || !sleepOrDone(ctx, reconnectInterval()) {
			return
		}
	}
}

func (c *ApplicationContext) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-c.Done:
			g.Log().Warningf(ctx, "%s@%d consumer stopped: %+v", c.Queue, c.Index, err)
			return
		case err, ok := <-c.ConnClose:
			if ok && err != nil {
				g.Log().Warningf(ctx, "%s@%d connection closed: %+v", c.Queue, c.Index, err)
			}
			return
		case err, ok := <-c.ChanClose:
			if ok && err != nil {
				g.Log().Warningf(ctx, "%s@%d channel closed: %+v", c.Queue, c.Index, err)
			}
			return
		case msg, ok := <-c.Messages:
			if !ok {
				return
			}
			if !c.Process(ctx, amqpDelivery{delivery: msg}) {
				return
			}
		}
	}
}

func (c *ApplicationContext) close(ctx context.Context) {
	if c.Channel != nil {
		if err := c.Channel.Cancel(c.Tag(), false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			g.Log().Warningf(ctx, "failed to cancel consumer %s: %+v", c.Tag(), err)
		}
		if err := c.Channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			g.Log().Warningf(ctx, "failed to close channel %s: %+v", c.Tag(), err)
		}
		c.Channel = nil
	}
	if c.Conn != nil {
		if err := c.Conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			g.Log().Warningf(ctx, "failed to close connection %s: %+v", c.Tag(), err)
		}
		c.Conn = nil
	}
}

func NewInstance(ctx context.Context, config *RabbitInfo, info *ConsumerInfo, app *ApplicationContext) error {
	conn, err := dial(ctx, config, app.Tag())
	if err != nil {
		return fmt.Errorf("failed to connect %s: %w", config.SafeAddress(), err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	prefetch := info.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	if err = channel.Qos(prefetch, 0, false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to set qos: %w", err)
	}
	messages, err := channel.ConsumeWithContext(ctx, app.Queue, app.Tag(), false, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("failed to consume queue %s: %w", app.Queue, err)
	}

	app.Conn = conn
	app.Channel = channel
	app.Messages = messages
	app.ConnClose = conn.NotifyClose(make(chan *amqp.Error, 1))
	app.ChanClose = channel.NotifyClose(make(chan *amqp.Error, 1))
	g.Log().Infof(ctx, "rabbitmq client ready, queue:%s, tag:%s", app.Queue, app.Tag())
	return nil
}

func NewBind(ctx context.Context, config *RabbitInfo, cfg *ConsumerInfo, queues []string) error {
	if config == nil || cfg == nil {
		return errors.New("rabbitmq config is empty")
	}
	if config.Exchange == "" {
		return errors.New("exchange is empty")
	}
	if len(queues) == 0 {
		return errors.New("queue name is empty")
	}
	for _, queue := range queues {
		if queue == "" {
			return errors.New("queue name is empty")
		}
	}

	conn, err := dial(ctx, config, cfg.Name+"-declare")
	if err != nil {
		return fmt.Errorf("failed to connect %s: %w", config.SafeAddress(), err)
	}
	defer func() { _ = conn.Close() }()
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open declare channel: %w", err)
	}
	defer func() { _ = channel.Close() }()

	for _, queue := range queues {
		args := amqp.Table{}
		if cfg.Quorum {
			args["x-queue-type"] = "quorum"
		}
		if cfg.DeadLetterExchange != "" {
			args["x-dead-letter-exchange"] = cfg.DeadLetterExchange
		}
		if _, err = channel.QueueDeclare(queue, true, false, false, false, args); err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", queue, err)
		}
		for _, routeKey := range cfg.Bind {
			g.Log().Infof(ctx, "binding %s -> %s -> %s", config.Exchange, queue, routeKey)
			if err = channel.QueueBind(queue, routeKey, config.Exchange, false, nil); err != nil {
				return fmt.Errorf("failed to bind %s -> %s with key %s: %w", config.Exchange, queue, routeKey, err)
			}
		}
	}
	warnOrphanQueues(ctx, conn, cfg.Queue, queues)
	return nil
}

func dial(ctx context.Context, config *RabbitInfo, connectionName string) (*amqp.Connection, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	options := amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
		Properties: amqp.Table{
			"connection_name": connectionName,
		},
	}
	if config.TLS {
		options.TLSClientConfig = &tls.Config{
			ServerName:         config.Host,
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}
	return amqp.DialConfig(config.Address(), options)
}

func orphanCandidates(queue string, active []string) []string {
	using := make(map[string]struct{}, len(active))
	for _, name := range active {
		using[name] = struct{}{}
	}
	candidates := make([]string, 0, len(active)+orphanLookahead+1)
	appendIfUnused := func(name string) {
		if _, ok := using[name]; ok {
			return
		}
		using[name] = struct{}{}
		candidates = append(candidates, name)
	}
	appendIfUnused(queue)
	for i := 0; i < len(active)+orphanLookahead; i++ {
		appendIfUnused(ShardQueueName(queue, i, 2))
	}
	return candidates
}

func warnOrphanQueues(ctx context.Context, conn *amqp.Connection, queue string, active []string) {
	for _, candidate := range orphanCandidates(queue, active) {
		channel, err := conn.Channel()
		if err != nil {
			return
		}
		info, inspectErr := channel.QueueInspect(candidate)
		_ = channel.Close()
		if inspectErr != nil {
			var amqpErr *amqp.Error
			if !errors.As(inspectErr, &amqpErr) || amqpErr.Code != 404 {
				g.Log().Debugf(ctx, "check orphan queue %s failed: %+v", candidate, inspectErr)
			}
			continue
		}
		g.Log().Warningf(ctx, "发现可能残留的队列 %s（消息 %d 条，消费者 %d 个），当前使用的是 %v。若已不再需要，请先解绑再删除", candidate, info.Messages, info.Consumers, active)
	}
}

func reconnectInterval() time.Duration {
	return time.Duration(grand.N(reconnectMinInterval, reconnectMaxInterval)) * time.Second
}

func sleepOrDone(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func logPanic(ctx context.Context, name string) {
	if recovered := recover(); recovered != nil {
		g.Log().Errorf(ctx, "%s panic:%+v, stack:%s", name, recovered, debug.Stack())
	}
}
