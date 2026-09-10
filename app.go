package rabbitmq_client

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-client/consts"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProcessorInterface interface {
	Process(ctx context.Context, j *gjson.Json) error
	Clone() ProcessorInterface
	Pk() string
	Name() string
	Set(key string, value interface{})
}

type delivery interface {
	Body() []byte
	MessageID() string
	Ack() error
	Nack(requeue bool) error
	Reject(requeue bool) error
}

type amqpDelivery struct {
	delivery amqp.Delivery
}

func (d amqpDelivery) Body() []byte              { return d.delivery.Body }
func (d amqpDelivery) MessageID() string         { return d.delivery.MessageId }
func (d amqpDelivery) Ack() error                { return d.delivery.Ack(false) }
func (d amqpDelivery) Nack(requeue bool) error   { return d.delivery.Nack(false, requeue) }
func (d amqpDelivery) Reject(requeue bool) error { return d.delivery.Reject(requeue) }

type ApplicationContext struct {
	AppName   string
	Name      string
	Queue     string
	Pk        string
	Index     int
	Total     int
	Requeue   bool
	Done      chan error
	Conn      *amqp.Connection
	Channel   *amqp.Channel
	Messages  <-chan amqp.Delivery
	ConnClose <-chan *amqp.Error
	ChanClose <-chan *amqp.Error
	Processor ProcessorInterface
}

func (c *ApplicationContext) Process(ctx context.Context, msg delivery) (next bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			g.Log().Errorf(ctx, "%s@%d process panic:%+v, stack:%s", c.Queue, c.Index, recovered, debug.Stack())
			if err := c.settle(msg); err != nil {
				c.notifyDone(fmt.Errorf("panic settlement failed: %w", err))
				next = false
				return
			}
			next = true
		}
	}()

	if msg == nil {
		return true
	}
	body := msg.Body()
	j, err := gjson.DecodeToJson(body)
	if err != nil {
		g.Log().Errorf(ctx, "%s@%d decode message failed:%+v", c.Queue, c.Index, err)
		if err = msg.Reject(false); err != nil {
			c.notifyDone(fmt.Errorf("reject invalid message failed: %w", err))
			return false
		}
		return true
	}

	if c.Total > 1 {
		pkVal := j.Get("data." + c.Pk).String()
		if pkVal == "" {
			g.Log().Errorf(ctx, "%s@%d shard key data.%s is empty", c.Queue, c.Index, c.Pk)
			if err = msg.Reject(false); err != nil {
				c.notifyDone(fmt.Errorf("reject message without shard key failed: %w", err))
				return false
			}
			return true
		}
		if HashShardByCount(pkVal, c.Total) != c.Index {
			if err = msg.Ack(); err != nil {
				c.notifyDone(fmt.Errorf("ack skipped message failed: %w", err))
				return false
			}
			return true
		}
	}

	processCtx := context.WithValue(ctx, consts.ContextKeyQueue, c.Queue)
	processCtx = context.WithValue(processCtx, consts.ContextKeyMessageId, msg.MessageID())
	if err = c.Processor.Process(processCtx, j); err != nil {
		g.Log().Errorf(ctx, "%s@%d process failed:%+v", c.Queue, c.Index, err)
		if settleErr := c.settle(msg); settleErr != nil {
			c.notifyDone(fmt.Errorf("failure settlement failed: %w", settleErr))
			return false
		}
		return true
	}

	if err = msg.Ack(); err != nil {
		c.notifyDone(fmt.Errorf("ack message failed: %w", err))
		return false
	}
	g.Log().Debugf(ctx, "%s@%d processed message, id:%s, size:%d", c.Queue, c.Index, msg.MessageID(), len(body))
	return true
}

func (c *ApplicationContext) settle(msg delivery) error {
	if c.Requeue {
		return msg.Nack(true)
	}
	return msg.Reject(false)
}

func (c *ApplicationContext) notifyDone(err error) {
	if err == nil || c.Done == nil {
		return
	}
	select {
	case c.Done <- err:
	default:
	}
}

func (c *ApplicationContext) Clone() *ApplicationContext {
	processor := c.Processor.Clone()
	processor.Set("key", c.Key())
	processor.Set("index", c.Index)
	return &ApplicationContext{
		AppName:   c.AppName,
		Name:      c.Name,
		Queue:     c.Queue,
		Pk:        c.Pk,
		Index:     c.Index,
		Total:     c.Total,
		Requeue:   c.Requeue,
		Done:      make(chan error, 1),
		Processor: processor,
	}
}

func (c *ApplicationContext) Key() string {
	return fmt.Sprintf("%s:%s:%s:%03d", c.AppName, c.Name, c.Processor.Name(), c.Index)
}

func (c *ApplicationContext) Tag() string {
	return fmt.Sprintf("%s-%s-%s-%03d", c.AppName, c.Name, c.Processor.Name(), c.Index)
}
