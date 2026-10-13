// Package rabbitmq_client 基于RabbitMQ官方AMQP 0.9.1客户端提供topic队列消费封装。
package rabbitmq_client

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
)

type RabbitMQConfig struct {
	*RabbitInfo
	Consumers []ConsumersConfig `yaml:"consumers" json:"consumers"`

	wg sync.WaitGroup // 跟踪Start启动的全部协程，供Wait等待退出
}

// Wait 等待Start启动的协程全部退出（需先取消传给Start的ctx），超时返回false
func (r *RabbitMQConfig) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (r *RabbitMQConfig) Start(ctx context.Context, handles map[string][]ProcessorInterface) {
	if r == nil || r.RabbitInfo == nil {
		g.Log().Error(ctx, "rabbitmq client config is empty, nothing to start")
		return
	}

	for _, consumer := range r.Consumers {
		item := consumer
		if item.ConsumerInfo == nil {
			g.Log().Warning(ctx, "ignore rabbitmq client consumer with empty config")
			continue
		}
		if !item.Enable {
			g.Log().Infof(ctx, "ignore rabbitmq client %s@%s", item.Name, item.Queue)
			continue
		}

		objects, ok := handles[item.Name]
		if !ok || len(objects) == 0 {
			g.Log().Warningf(ctx, "ignore rabbitmq client %s, no processor registered", item.Name)
			continue
		}
		if len(objects) > 1 {
			g.Log().Errorf(ctx, "rabbitmq client %s registered %d processors; each consumer must use an independent queue", item.Name, len(objects))
			continue
		}

		instance := objects[0]
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer logPanic(ctx, "rabbitmq client starter")
			g.Log().Infof(ctx, "start rabbitmq client %s@%s", item.Name, instance.Name())
			item.run(ctx, r.RabbitInfo, instance, &r.wg)
		}()
	}
}

type RabbitInfo struct {
	Host          string `yaml:"host" json:"host"`
	Port          int    `yaml:"port" json:"port"`
	Username      string `yaml:"username" json:"username"`
	Password      string `yaml:"password" json:"password"`
	VHost         string `yaml:"vhost" json:"vhost"`
	Exchange      string `yaml:"exchange" json:"exchange"`
	TLS           bool   `yaml:"tls" json:"tls"`
	TLSSkipVerify bool   `yaml:"tlsSkipVerify" json:"tlsSkipVerify"`
}

func (r *RabbitInfo) Address() string {
	return r.buildURL(false)
}

func (r *RabbitInfo) SafeAddress() string {
	return r.buildURL(true)
}

func (r *RabbitInfo) buildURL(mask bool) string {
	scheme := "amqp"
	if r.TLS {
		scheme = "amqps"
	}
	host := r.Host
	if r.Port > 0 {
		host = net.JoinHostPort(r.Host, strconv.Itoa(r.Port))
	}
	u := &url.URL{Scheme: scheme, Host: host, Path: "/" + strings.TrimPrefix(strings.TrimSpace(r.VHost), "/")}
	if r.Username != "" {
		if mask {
			u.User = url.UserPassword(r.Username, "******")
		} else {
			u.User = url.UserPassword(r.Username, r.Password)
		}
	}
	return u.String()
}

type ConsumersConfig struct {
	*ConsumerInfo
}

func (r *ConsumersConfig) Run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface) {
	r.run(ctx, info, handle, nil)
}

// run 在wg不为空时登记启动的消费者协程。调用方需已在wg中占位，保证Add发生在Wait归零之前
func (r *ConsumersConfig) run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface, wg *sync.WaitGroup) {
	num := r.Num
	if num <= 0 {
		num = 1
	}
	if num > 1 && handle.Pk() == "" {
		g.Log().Errorf(ctx, "rabbitmq client %s@%s num=%d but processor %s has no shard key", r.Name, r.Queue, num, handle.Name())
		return
	}

	queues := make([]string, 0, num)
	for i := 0; i < num; i++ {
		queues = append(queues, ShardQueueName(r.Queue, i, num))
	}

	for {
		if err := NewBind(ctx, info, r.ConsumerInfo, queues); err != nil {
			g.Log().Warningf(ctx, "declare rabbitmq client %s@%s failed: %+v", r.Name, r.Queue, err)
			if !sleepOrDone(ctx, reconnectInterval()) {
				return
			}
			continue
		}
		break
	}

	for i := 0; i < num; i++ {
		index := i
		instance := handle.Clone()
		instance.Set("key", fmt.Sprintf("%s:%s:%s:%03d", info.Exchange, r.Name, instance.Name(), index))
		instance.Set("index", index)
		if wg != nil {
			wg.Add(1)
		}
		go func() {
			if wg != nil {
				defer wg.Done()
			}
			defer logPanic(ctx, "rabbitmq client consumer")
			app := &ApplicationContext{
				AppName:   info.Exchange,
				Name:      r.Name,
				Queue:     ShardQueueName(r.Queue, index, num),
				Pk:        handle.Pk(),
				Index:     index,
				Total:     num,
				Requeue:   r.Requeue,
				Done:      make(chan error, 1),
				Processor: instance,
			}
			Connect(ctx, app, info, r.ConsumerInfo)
		}()
	}
}

type ConsumerInfo struct {
	Name               string        `yaml:"name" json:"name"`
	Queue              string        `yaml:"queue" json:"queue"`
	Enable             bool          `yaml:"enable" json:"enable"`
	Key                string        `yaml:"key" json:"key"`
	Num                int           `yaml:"num" json:"num"`
	Bind               []string      `yaml:"bind" json:"bind"`
	Requeue            bool          `yaml:"requeue" json:"requeue"`
	Quorum             bool          `yaml:"quorum" json:"quorum"`
	DeadLetterExchange string        `yaml:"deadLetterExchange" json:"deadLetterExchange"`
	ReceiveTimeout     time.Duration `yaml:"receiveTimeout" json:"receiveTimeout"`
	Prefetch           int           `yaml:"prefetch" json:"prefetch"`
}

func ShardQueueName(queue string, index, total int) string {
	if total <= 1 {
		return queue
	}
	return fmt.Sprintf("%s-%03d", queue, index)
}
