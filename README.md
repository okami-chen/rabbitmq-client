# rabbitmq-client

基于 RabbitMQ 官方 amqp091-go 的 AMQP 0.9.1 topic 队列消费封装，面向 RabbitMQ 4.x。

接口与 rabbitmq-topic 保持一致，支持：

- topic exchange 路由键绑定
- 手动 ACK、失败 NACK/Reject 与死信交换机
- classic/quorum 持久化队列
- 按业务主键分片的多副本队列
- QoS/prefetch、TLS、连接与 channel 断线重连
- 启动时声明失败持续重试

主服务通过 go run main.go client 启动，读取 rabbitmq.client 配置。
