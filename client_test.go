package rabbitmq_client

import (
	"strings"
	"testing"
)

func TestAddress(t *testing.T) {
	info := &RabbitInfo{
		Host:     "127.0.0.1",
		Port:     5672,
		Username: "user@name",
		Password: "p@ss:w/rd",
		VHost:    "prod",
	}
	if strings.Contains(info.Address(), info.Password) {
		t.Fatal("credentials must be URL encoded")
	}
	if !strings.HasSuffix(info.Address(), "/prod") {
		t.Fatalf("unexpected address: %s", info.Address())
	}
	if strings.Contains(info.SafeAddress(), info.Password) {
		t.Fatal("safe address must mask password")
	}
}

func TestAddressTLS(t *testing.T) {
	info := &RabbitInfo{Host: "mq.test", Port: 5671, TLS: true}
	if !strings.HasPrefix(info.Address(), "amqps://") {
		t.Fatalf("unexpected TLS address: %s", info.Address())
	}
}

func TestShardQueueName(t *testing.T) {
	if got := ShardQueueName("orders", 0, 1); got != "orders" {
		t.Fatalf("unexpected single shard queue: %s", got)
	}
	if got := ShardQueueName("orders", 2, 3); got != "orders-002" {
		t.Fatalf("unexpected shard queue: %s", got)
	}
}

func TestOrphanCandidates(t *testing.T) {
	active := []string{"orders-000", "orders-001"}
	candidates := orphanCandidates("orders", active)
	contains := func(expected string) bool {
		for _, candidate := range candidates {
			if candidate == expected {
				return true
			}
		}
		return false
	}
	if !contains("orders") || !contains("orders-002") {
		t.Fatalf("missing orphan candidates: %v", candidates)
	}
}
