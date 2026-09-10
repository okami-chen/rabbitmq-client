package rabbitmq_client

import (
	"context"
	"errors"
	"testing"

	"github.com/gogf/gf/v2/encoding/gjson"
)

type fakeDelivery struct {
	body     []byte
	message  string
	ack      bool
	nack     bool
	reject   bool
	requeue  bool
	ackError error
}

func (d *fakeDelivery) Body() []byte      { return d.body }
func (d *fakeDelivery) MessageID() string { return d.message }
func (d *fakeDelivery) Ack() error {
	d.ack = true
	return d.ackError
}
func (d *fakeDelivery) Nack(requeue bool) error {
	d.nack = true
	d.requeue = requeue
	return nil
}
func (d *fakeDelivery) Reject(requeue bool) error {
	d.reject = true
	d.requeue = requeue
	return nil
}

type testProcessor struct {
	err    error
	panics bool
	pk     string
	calls  int
}

func (p *testProcessor) Process(context.Context, *gjson.Json) error {
	p.calls++
	if p.panics {
		panic("boom")
	}
	return p.err
}
func (p *testProcessor) Clone() ProcessorInterface {
	return &testProcessor{err: p.err, panics: p.panics, pk: p.pk}
}
func (p *testProcessor) Pk() string                        { return p.pk }
func (p *testProcessor) Name() string                      { return "test" }
func (p *testProcessor) Set(key string, value interface{}) {}

func newTestApp(processor ProcessorInterface, requeue bool) *ApplicationContext {
	return &ApplicationContext{
		Queue:     "test-queue",
		Requeue:   requeue,
		Done:      make(chan error, 1),
		Processor: processor,
	}
}

func TestProcessAck(t *testing.T) {
	processor := &testProcessor{}
	app := newTestApp(processor, false)
	msg := &fakeDelivery{body: []byte(`{"data":{"id":1}}`), message: "1"}
	if !app.Process(context.Background(), msg) {
		t.Fatal("successful message should continue")
	}
	if !msg.ack || msg.nack || msg.reject || processor.calls != 1 {
		t.Fatalf("unexpected delivery state: %+v, calls:%d", msg, processor.calls)
	}
}

func TestProcessFailureSettlement(t *testing.T) {
	requeue := &fakeDelivery{body: []byte(`{"data":{"id":1}}`)}
	newTestApp(&testProcessor{err: errors.New("failed")}, true).Process(context.Background(), requeue)
	if !requeue.nack || !requeue.requeue {
		t.Fatalf("failed message should be nacked and requeued: %+v", requeue)
	}

	discard := &fakeDelivery{body: []byte(`{"data":{"id":1}}`)}
	newTestApp(&testProcessor{err: errors.New("failed")}, false).Process(context.Background(), discard)
	if !discard.reject || discard.requeue {
		t.Fatalf("failed message should be rejected without requeue: %+v", discard)
	}
}

func TestInvalidPayloadAlwaysRejects(t *testing.T) {
	processor := &testProcessor{}
	msg := &fakeDelivery{body: []byte("not-json")}
	newTestApp(processor, true).Process(context.Background(), msg)
	if !msg.reject || msg.requeue || processor.calls != 0 {
		t.Fatalf("invalid payload settlement is incorrect: %+v", msg)
	}
}

func TestAckFailureStopsConsumer(t *testing.T) {
	app := newTestApp(&testProcessor{}, false)
	msg := &fakeDelivery{body: []byte(`{"data":{"id":1}}`), ackError: errors.New("closed")}
	if app.Process(context.Background(), msg) {
		t.Fatal("ack failure should stop current consumer")
	}
	if len(app.Done) != 1 {
		t.Fatal("ack failure should notify reconnect loop")
	}
}

func TestProcessShard(t *testing.T) {
	processor := &testProcessor{pk: "order_no"}
	app := newTestApp(processor, false)
	app.Pk = "order_no"
	app.Total = 2
	app.Index = 0
	key := "a"
	for HashShardByCount(key, app.Total) == app.Index {
		key += "a"
	}
	msg := &fakeDelivery{body: []byte(`{"data":{"order_no":"` + key + `"}}`)}
	app.Process(context.Background(), msg)
	if !msg.ack || processor.calls != 0 {
		t.Fatalf("message owned by another shard should only be acked: %+v", msg)
	}
}

func TestMissingShardKeyRejects(t *testing.T) {
	processor := &testProcessor{pk: "order_no"}
	app := newTestApp(processor, true)
	app.Pk = "order_no"
	app.Total = 2
	msg := &fakeDelivery{body: []byte(`{"data":{}}`)}
	app.Process(context.Background(), msg)
	if !msg.reject || msg.requeue || processor.calls != 0 {
		t.Fatalf("message without shard key should be rejected: %+v", msg)
	}
}
