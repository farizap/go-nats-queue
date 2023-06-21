package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Topic format
// trx.{chainID}-{contractAddress}
var (
	topic1 = "1-0x132f891110795D83197105789c229a98cBeFb98b"
	topic2 = "5-0x132f891110795D83197105789c229a98cBeFb98b"
	topic3 = "2525-0x132f891110795D83197105789c229a98cBeFb98b"

	subject1 = "trx." + topic1
	subject2 = "trx." + topic2
	subject3 = "trx." + topic3
	subject  = "trx"

	streamName       = "TRX"
	deliverGroupName = "trx-processor"
	deliverSubject   = "deliver-subject"
)

func main() {

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url, nats.UserInfo("dev", "devpass"))
	defer nc.Drain()

	js, _ := nc.JetStream()

	js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{fmt.Sprintf("%s.*", subject)},
	})

	defer js.DeleteStream(streamName)

	js.Publish(subject1, []byte("whitelist (order 1)"))
	js.Publish(subject1, []byte("whitelist (order 2)"))
	js.Publish(subject3, []byte("whitelist (order 3)"))
	js.Publish(subject1, []byte("self-minting (order 1)"))
	js.Publish(subject2, []byte("whitelist (order 4)"))
	js.Publish(subject2, []byte("self-minting (order 4)"))
	js.Publish(subject3, []byte("self-minting (order 3)"))
	js.Publish(subject1, []byte("self-minting (order 2)"))

	fmt.Println("\n# Durable (explicit)")

	js.AddConsumer(streamName, &nats.ConsumerConfig{
		FilterSubject:  subject1,
		Durable:        topic1,
		DeliverSubject: topic1,
		DeliverGroup:   deliverGroupName,
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  1,
	})
	js.AddConsumer(streamName, &nats.ConsumerConfig{
		FilterSubject:  subject2,
		Durable:        topic2,
		DeliverSubject: topic2,
		DeliverGroup:   deliverGroupName,
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  1,
	})
	js.AddConsumer(streamName, &nats.ConsumerConfig{
		FilterSubject:  subject3,
		Durable:        topic3,
		DeliverSubject: topic3,
		DeliverGroup:   deliverGroupName,
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  1,
	})

	wg := &sync.WaitGroup{}
	wg.Add(8)

	nc.QueueSubscribe(topic1, deliverGroupName, func(msg *nats.Msg) {
		fmt.Printf("sub1: received message %q: %q\n", msg.Subject, string(msg.Data))
		time.Sleep(5 * time.Second) // simulate long process

		msg.Ack()
		wg.Done()
	})

	nc.QueueSubscribe(topic2, deliverGroupName, func(msg *nats.Msg) {
		fmt.Printf("sub2: received message %q: %q\n", msg.Subject, string(msg.Data))
		time.Sleep(5 * time.Second) // simulate long process

		msg.Ack()
		wg.Done()
	})

	nc.QueueSubscribe(topic3, deliverGroupName, func(msg *nats.Msg) {
		fmt.Printf("sub3: received message %q: %q\n", msg.Subject, string(msg.Data))
		time.Sleep(5 * time.Second) // simulate long process

		msg.Ack()
		wg.Done()
	})

	wg.Wait()
}
