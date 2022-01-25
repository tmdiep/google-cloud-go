package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/pscompat"
)

var topicPath = flag.String("topic_path", "", "the full topic path to publish to")

func main() {
	testOverflow(*topicPath)
}

func testOverflow(topic string) {
	const msgSize = 500000
	const msgCount = 100

	ctx := context.Background()
	settings := pscompat.PublishSettings{
		DelayThreshold:    10 * time.Millisecond,
		CountThreshold:    100,
		ByteThreshold:     1000000,
		Timeout:           60 * time.Second,
		BufferedByteLimit: 10000000,
	}
	publisher, err := pscompat.NewPublisherClientWithSettings(ctx, topic, settings)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Stop()

	var results []*pubsub.PublishResult
	for i := 0; i < msgCount; i++ {
		r := publisher.Publish(ctx, &pubsub.Message{
			Data: bytes.Repeat([]byte{'*'}, msgSize),
		})
		results = append(results, r)
	}

	for i, r := range results {
		id, err := r.Get(ctx)
		if err != nil {
			log.Printf("Failed to publish message #%d: %v", i, err)
			if errors.Is(err, pscompat.ErrOverflow) {
				log.Printf("Publish buffer overflow detected at message #%d", i)
			}
		} else {
			log.Printf("Published message #%d with id: %s\n", i, id)
		}
	}

	if err := publisher.Error(); err != nil {
		log.Printf("Publisher failed with error: %v", err)
	}
}
