package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	endpoint := flag.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	stream := flag.String("stream", "orders", "stream name")
	group := flag.String("group", "search", "consumer group")
	max := flag.Int("max", 10, "max messages to pull")
	visibility := flag.Int("visibility", 30, "visibility timeout seconds")
	consumerID := flag.String("consumer", "", "consumer id")
	sleep := flag.Duration("sleep", 2*time.Second, "sleep between empty polls")
	flag.Parse()

	if *max > math.MaxInt32 {
		log.Fatalf("max exceeds int32: %d", *max)
	}
	if *visibility > math.MaxInt32 {
		log.Fatalf("visibility exceeds int32: %d", *visibility)
	}
	maxMessages := int32(*max)              // #nosec G115 -- bounds checked above.
	visibilitySeconds := int32(*visibility) // #nosec G115 -- bounds checked above.

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("close connection: %v", err)
		}
	}()

	client := wallabypb.NewStreamServiceClient(conn)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.Pull(ctx, &wallabypb.StreamPullRequest{
			Stream:                   *stream,
			ConsumerGroup:            *group,
			MaxMessages:              maxMessages,
			VisibilityTimeoutSeconds: visibilitySeconds,
			ConsumerId:               *consumerID,
		})
		cancel()
		if err != nil {
			log.Printf("pull: %v", err)
			return
		}

		if len(resp.Messages) == 0 {
			time.Sleep(*sleep)
			continue
		}

		ids := make([]int64, 0, len(resp.Messages))
		for _, msg := range resp.Messages {
			fmt.Printf("id=%d table=%s lsn=%s bytes=%d\n", msg.Id, msg.Table, msg.Lsn, len(msg.Payload))
			ids = append(ids, msg.Id)
		}

		ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = client.Ack(ackCtx, &wallabypb.StreamAckRequest{
			Stream:        *stream,
			ConsumerGroup: *group,
			Ids:           ids,
		})
		ackCancel()
		if err != nil {
			log.Printf("ack: %v", err)
			return
		}
	}
}
