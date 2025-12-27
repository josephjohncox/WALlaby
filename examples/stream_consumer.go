package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
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

	conn, err := grpc.Dial(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	client := ductstreampb.NewStreamServiceClient(conn)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.Pull(ctx, &ductstreampb.StreamPullRequest{
			Stream:                   *stream,
			ConsumerGroup:            *group,
			MaxMessages:              int32(*max),
			VisibilityTimeoutSeconds: int32(*visibility),
			ConsumerId:               *consumerID,
		})
		cancel()
		if err != nil {
			log.Fatalf("pull: %v", err)
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
		_, err = client.Ack(ackCtx, &ductstreampb.StreamAckRequest{
			Stream:        *stream,
			ConsumerGroup: *group,
			Ids:           ids,
		})
		ackCancel()
		if err != nil {
			log.Fatalf("ack: %v", err)
		}
	}
}
