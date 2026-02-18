package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type streamConsumerOptions struct {
	endpoint   string
	stream     string
	group      string
	max        int
	visibility int
	consumerID string
	sleep      time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	command := newStreamConsumerCommand()
	return command.Execute()
}

func newStreamConsumerCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "stream-consumer",
		Short:        "Pull and acknowledge messages from a Wallaby stream",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStreamConsumer(cmd)
		},
	}
	command.Flags().String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	command.Flags().String("stream", "orders", "stream name")
	command.Flags().String("group", "search", "consumer group")
	command.Flags().Int("max", 10, "max messages to pull")
	command.Flags().Int("visibility", 30, "visibility timeout seconds")
	command.Flags().String("consumer", "", "consumer id")
	command.Flags().Duration("sleep", 2*time.Second, "sleep between empty polls")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initStreamConsumerConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initStreamConsumerConfig(_ *cobra.Command) error {
	viper.Reset()
	viper.SetEnvPrefix("WALLABY_STREAM_CONSUMER")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	return nil
}

func runStreamConsumer(cmd *cobra.Command) error {
	opts := streamConsumerOptions{
		endpoint:   resolveStringFlag(cmd, "endpoint"),
		stream:     resolveStringFlag(cmd, "stream"),
		group:      resolveStringFlag(cmd, "group"),
		max:        resolveIntFlag(cmd, "max"),
		visibility: resolveIntFlag(cmd, "visibility"),
		consumerID: resolveStringFlag(cmd, "consumer"),
		sleep:      resolveDurationFlag(cmd, "sleep"),
	}
	return runConsumerLoop(opts)
}

func runConsumerLoop(opts streamConsumerOptions) error {
	if opts.max > math.MaxInt32 {
		return fmt.Errorf("max exceeds int32: %d", opts.max)
	}
	if opts.visibility > math.MaxInt32 {
		return fmt.Errorf("visibility exceeds int32: %d", opts.visibility)
	}

	maxMessages := int32(opts.max)              // #nosec G115 -- bounds checked above.
	visibilitySeconds := int32(opts.visibility) // #nosec G115 -- bounds checked above.
	conn, err := grpc.NewClient(opts.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
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
			Stream:                   opts.stream,
			ConsumerGroup:            opts.group,
			MaxMessages:              maxMessages,
			VisibilityTimeoutSeconds: visibilitySeconds,
			ConsumerId:               opts.consumerID,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("pull: %w", err)
		}

		if len(resp.Messages) == 0 {
			time.Sleep(opts.sleep)
			continue
		}

		ids := make([]int64, 0, len(resp.Messages))
		for _, msg := range resp.Messages {
			fmt.Printf("id=%d table=%s lsn=%s bytes=%d\n", msg.Id, msg.Table, msg.Lsn, len(msg.Payload))
			ids = append(ids, msg.Id)
		}

		ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = client.Ack(ackCtx, &wallabypb.StreamAckRequest{
			Stream:        opts.stream,
			ConsumerGroup: opts.group,
			Ids:           ids,
		})
		ackCancel()
		if err != nil {
			return fmt.Errorf("ack: %w", err)
		}
	}
}

func resolveStringFlag(cmd *cobra.Command, key string) string {
	value, err := cmd.Flags().GetString(key)
	if err != nil {
		return ""
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetString(key)
	}
	return value
}

func resolveIntFlag(cmd *cobra.Command, key string) int {
	value, err := cmd.Flags().GetInt(key)
	if err != nil {
		return 0
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetInt(key)
	}
	return value
}

func resolveDurationFlag(cmd *cobra.Command, key string) time.Duration {
	value, err := cmd.Flags().GetDuration(key)
	if err != nil {
		return 0
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetDuration(key)
	}
	return value
}
