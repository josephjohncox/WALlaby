package main

import (
	"context"
	"errors"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	Flow wallabypb.FlowServiceClient
}

func newClient(ctx context.Context, endpoint string, insecureConn bool) (*Client, error) {
	if endpoint == "" {
		return nil, errors.New("endpoint is required")
	}

	var opts []grpc.DialOption
	if insecureConn {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, errors.New("secure grpc is not configured")
	}

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
		Flow: wallabypb.NewFlowServiceClient(conn),
	}, nil
}
