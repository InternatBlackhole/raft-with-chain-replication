package main

//TODO: make an override of the grpc ReplicationProviderClient interface so that it reports to the controller for failures

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"timkr.si/ps-izziv/replicator/rpc"
)

type AlwaysLeaderControllerClient struct {
	rpc.ControllerClient
	leaderConn *grpc.ClientConn
	leaderLock sync.RWMutex
}

func (c *AlwaysLeaderControllerClient) GetLeader(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*rpc.Node, error) {
	c.leaderLock.RLock()
	val, err := c.ControllerClient.GetLeader(ctx, in, opts...)
	c.leaderLock.RUnlock()
	if err != nil && strings.Contains(err.Error(), "connection refused") {
		//aka leader is dead
		c.leaderLock.Lock()
		defer c.leaderLock.Unlock()
		c.leaderConn.Close()
		c.leaderConn, err = getControllerNode(c.leaderConn.Target())
		if err != nil {
			return nil, err
		}
		return c.ControllerClient.GetLeader(ctx, in, opts...)
	}
	return val, err
}

func (c *AlwaysLeaderControllerClient) GetHead(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*rpc.Node, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) GetTail(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*rpc.Node, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) GetHeartbeatEndpoint(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*wrapperspb.UInt32Value, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) RegisterAsReplicator(ctx context.Context, in *rpc.Node, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) RegisterAsController(ctx context.Context, in *rpc.Node, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) MarkTransferDone(ctx context.Context, in *rpc.Node, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}

func (c *AlwaysLeaderControllerClient) ReportDeath(ctx context.Context, in *rpc.Node, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil
}
