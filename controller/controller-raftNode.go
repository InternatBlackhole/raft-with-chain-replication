package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	pb "timkr.si/ps-izziv/controller/rpc"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type controllerNode struct {
	sync.RWMutex

	hbPort int

	state *raftState
	raft  *raft.Raft

	pb.UnimplementedControllerServer
}

func newControllerNode(hbPort int, raft *raft.Raft, state *raftState) *controllerNode {
	if state == nil {
		panic("state is nil")
	}
	this := &controllerNode{hbPort: hbPort, raft: raft, state: state}
	return this
}

func (c *controllerNode) GetLeader(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	if c.raft.State() == raft.Candidate {
		return nil, errors.New("elections ongoing")
	}
	laddr, lid := c.raft.LeaderWithID()
	if lid == "" {
		return nil, errors.New("no leader")
	}
	return getNodeFromHostname(string(laddr))
}

func (c *controllerNode) GetHead(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	c.state.mtx.RLock()
	defer c.state.mtx.RUnlock()
	if c.state.head == nil {
		return nil, nil
	}
	return c.state.head.ToNode(), nil
}

func (c *controllerNode) GetTail(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	c.state.mtx.RLock()
	defer c.state.mtx.RUnlock()
	if c.state.tail == nil {
		return nil, nil
	}
	return c.state.tail.ToNode(), nil
}

func (c *controllerNode) GetHeartbeatEndpoint(ctx context.Context, in *emptypb.Empty) (*wrapperspb.UInt32Value, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}
	return &wrapperspb.UInt32Value{Value: uint32(c.hbPort)}, nil
}

func (c *controllerNode) RegisterAsReplicator(ctx context.Context, in *pb.Node) (*pb.Neighbors, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}

	fmt.Println("Registering replicator: ", in.Address, ":", in.Port, " with id: ", *in.Id)

	if in.Id == nil || *in.Id == "" {
		return nil, errors.New("node id not set")
	}

	c.Lock()
	defer c.Unlock()

	data, err := proto.Marshal(in)
	if err != nil {
		fmt.Println("Error marshalling node: ", err)
		return nil, err
	}

	//also add to extensions
	log := raft.Log{Data: data, Extensions: []byte(NodeAdd)}
	af := c.raft.ApplyLog(log, 1*time.Second)
	err, ret := af.Error(), af.Response()
	if err != nil {
		fmt.Println("Error applying log: ", err)
		return nil, errors.New("error occured")
	}
	if err, ok := ret.(error); ok {
		fmt.Println("Error applying log: ", err)
		return nil, errors.New("error occured")
	}
	//ret is a [3]*replicactionNode of prev, current, next nodes
	if arr, ok := ret.([]*replicationNode); ok {
		fmt.Println("Added replicator: ", in.Address, ":", in.Port, " with id: ", *in.Id)
		n, e := c.replNodeAdded(arr[0], arr[1], arr[2])
		if n == nil {
			fmt.Println("Error adding replicator: ", e)
			return nil, errors.New("error occured")
		}
		return n, e
	} else {
		fmt.Println("Unknown return type: ", ret)
		return nil, errors.New("error occured")
	}
}

func (c *controllerNode) RegisterAsController(ctx context.Context, in *pb.Node) (*wrapperspb.UInt64Value, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}

	if in.Id == nil || *in.Id == "" {
		return nil, errors.New("node id not set")
	}

	fu := c.raft.AddVoter(raft.ServerID(*in.Id), raft.ServerAddress(in.Address+":"+strconv.Itoa(int(in.Port))), 0, 1*time.Second)
	err := fu.Error()
	if err != nil {
		fmt.Println("Error adding voter: ", err)
		return nil, err
	}
	return &wrapperspb.UInt64Value{Value: fu.Index()}, nil
}

func (c *controllerNode) mustEmbedUnimplementedControllerServer() {
	panic("mustEmbedUnimplementedControllerServer")
}

func getNodeFromHostname(host string) (*pb.Node, error) {
	host, port, err := net.SplitHostPort(host)
	if err != nil {
		fmt.Println("Error splitting hostname: ", err)
		return nil, errors.New("error occured")
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		fmt.Println("Error parsing port: ", err)
		return nil, errors.New("error occured")
	}
	return &pb.Node{Address: host, Port: uint32(p)}, nil
}

func (c *controllerNode) changeHead(newHead *replicationNode) {
	node := newHead.ToNode()
	data, err := proto.Marshal(node)
	if err != nil {
		fmt.Println("Error marshalling node: ", err)
		return
	}
	fu := c.raft.ApplyLog(raft.Log{Data: data, Extensions: []byte(HeadChaned)}, 2*time.Second)
	if err := fu.Error(); err != nil {
		fmt.Println("Error applying log: ", err)
	}
	_, ok := fu.Response().(*replicationNode)
	if !ok {
		fmt.Println("Unknown return type: ", fu.Response())
		return
	}
}

func (c *controllerNode) changeTail(newTail *replicationNode) {
	node := newTail.ToNode()
	data, err := proto.Marshal(node)
	if err != nil {
		fmt.Println("Error marshalling node: ", err)
		return
	}
	fu := c.raft.ApplyLog(raft.Log{Data: data, Extensions: []byte(TailChaned)}, 2*time.Second)
	if err := fu.Error(); err != nil {
		fmt.Println("Error applying log: ", err)
	}
	_, ok := fu.Response().(*replicationNode)
	if !ok {
		fmt.Println("Unknown return type: ", fu.Response())
		return
	}
}

// run when a replication node is added, always adds to the end of the chain (next should be nil)
func (c *controllerNode) replNodeAdded(prev, current, next *replicationNode) (*pb.Neighbors, error) {
	//next should always be nil

	if prev == nil {
		//there was no previous node
		c.changeHead(current)
		c.changeTail(current)
		return &pb.Neighbors{Prev: prev.ToNode(), Next: &pb.Node{}}, nil
	}
	currentNode := current.ToNode()

	//there was a previous node
	prevClient, err := prev.lazyDial()
	if err != nil {
		fmt.Println("Error dialing previous node ", prev.String(), ": ", err)
		return nil, err
	}

	ctx, cancel := ctxTimeout()
	_, err = prevClient.NextChanged(ctx, currentNode)
	if err != nil {
		fmt.Println("Error notifying next node ", prev.String(), ": ", err)
	}
	cancel()

	c.changeTail(current)

	return &pb.Neighbors{Prev: prev.ToNode(), Next: &pb.Node{}}, nil
}

func ctxTimeout() (context.Context, context.CancelFunc) {
	//TODO: change timeout to 1 second
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// run when a replication node is removed
func (c *controllerNode) replNodeRemoved(prev, removed, next *replicationNode) {

	if prev == nil && next == nil {
		//there was only one node, now there are none, reset head and tail
		fmt.Println("Case prev == nil && next == nil")
		c.changeHead(nil)
		c.changeTail(nil)
		return
	} else if prev == nil && next != nil {
		//there was no previous node, and it was not the last node
		//become head
		fmt.Println("Case prev == nil && next != nil")
		c.changeHead(next)
		nextClient, err := next.lazyDial()
		if err != nil {
			fmt.Println("Error dialing next node ", next.String(), ": ", err)
		}

		ctx, cancel := ctxTimeout()
		_, err = nextClient.PrevChanged(ctx, &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying next node ", next.String(), ": ", err)
		}
		cancel()
	} else if prev != nil && next == nil {
		//there was no next node, and it was not the first node
		//become tail
		fmt.Println("Case prev != nil && next == nil")
		c.changeTail(prev)
		prevClient, err := prev.lazyDial()
		if err != nil {
			fmt.Println("Error dialing previous node ", prev.String(), ": ", err)
		}

		ctx, cancel := ctxTimeout()
		_, err = prevClient.NextChanged(ctx, &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying previous node ", prev.String(), ": ", err)
		}
		cancel()
	} else {
		//middle node
		//contact both
		fmt.Println("Case prev != nil && next != nil")
		prevClient, err := prev.lazyDial()
		if err != nil {
			fmt.Println("Error dialing previous node ", prev.String(), ": ", err)
		}

		ctx, cancel := ctxTimeout()
		_, err = prevClient.NextChanged(ctx, &pb.Node{Address: next.addr, Port: uint32(next.port)})
		if err != nil {
			fmt.Println("Error notifying previous node ", prev.String(), ": ", err)
		}
		cancel()

		nextClient, err := next.lazyDial()
		if err != nil {
			fmt.Println("Error dialing next node ", next.String(), ": ", err)
		}

		ctx, cancel = ctxTimeout()
		_, err = nextClient.PrevChanged(ctx, &pb.Node{Address: prev.addr, Port: uint32(prev.port)})
		if err != nil {
			fmt.Println("Error notifying next node ", next.String(), ": ", err)
		}
		cancel()
	}
}
