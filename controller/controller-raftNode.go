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
	//emtx     sync.RWMutex //ends mutex
	headHost replicationNode
	tailHost replicationNode

	hbPort int

	state *raftState
	raft  *raft.Raft

	//cmtx  sync.RWMutex //chain mutex
	//chain replicationChain

	pb.UnimplementedControllerServer
}

/*func genCallback(f func(*replicationNode)) func(string) {
	return func(addr string) {
		node, err := getNodeFromHostname(addr)
		if err != nil {
			return
		}
		f(newReplicationNode(node.Address, int(node.Port)))
	}
}*/

func newControllerNode(hbPort int, raft *raft.Raft, state *raftState) *controllerNode {
	if state == nil {
		panic("state is nil")
	}
	this := &controllerNode{hbPort: hbPort, raft: raft, state: state}
	//state.nodeAdded = genCallback(this.replNodeAdded)
	//state.nodeRemoved = genCallback(this.replNodeRemoved)
	return this
}

func (c *controllerNode) GetLeader(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	laddr, lid := c.raft.LeaderWithID()
	if lid == "" {
		return nil, errors.New("no leader, maybe elections are ongoing")
	}
	return getNodeFromHostname(string(laddr))
}

func (c *controllerNode) GetHead(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	c.RLock()
	defer c.RUnlock()
	return c.headHost.ToNode(), nil
}

func (c *controllerNode) GetTail(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	c.RLock()
	defer c.RUnlock()
	return c.tailHost.ToNode(), nil
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
		}
		return n, e
	} else {
		fmt.Println("Unknown return type: ", ret)
		return nil, errors.New("error occured")
	}
	//return &emptypb.Empty{}, nil
}

func (c *controllerNode) RegisterAsController(ctx context.Context, in *pb.Node) (*wrapperspb.UInt64Value, error) {
	//return nil, errors.New("not implemented")
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

// run when a replication node is added, always adds to the end of the chain (next should be nil)
func (c *controllerNode) replNodeAdded(prev, current, next *replicationNode) (*pb.Neighbors, error) {
	//next should always be nil

	/*nodeClient, err := current.lazyDial()
	if err != nil {
		fmt.Println("Error dialing node: ", err)
		return nil, err
	}*/

	if prev == nil {
		//there was no previous node
		c.headHost = *current
		c.tailHost = *current
		/*ctx, cancel := ctxTimeout()
		_, err = nodeClient.PrevChanged(ctx, &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying node ", current.String(), ": ", err)
		}
		cancel()
		ctx, cancel = ctxTimeout()
		_, err = nodeClient.NextChanged(ctx, &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying node ", current.String(), ": ", err)
		}
		cancel()*/
		return &pb.Neighbors{Prev: prev.ToNode(), Next: &pb.Node{}}, nil
	}

	//prevNode := prev.ToNode()
	//nextNode := next.ToNode()
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

	/*ctx, cancel = ctxTimeout()
	_, err = nodeClient.PrevChanged(ctx, prevNode)
	if err != nil {
		fmt.Println("Error notifying node ", current.String(), ": ", err)
	}
	cancel()

	ctx, cancel = ctxTimeout()
	_, err = nodeClient.NextChanged(ctx, &pb.Node{Address: "", Port: 0})
	if err != nil {
		fmt.Println("Error notifying node ", current.String(), ": ", err)
	}
	cancel()*/

	c.tailHost = *current

	return &pb.Neighbors{Prev: prev.ToNode(), Next: &pb.Node{}}, nil
}

func ctxTimeout() (context.Context, context.CancelFunc) {
	//TODO: change timeout to 1 second
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// run when a replication node is removed
func (c *controllerNode) replNodeRemoved(prev, removed, next *replicationNode) {
	//TODO: i think this is buggy
	//c.cmtx.Lock()
	//defer c.cmtx.Unlock()

	if prev == nil && next == nil {
		//there was only one node, now there are none, reset head and tail
		c.headHost = replicationNode{}
		c.tailHost = replicationNode{}
		return
	} else if prev == nil && next != nil {
		//there was no previous node, and it was not the last node
		//become head
		c.headHost = *next
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
		c.tailHost = *prev
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

	//c.emtx.Unlock()
}
