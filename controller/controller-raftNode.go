package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
	pb "tkNaloga04/rpc"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type controllerNode struct {
	emtx     sync.RWMutex //ends mutex
	headHost replicationNode
	tailHost replicationNode

	hbPort int

	state *raftState
	raft  *raft.Raft

	cmtx  sync.RWMutex //chain mutex
	chain replicationChain

	pb.UnimplementedControllerServer
}

func genCallback(f func(*replicationNode)) func(string) {
	return func(addr string) {
		node, err := getNodeFromHostname(addr)
		if err != nil {
			return
		}
		f(newReplicationNode(node.Address, int(node.Port)))
	}
}

func newControllerNode(hbPort int, raft *raft.Raft, state *raftState) *controllerNode {
	if state == nil {
		panic("state is nil")
	}
	this := &controllerNode{hbPort: hbPort, raft: raft, state: state}
	state.nodeAdded = genCallback(this.replNodeAdded)
	state.nodeRemoved = genCallback(this.replNodeRemoved)
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
	c.emtx.RLock()
	defer c.emtx.RUnlock()
	return c.headHost.ToNode(), nil
}

func (c *controllerNode) GetTail(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	c.emtx.RLock()
	defer c.emtx.RUnlock()
	return c.tailHost.ToNode(), nil
}

func (c *controllerNode) GetHeartbeatPort(ctx context.Context, in *emptypb.Empty) (*wrapperspb.UInt32Value, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}
	return &wrapperspb.UInt32Value{Value: uint32(c.hbPort)}, nil
}

func (c *controllerNode) RegisterAsReplicator(ctx context.Context, in *pb.Node) (*emptypb.Empty, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}

	//also add to extensions
	log := raft.Log{Data: []byte(in.Address + ":" + strconv.Itoa(int(in.Port))), Extensions: []byte(NodeAdd)}
	af := c.raft.ApplyLog(log, 1*time.Second)
	err := af.Error()
	if err != nil {
		fmt.Println("Error applying log: ", err)
		return &emptypb.Empty{}, errors.New("error occured")
	}
	return &emptypb.Empty{}, nil
}

func (c *controllerNode) RegisterAsController(ctx context.Context, in *pb.Node) (*emptypb.Empty, error) {
	if c.raft.State() != raft.Leader {
		return nil, errors.New("not leader")
	}

	if in.Id == nil {
		return nil, errors.New("node id not set")
	}
	if *in.Id == "" {
		return nil, errors.New("node id invalid")
	}

	fu := c.raft.AddVoter(raft.ServerID(*in.Id), raft.ServerAddress(in.Address+":"+strconv.Itoa(int(in.Port))), 0, 1*time.Second)
	err := fu.Error()
	if err != nil {
		fmt.Println("Error adding voter: ", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
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

// run when a replication node is added, always adds to the end of the chain
func (c *controllerNode) replNodeAdded(node *replicationNode) {
	c.cmtx.Lock()
	prev := c.chain.AddNode(node)
	defer c.cmtx.Unlock()
	if prev == nil {
		//there was no previous node
		c.emtx.Lock()
		c.headHost = *node
		c.tailHost = *node
		c.emtx.Unlock()
		return
	}
	//there was a previous node
	prevClient, err := prev.lazyDial()
	if err != nil {
		fmt.Println("Error dialing previous node: ", err)
		return
	}
	nodeClient, err := node.lazyDial()
	if err != nil {
		fmt.Println("Error dialing node: ", err)
		return
	}

	_, err = prevClient.NextChanged(context.Background(), &pb.Node{Address: node.addr, Port: uint32(node.port)})
	if err != nil {
		fmt.Println("Error notifying previous node: ", err)
	}

	_, err = nodeClient.PrevChanged(context.Background(), &pb.Node{Address: prev.addr, Port: uint32(prev.port)})
	if err != nil {
		fmt.Println("Error notifying node: ", err)
	}
	_, err = nodeClient.NextChanged(context.Background(), &pb.Node{Address: "", Port: 0})
	if err != nil {
		fmt.Println("Error notifying node: ", err)
	}

	c.emtx.Lock()
	c.tailHost = *node
	c.emtx.Unlock()
}

// run when a replication node is removed
func (c *controllerNode) replNodeRemoved(node *replicationNode) {
	c.cmtx.Lock()
	defer c.cmtx.Unlock()
	prev, next := c.chain.RemoveNode(node)
	if prev == nil && next == nil {
		//there was only one node, now there are none
		return
	}

	c.emtx.Lock()
	var nextClient, prevClient pb.ControllerEventsClient
	var err error

	if prev == nil {
		//there was no previous node
		c.headHost = *next
		//message just next node
		nextClient, err = next.lazyDial()
		if err != nil {
			fmt.Println("Error dialing next node: ", err)
		}
		_, err = nextClient.PrevChanged(context.Background(), &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying next node: ", err)
		}
	} else {
		//there was a previous node
		prevClient, err = prev.lazyDial()
		if err != nil {
			fmt.Println("Error dialing previous node: ", err)
		}
		_, err = prevClient.NextChanged(context.Background(), &pb.Node{Address: next.addr, Port: uint32(next.port)})
		if err != nil {
			fmt.Println("Error notifying previous node: ", err)
		}
	}

	if next == nil {
		//there was no next node
		c.tailHost = *prev
		//message just prev node
		prevClient, err = prev.lazyDial()
		if err != nil {
			fmt.Println("Error dialing previous node: ", err)
		}
		_, err = prevClient.NextChanged(context.Background(), &pb.Node{Address: "", Port: 0})
		if err != nil {
			fmt.Println("Error notifying previous node: ", err)
		}
	} else {
		//there was a next node
		nextClient, err = next.lazyDial()
		if err != nil {
			fmt.Println("Error dialing next node: ", err)
		}
		_, err = nextClient.PrevChanged(context.Background(), &pb.Node{Address: prev.addr, Port: uint32(prev.port)})
		if err != nil {
			fmt.Println("Error notifying next node: ", err)
		}
	}

	c.emtx.Unlock()
}
