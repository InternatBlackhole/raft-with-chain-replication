package controller

import (
	"container/list"
	"net"
	"strconv"
	"tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type replicationNode struct {
	addr string
	port int
	conn rpc.ControllerEventsClient
}

func (r *replicationNode) serialize() []byte {
	return []byte(r.addr + ":" + strconv.Itoa(r.port) + "\n")
}

func deserializeReplicationNode(replicator string) (*replicationNode, error) {
	host, port, err := net.SplitHostPort(replicator)
	if err != nil {
		return nil, err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	return &replicationNode{addr: host, port: p}, nil
}

func newReplicationNode(addr string, port int) *replicationNode {
	return &replicationNode{addr: addr, port: port}
}

func (r *replicationNode) lazyDial() (rpc.ControllerEventsClient, error) {
	if r.conn == nil {
		events, err := grpc.Dial(r.addr+":"+strconv.Itoa(r.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		//i hope this copies correctly
		r.conn = rpc.NewControllerEventsClient(events)
	}
	return r.conn, nil
}

func (r *replicationNode) String() string {
	return r.addr + ":" + strconv.Itoa(r.port)
}

func (r *replicationNode) ToNode() *rpc.Node {
	return &rpc.Node{Address: r.addr, Port: uint32(r.port)}
}

type replicationChain struct {
	list.List
}

// AddNode adds a node to the chain and returns the previous last node, nil if there was none
func (r *replicationChain) AddNode(node *replicationNode) *replicationNode {
	r.PushBack(node)
	if r.Len() <= 1 {
		return nil
	}
	return r.Back().Prev().Value.(*replicationNode)
}

// RemoveNode removes a node from the chain and returns the neighbouring nodes (prev, next), nil if there isn't one
func (r *replicationChain) RemoveNode(node *replicationNode) (*replicationNode, *replicationNode) {
	if r.Len() <= 1 {
		return nil, nil
	}

	var prev, next *replicationNode
	for val := r.Front(); val != nil; val = val.Next() {
		if val.Value.(*replicationNode) == node {
			prev = val.Prev().Value.(*replicationNode)
			next = val.Next().Value.(*replicationNode)
			r.Remove(val)
			break
		}
	}
	return prev, next
}

func (r *replicationChain) ContainsHostname(addr string) bool {
	for val := r.Front(); val != nil; val = val.Next() {
		cast := val.Value.(*replicationNode)
		if net.JoinHostPort(cast.addr, strconv.Itoa(cast.port)) == addr {
			return true
		}
	}
	return false
}
