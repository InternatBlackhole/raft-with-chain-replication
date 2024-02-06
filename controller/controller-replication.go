package main

import (
	"net"
	"strconv"
	"strings"
	"sync"

	"timkr.si/ps-izziv/controller/rpc"

	"google.golang.org/grpc"
)

type replicationNode struct {
	addr string
	port int
	conn grpc.ClientConnInterface
	id   string
}

func deserializeReplicationNode(replicator string) (*replicationNode, error) {
	split := strings.Split(replicator, "|")
	host, port, err := net.SplitHostPort(split[0])
	if err != nil {
		return nil, err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	return &replicationNode{addr: host, port: p, id: split[1]}, nil
}

func newReplicationNode(addr string, port int, id string) *replicationNode {
	return &replicationNode{addr: addr, port: port, id: id}
}

func (r *replicationNode) lazyDial() (rpc.ControllerEventsClient, error) {
	if r.conn == nil {
		conn, err := grpc.Dial(r.addr+":"+strconv.Itoa(r.port), grpcDialOptions(true)...)
		if err != nil {
			return nil, err
		}
		r.conn = conn
	}
	return rpc.NewControllerEventsClient(r.conn), nil
}

func (r *replicationNode) String() string {
	if r == nil {
		return ""
	}
	return r.addr + ":" + strconv.Itoa(r.port) + "|" + r.id
}

func (r *replicationNode) ToNode() *rpc.Node {
	if r == nil {
		return &rpc.Node{Address: "", Port: 0, Id: nil}
	}
	return &rpc.Node{Address: r.addr, Port: uint32(r.port), Id: &r.id}
}

func (r *replicationNode) Copy() *replicationNode {
	return &replicationNode{addr: r.addr, port: r.port, id: r.id}
}

type replicationChain struct {
	sync.RWMutex
	list
}

func newReplicationChain() *replicationChain {
	return &replicationChain{list: *newList()}
}

// AddNode adds a node to the chain and returns the previous last node, nil if there was none
func (r *replicationChain) AddNode(node *replicationNode) *replicationNode {
	r.Lock()
	defer r.Unlock()

	r.PushBack(node)
	if r.Len() <= 1 {
		return nil
	}
	back := r.Back().Prev()
	val := back.Value
	return val
}

// RemoveNode removes a node from the chain and returns the neighbouring nodes (prev, next), nil if there isn't one
func (r *replicationChain) RemoveNode(node *replicationNode, fun func(*replicationNode) bool) (*replicationNode, *replicationNode) {
	r.Lock()
	defer r.Unlock()

	if r.Len() <= 1 {
		return nil, nil
	}

	var prev, next *replicationNode
	for val := r.Front(); val != nil; val = val.Next() {
		if val.Value == nil {
			continue
		}
		if fun(val.Value) {
			if val.Prev() != nil {
				prev = val.Prev().Value
			}

			if val.Next() != nil {
				next = val.Next().Value
			}

			r.Remove(val)
			break
		}
	}
	return prev, next
}

func (r *replicationChain) ContainsId(id string) bool {
	r.RLock()
	defer r.RUnlock()

	for val := r.Front(); val != nil; val = val.Next() {
		cast := val.Value
		if cast.id == id {
			return true
		}
	}
	return false
}

func (r *replicationChain) GetNodeById(id string) *replicationNode {
	r.RLock()
	defer r.RUnlock()

	for val := r.Front(); val != nil; val = val.Next() {
		cast := val.Value
		if cast.id == id {
			return cast
		}
	}
	return nil
}

func (r *replicationChain) ToSlice() []replicationNode {
	r.RLock()
	defer r.RUnlock()

	arr := make([]replicationNode, r.Len())
	for val, i := r.Front(), 0; val != nil; val, i = val.Next(), i+1 {
		arr[i] = *val.Value.Copy()
	}
	return arr
}
