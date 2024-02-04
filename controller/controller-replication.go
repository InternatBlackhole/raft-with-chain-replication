package main

import (
	"container/list"
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
	conn rpc.ControllerEventsClient
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
		events, err := grpc.Dial(r.addr+":"+strconv.Itoa(r.port), grpcDialOptions(true)...)
		if err != nil {
			return nil, err
		}
		//i hope this copies correctly
		r.conn = rpc.NewControllerEventsClient(events)
	}
	return r.conn, nil
}

func (r *replicationNode) String() string {
	return r.addr + ":" + strconv.Itoa(r.port) + "|" + r.id
}

func (r *replicationNode) ToNode() *rpc.Node {
	if r == nil {
		return &rpc.Node{}
	}
	return &rpc.Node{Address: r.addr, Port: uint32(r.port), Id: &r.id}
}

func (r *replicationNode) Copy() *replicationNode {
	return &replicationNode{addr: r.addr, port: r.port, id: r.id}
}

type replicationChain struct {
	sync.RWMutex
	list.List
	//idCache map[string]*replicationNode
}

func newReplicationChain() *replicationChain {
	return &replicationChain{List: *list.New() /*, idCache: make(map[string]*replicationNode)*/}
}

// AddNode adds a node to the chain and returns the previous last node, nil if there was none
func (r *replicationChain) AddNode(node *replicationNode) *replicationNode {
	r.Lock()
	defer r.Unlock()
	//r.idCache[node.id] = node

	r.PushBack(nil)
	r.Back().Value = node
	//r.PushBack(node)
	if r.Len() <= 1 {
		return nil
	}
	//r.PrintChain()
	back := r.Back()
	val, ok := back.Value.(*replicationNode)
	if !ok {
		return nil
	}
	return val
}

// RemoveNode removes a node from the chain and returns the neighbouring nodes (prev, next), nil if there isn't one
func (r *replicationChain) RemoveNode(node *replicationNode) (*replicationNode, *replicationNode) {
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
		if val.Value.(*replicationNode) == node {
			prev = val.Prev().Value.(*replicationNode)
			next = val.Next().Value.(*replicationNode)
			r.Remove(val)
			break
		}
	}
	//delete(r.idCache, node.id)
	return prev, next
}

func (r *replicationChain) ContainsId(id string) bool {
	//could use the cache, but this is critical
	//_, ok := r.idCache[id]
	//return ok
	r.RLock()
	defer r.RUnlock()

	for val := r.Front(); val != nil; val = val.Next() {
		cast := val.Value.(*replicationNode)
		if cast.id == id {
			return true
		}
	}
	return false
}

func (r *replicationChain) GetNodeById(id string) *replicationNode {
	/*nod, ok := r.idCache[id]
	if ok {
		return nod
	}*/
	r.RLock()
	defer r.RUnlock()

	for val := r.Front(); val != nil; val = val.Next() {
		cast := val.Value.(*replicationNode)
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
		arr[i] = *val.Value.(*replicationNode).Copy()
	}
	return arr
}
