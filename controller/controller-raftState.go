package main

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"timkr.si/ps-izziv/controller/rpc"
)

// contains the chain layout
type raftState struct {
	mtx   sync.RWMutex
	chain replicationChain
	head  *replicationNode
	tail  *replicationNode
}

type LogType string

const (
	NodeAdd    LogType = "timkr.si/add"
	NodeRemove LogType = "timkr.si/remove"
	HeadChaned LogType = "timkr.si/headChanged"
	TailChaned LogType = "timkr.si/tailChanged"
)

func newRaftState() *raftState {
	return &raftState{chain: *newReplicationChain()}
}

func (r *raftState) Apply(l *raft.Log) interface{} {
	if l.Extensions == nil {
		return nil
	}
	ext := LogType(l.Extensions)
	fmt.Println("Applying log: ", ext)
	r.mtx.Lock()
	defer r.mtx.Unlock()

	data, err := nodeDecode(l.Data)
	if err != nil {
		return err
	}
	var node *replicationNode
	if l.Data != nil {
		var id string = ""
		if data.Id != nil {
			id = *data.Id
		}
		node = newReplicationNode(data.Address, int(data.Port), id)
	}

	switch ext {
	case NodeAdd:
		prev := r.chain.AddNode(node)
		return []*replicationNode{prev, node, nil}
	case NodeRemove:
		prev, next := r.chain.RemoveNode(node, func(n *replicationNode) bool {
			return n.id == node.id
		})
		return []*replicationNode{prev, node, next}
	case HeadChaned:
		r.head = node
		return node
	case TailChaned:
		r.tail = node
		return node
	default:
		fmt.Println("Unknown log type: ", ext)
	}
	return nil
}

func nodeDecode(data []byte) (*rpc.Node, error) {
	node := &rpc.Node{}
	err := proto.Unmarshal(data, node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (r *raftState) Snapshot() (raft.FSMSnapshot, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	//this should copy the chain
	return &stateSnapshot{chain: r.chain.ToSlice(), head: *r.head, tail: *r.tail}, nil
}

func (r *raftState) Restore(rc io.ReadCloser) error {
	//locks not needed in restore, no other goroutines should be running in r
	r.chain = *newReplicationChain()
	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	replicators := strings.Split(string(b), "\n")
	//first two are head and tail
	head, err := deserializeReplicationNode(replicators[0])
	if err != nil {
		return err
	}
	r.head = head
	tail, err := deserializeReplicationNode(replicators[1])
	if err != nil {
		return err
	}
	r.tail = tail

	for _, replicator := range replicators {
		node, err := deserializeReplicationNode(replicator)
		if err != nil {
			return err
		}
		r.chain.AddNode(node)
	}
	return nil
}

type stateSnapshot struct {
	//chain list.List
	chain []replicationNode
	head  replicationNode
	tail  replicationNode
}

func (s *stateSnapshot) Persist(sink raft.SnapshotSink) error {
	//no need to lock, this is a copy
	sink.Write([]byte(s.head.String() + "\n"))
	sink.Write([]byte(s.tail.String() + "\n"))
	for _, v := range s.chain {
		sink.Write([]byte(v.String() + "\n"))
	}
	sink.Close()
	return nil
}

func (s *stateSnapshot) Release() {}
