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
// TODO: change the state so that it holds alo the id
type raftState struct {
	mtx sync.RWMutex
	//chain list.List //list of replicationNodes
	//should it be a pointer?
	chain replicationChain

	//nodeAdded   func(replicationNode)
	//nodeRemoved func(replicationNode)
}

type LogType string

const (
	NodeAdd    LogType = "timkr.si/add"
	NodeRemove LogType = "timkr.si/remove"
)

func newRaftState() *raftState {
	return &raftState{chain: *newReplicationChain()}
}

func (r *raftState) Apply(l *raft.Log) interface{} {
	ext := LogType(l.Extensions)

	var data *rpc.Node
	if ext == NodeAdd || ext == NodeRemove {
		data = &rpc.Node{}
		err := proto.Unmarshal(l.Data, data)
		if err != nil {
			fmt.Println("Error unmarshalling data: ", err)
			return err
		}
	} else {
		fmt.Println("Unknown log type: ", ext)
		return nil
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()
	node := newReplicationNode(data.Address, int(data.Port), *data.Id)

	switch ext {
	case NodeAdd:
		prev := r.chain.AddNode(node)
		return []*replicationNode{prev, node, nil}
		//go r.nodeAdded(data)
		//fmt.Println("Added node: ", data)
	case NodeRemove:
		/*for val := r.chain.Front(); val != nil; val = val.Next() {
			if val.Value.(string) == data {
				r.chain.Remove(val)
				go r.nodeRemoved(data)
				fmt.Println("Removed node: ", data)
				break
			}
		}*/
		prev, next := r.chain.RemoveNode(node)
		return []*replicationNode{prev, node, next}
	}
	return nil
}

func (r *raftState) Snapshot() (raft.FSMSnapshot, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	//this should copy the chain
	return &stateSnapshot{chain: r.chain.ToSlice()}, nil
}

func (r *raftState) Restore(rc io.ReadCloser) error {
	//locks not needed in restore, no other goroutines should be running in r
	//r.mtx.Lock()
	//defer r.mtx.Unlock()
	r.chain = *newReplicationChain()
	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	replicators := strings.Split(string(b), "\n")
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
}

func (s *stateSnapshot) Persist(sink raft.SnapshotSink) error {
	//no need to lock, this is a copy
	for _, v := range s.chain {
		sink.Write([]byte(v.String() + "\n"))
	}
	sink.Close()
	return nil
}

func (s *stateSnapshot) Release() {}
