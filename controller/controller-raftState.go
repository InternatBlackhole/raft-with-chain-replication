package controller

import (
	"container/list"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// contains the chain layout
type raftState struct {
	mtx   sync.RWMutex
	chain list.List //list of "hostname:port"

	nodeAdded   func(string) //accepts hostname:port
	nodeRemoved func(string) //accepts hostname:port
}

type LogType string

const (
	NodeAdd    LogType = "add"
	NodeRemove LogType = "remove"
)

func (r *raftState) Apply(l *raft.Log) interface{} {
	ext := LogType(l.Extensions)
	data := string(l.Data)

	r.mtx.Lock()
	defer r.mtx.Unlock()

	switch ext {
	case NodeAdd:
		r.chain.PushBack(data)
		go r.nodeAdded(data)
	case NodeRemove:
		for val := r.chain.Front(); val != nil; val = val.Next() {
			if val.Value.(string) == data {
				r.chain.Remove(val)
				go r.nodeRemoved(data)
				break
			}
		}
	}
	return nil
}

func (r *raftState) Snapshot() (raft.FSMSnapshot, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	//this should copy the chain
	return &stateSnapshot{chain: r.chain}, nil
}

func (r *raftState) Restore(rc io.ReadCloser) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	b, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	replicators := strings.Split(string(b), "\n")
	for _, replicator := range replicators {
		/*node, err := deserializeReplicationNode(replicator)
		if err != nil {
			return err
		}*/
		r.chain.PushBack(replicator)
	}
	return nil
}

type stateSnapshot struct {
	chain list.List
}

func (s *stateSnapshot) Persist(sink raft.SnapshotSink) error {
	//no need to lock, this is a copy
	for val := s.chain.Front(); val != nil; val = val.Next() {
		sink.Write([]byte(val.Value.(string)))
	}
	sink.Close()
	return nil
}

func (s *stateSnapshot) Release() {}
