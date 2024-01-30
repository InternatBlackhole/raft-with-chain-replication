package replicator

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"tkNaloga04/rpc"

	"sync"

	"errors"

	"google.golang.org/protobuf/types/known/emptypb"
)

type NextReplicator func() rpc.ReplicationProviderClient

/*type NodeType int

const (
	CHAIN_HEAD   NodeType = 1 << iota
	CHAIN_MIDDLE NodeType = 1 << iota
	CHAIN_TAIL   NodeType = 1 << iota
)*/

type replicatorNode struct {
	storage sync.Map // perhaps make it a sync.Map
	prev    NextReplicator
	next    NextReplicator
	//nType   NodeType
	agent *Agent

	rpc.UnimplementedReplicationProviderServer
	rpc.UnimplementedReadProviderServer
	rpc.UnimplementedPutProviderServer
}

func myerr(err string) string {
	return ("Error: " + err + "from node " + strconv.Itoa(os.Getpid()))
}

func NewReplicatorNode(prev NextReplicator, next NextReplicator) *replicatorNode {
	return &replicatorNode{prev: prev, next: next, agent: NewAgent()}
}

func (r *replicatorNode) PutInternal(ctx context.Context, in *rpc.InternalEntry) (*emptypb.Empty, error) {
	var err error = nil
	next := r.next()
	val, existed := r.storage.LoadOrStore(in.Key, newEntry(in))
	if next == nil {
		// i am a tail node
		//r.storage.Store(in.Key, newEntry(in))
		//start commit process
		prev := r.prev()
		//check if i am the only node
		if prev != nil {
			_, err = prev.Commit(context.Background(), &rpc.EntryCommited{Key: in.Key, Version: in.Version})
			//panic("prev is nil, figure it out, are you running on one node?")
		}
		fmt.Printf("Stored commited value %s for key %s, version %s\n", in.Value, in.Key, in.Version)
	} else {
		// i am not a tail node, send Put to next
		// and also save uncommited value
		//val, ok := r.storage.Load(in.Key)
		if existed {
			//does this create a copy?
			val := val.(entry)
			val.pendingVersion = in.Version
			r.storage.Store(in.Key, val)
		}
		// already stored in LoadOrStore
		/*else {
			//err = errors.New("no key " + in.Key + " found")
			r.storage.Store(in.Key, newEntry(in))
		}*/
		_, err = next.PutInternal(context.Background(), in)
		fmt.Printf("Stored uncommited value %s for key %s\n", in.Value, in.Key)
	}
	if err != nil {
		panic(err)
	}
	return &emptypb.Empty{}, nil
}

// ReplicationProvider implementation, provides updates to channel
func (r *replicatorNode) Commit(ctx context.Context, in *rpc.EntryCommited) (*emptypb.Empty, error) {
	//Commit gets called from tail to prev all the way to head
	val, loaded := r.storage.Load(in.Key)
	if !loaded {
		//this should never happen
		//return &emptypb.Empty{}, errors.New(myerr("key not found"))
		panic(errors.New("key not found, should not happen in Commit"))
	}
	v := val.(entry)
	v.commitedVersion = in.Version
	r.storage.Store(in.Key, v)
	fmt.Printf("Commited value %s for key %s\n", v.value, in.Key)

	go r.agent.Publish(in.Key, entry{value: v.value, commitedVersion: in.Version, pendingVersion: v.pendingVersion, key: in.Key})

	var err error = nil
	prev := r.prev()
	if prev != nil {
		//i am not a head node
		_, err = prev.Commit(context.Background(), in)
	}
	//i am a head node, do nothing
	return &emptypb.Empty{}, err
}

func (r *replicatorNode) mustEmbedUnimplementedReplicatorServer() {
	panic("mustEmbedUnimplementedReplicatorServer")
}
