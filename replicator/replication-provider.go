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

type NodeType int

const (
	CHAIN_HEAD   NodeType = iota
	CHAIN_MIDDLE NodeType = iota
	CHAIN_TAIL   NodeType = iota
)

type ReplicatorNode struct {
	storage sync.Map // perhaps make it a sync.Map
	prev    NextReplicator
	next    NextReplicator
	updates chan entry
	nType   NodeType

	rpc.UnimplementedReplicationProviderServer
	rpc.UnimplementedReadProviderServer
	rpc.UnimplementedPutProviderServer
}

func myerr(err string) string {
	return ("Error: " + err + "from node " + strconv.Itoa(os.Getpid()))
}

func (r *ReplicatorNode) PutInternal(ctx context.Context, in *rpc.InternalEntry) (*emptypb.Empty, error) {
	var err error = nil
	next := r.next()
	if next == nil {
		// i am a tail node, commit
		r.storage.Store(in.Key, entry{value: in.Value, commitedVersion: in.Version, pendingVersion: in.Version})
		//start commit process
		prev := r.prev()
		if prev != nil {
			_, err = prev.Commit(context.Background(), &rpc.EntryCommited{Key: in.Key, Version: in.Version})
			//panic("prev is nil, figure it out, are you running on one node?")
		}
		fmt.Printf("Stored commited value %s for key %s, version %s\n", in.Value, in.Key, in.Version)
	} else {
		// i am not a tail node, send Put to next
		// and also save uncommited value
		val, ok := r.storage.Load(in.Key)
		if ok {
			val := val.(entry)
			val.pendingVersion = in.Version
			r.storage.Store(in.Key, val)
		} else {
			//err = errors.New("no key " + in.Key + " found")
			r.storage.Store(in.Key, entry{value: in.Value, commitedVersion: in.Version, pendingVersion: in.Version})
		}
		_, err = next.PutInternal(context.Background(), in)
		fmt.Printf("Stored uncommited value %s for key %s\n", in.Value, in.Key)
	}
	if err != nil {
		panic(err)
	}
	return &emptypb.Empty{}, nil
}

func (r *ReplicatorNode) Commit(ctx context.Context, in *rpc.EntryCommited) (*emptypb.Empty, error) {
	//Commit gets called from tail to prev all the way to head
	val, loaded := r.storage.Load(in.Key)
	if !loaded {
		return &emptypb.Empty{}, errors.New(myerr("key not found"))
	}
	v := val.(entry)
	v.commitedVersion = in.Version
	r.storage.Store(in.Key, v)
	fmt.Printf("Commited value %s for key %s\n", v.value, in.Key)
	var err error = nil
	prev := r.prev()
	if prev != nil {
		//i am not a head node
		_, err = prev.Commit(context.Background(), in)
	}
	//i am a head node, do nothing
	return &emptypb.Empty{}, err
}

func (r *ReplicatorNode) Get(ctx context.Context, in *rpc.Entry) (*rpc.Entry, error) {
	next := r.next()
	val, loaded := r.storage.Load(in.Key)
	if !loaded {
		return nil, errors.New(myerr("key not found"))
	}

	if next == nil {
		// i am a tail node, get
		fmt.Printf("Got value %s for key %s\n", val.(entry).value, in.Key)
		return &rpc.Entry{Key: in.Key, Value: val.(entry).value}, nil
	} else {
		// i am not a tail node
		if val.(entry).isDirty() {
			fmt.Printf("Got uncommited value %s for key %s. Waiting for commit...\n", val.(entry).value, in.Key)
			//wait for commit
			for {
				select {
				case read := <-r.updates:
					{
						if read.key == in.Key {
							fmt.Printf("Got commited value %s for key %s\n", read.value, in.Key)
							return &rpc.Entry{Key: in.Key, Value: read.value}, nil
						}
						fmt.Printf("Got commited value %s for key %s, but was waiting for %s\n", read.value, read.key, in.Key)
					}
				case <-ctx.Done():
					return nil, errors.New(myerr("timeout"))
				}
			}
		} else {
			fmt.Printf("Got commited value %s for key %s\n", val.(entry).value, in.Key)
			return &rpc.Entry{Key: in.Key, Value: val.(entry).value}, nil
		}
	}
}

func (r *ReplicatorNode) Put(ctx context.Context, in *rpc.Entry) (*emptypb.Empty, error) {
	val, ok := r.storage.Load(in.Key)
	if ok {
		//value exists
		val := val.(entry)
		val.pendingVersion++
		r.storage.Store(in.Key, val)
	} else {
		r.storage.Store(in.Key, entry{value: in.Value, commitedVersion: 0, pendingVersion: 1})
	}
	return &emptypb.Empty{}, nil
}

func (r *ReplicatorNode) mustEmbedUnimplementedReplicatorServer() {
	panic("mustEmbedUnimplementedReplicatorServer")
}

func (r *ReplicatorNode) mustEmbedUnimplementedReadProviderServer() {
	panic("mustEmbedUnimplementedReadProviderServer")
}

func (r *ReplicatorNode) mustEmbedUnimplementedPutProviderServer() {
	panic("mustEmbedUnimplementedPutProviderServer")
}

func (r *ReplicatorNode) putInternal_asTail(in *rpc.InternalEntry) {
	r.PutInternal(context.Background(), in)
}

func (r *ReplicatorNode) putInternal_asMiddleOrHead(in *rpc.Entry) {
	r.Put(context.Background(), in)
}
