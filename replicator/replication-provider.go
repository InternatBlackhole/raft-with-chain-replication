package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"timkr.si/ps-izziv/replicator/rpc"

	"sync"

	"errors"

	"google.golang.org/protobuf/types/known/emptypb"
)

type NextReplicator func() rpc.ReplicationProviderClient

type replicatorNode struct {
	storage   *sync.Map // perhaps make it a sync.Map
	prev      NextReplicator
	next      NextReplicator
	publisher *Agent

	rpc.UnimplementedReplicationProviderServer
	rpc.UnimplementedReadProviderServer
	rpc.UnimplementedPutProviderServer
}

func myerr(err string) string {
	return ("Error: " + err + " from node " + strconv.Itoa(myPort))
}

func NewReplicatorNode(prev NextReplicator, next NextReplicator, storage *sync.Map) *replicatorNode {
	return &replicatorNode{prev: prev, next: next, publisher: NewAgent(), storage: storage}
}

func (r *replicatorNode) PutInternal(ctx context.Context, in *rpc.InternalEntry) (*emptypb.Empty, error) {
	var err error = nil
	next := r.next()
	val, existed := r.storage.LoadOrStore(in.Key, newEntry(in))
	fmt.Printf("Stored uncommited value %s for key %s\n", in.Value, in.Key)
	if next == nil {
		// i am a tail node
		r.storage.Store(in.Key, newEntry(in))
		fmt.Printf("Stored commited value %s for key %s, version %d\n", in.Value, in.Key, in.Version)
		//start commit process
		prev := r.prev()
		//check if i am the only node
		if prev != nil {
			ctx, cancel := ctxTimeout()
			_, err = prev.Commit(ctx, &rpc.EntryCommited{Key: in.Key, Version: in.Version})
			if err != nil {
				fmt.Println("Error in commit: ", err)
			}
			cancel()
		}
	} else {
		// i am not a tail node, send Put to next
		// and also save uncommited value
		if existed {
			//does this create a copy?
			val := val.(entry)
			val.pendingVersion = in.Version
			val.value = in.Value
			r.storage.Store(in.Key, val)
		}
		// already stored in LoadOrStore
		ctx, cancel := ctxTimeout()
		_, err = next.PutInternal(ctx, in)
		if err != nil {
			fmt.Println("Error in PutInternal: ", err)
		}
		cancel()
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
		panic(errors.New("key not found, should not happen in Commit"))
	}
	//TODO: remove this artifical delay
	time.Sleep(1000 * time.Millisecond)
	v := val.(entry)
	v.commitedVersion = in.Version
	r.storage.Store(in.Key, v)
	fmt.Printf("Commited value %s for key %s, version %d\n", v.value, in.Key, in.Version)

	go r.publisher.Publish(in.Key, entry{value: v.value, commitedVersion: in.Version, pendingVersion: v.pendingVersion, key: in.Key})

	var err error = nil
	prev := r.prev()
	if prev != nil {
		//i am not a head node
		ctx, cancel := ctxTimeout()
		_, err = prev.Commit(ctx, in)
		if err != nil {
			fmt.Println("Error in Commit: ", err)
		}
		cancel()
	}
	//i am a head node, do nothing
	return &emptypb.Empty{}, err
}

func (r *replicatorNode) StreamEntries(stream rpc.ReplicationProvider_StreamEntriesServer) error {
	for {
		ent, err := stream.Recv()
		if err == io.EOF {
			//no more entries
			fmt.Println("Storage synced")
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if ent == nil {
			fmt.Println("Received nil entry")
			return nil
		}
		e := newEntry(ent)
		e.commitedVersion = ent.Version
		r.storage.Store(ent.Key, e)
	}
}

func (r *replicatorNode) mustEmbedUnimplementedReplicatorServer() {
	panic("mustEmbedUnimplementedReplicatorServer")
}
