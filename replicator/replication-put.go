package main

import (
	"context"
	"errors"

	"timkr.si/ps-izziv/replicator/rpc"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (r *replicatorNode) Put(ctx context.Context, in *rpc.Entry) (*emptypb.Empty, error) {
	prev := r.prev()
	if prev != nil {
		// i am not a head node
		return &emptypb.Empty{}, errors.New(myerr("not a head node"))
	}

	//val, ok := r.storage.LoadOrStore(in.Key, entry{value: in.Value, commitedVersion: 0, pendingVersion: 1})
	val, ok := r.storage.Load(in.Key)
	var pendingVersion uint32 = 1
	if ok {
		//value exists
		val := val.(entry)
		//val.pendingVersion++
		pendingVersion = val.pendingVersion + 1
		//r.storage.Store(in.Key, val)
	}
	//start commit process
	r.PutInternal(ctx, &rpc.InternalEntry{Key: in.Key, Value: in.Value, Version: pendingVersion})
	return &emptypb.Empty{}, nil
}

func (r *replicatorNode) mustEmbedUnimplementedPutProviderServer() {
	panic("mustEmbedUnimplementedPutProviderServer")
}
