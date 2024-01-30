package replicator

import (
	"context"
	"tkNaloga04/rpc"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (r *replicatorNode) Put(ctx context.Context, in *rpc.Entry) (*emptypb.Empty, error) {
	val, ok := r.storage.LoadOrStore(in.Key, entry{value: in.Value, commitedVersion: 0, pendingVersion: 1})
	if ok {
		//value exists
		val := val.(entry)
		val.pendingVersion++
		r.storage.Store(in.Key, val)
	}
	return &emptypb.Empty{}, nil
}

func (r *replicatorNode) mustEmbedUnimplementedPutProviderServer() {
	panic("mustEmbedUnimplementedPutProviderServer")
}
