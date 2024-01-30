package replicator

import (
	"context"
	"errors"
	"fmt"
	"tkNaloga04/rpc"
)

// GetProvider implementation
func (r *replicatorNode) Get(ctx context.Context, in *rpc.Entry) (*rpc.Entry, error) {
	next := r.next()
	val, exists := r.storage.Load(in.Key)
	if !exists {
		return nil, errors.New(myerr("key not found"))
	}

	vale := val.(entry)

	if next == nil {
		// i am a tail node, get
		fmt.Printf("Got value %s for key %s\n", vale.value, in.Key)
		return &rpc.Entry{Key: in.Key, Value: vale.value}, nil
	}

	// i am not a tail node
	if vale.isDirty() {
		fmt.Printf("Got uncommited value %s for key %s. Waiting for commit...\n", vale.value, in.Key)
		//wait for commit
		subs := r.agent.SubscribeOnce(in.Key)
		for {
			select {
			case read := <-subs:
				{
					if read.commitedVersion == vale.pendingVersion {
						fmt.Printf("Got commited value %s for key %s\n", read.value, in.Key)
						return &rpc.Entry{Key: in.Key, Value: read.value}, nil
					}
					fmt.Printf("Got commited value %s for key %s (version %d), but was waiting for version %d\n", read.value, read.key, read.commitedVersion, vale.pendingVersion)
				}
			case <-ctx.Done():
				return nil, errors.New(myerr("timeout"))
			}
		}
	} else {
		fmt.Printf("Got commited value %s for key %s\n", vale.value, in.Key)
		return &rpc.Entry{Key: in.Key, Value: vale.value}, nil
	}

}

func (r *replicatorNode) mustEmbedUnimplementedReadProviderServer() {
	panic("mustEmbedUnimplementedReadProviderServer")
}
