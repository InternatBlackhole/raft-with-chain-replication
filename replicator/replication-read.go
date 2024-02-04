package main

import (
	"context"
	"errors"
	"fmt"

	"timkr.si/ps-izziv/replicator/rpc"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// GetProvider implementation
func (r *replicatorNode) Get(ctx context.Context, key *wrapperspb.StringValue) (*rpc.Entry, error) {
	next := r.next()
	val, exists := r.storage.Load(key.Value)
	if !exists {
		return nil, errors.New(myerr("key not found"))
	}

	vale := val.(entry)

	if next == nil {
		// i am a tail node, get
		fmt.Printf("Got value %s for key %s\n", vale.value, key.Value)
		return &rpc.Entry{Key: key.Value, Value: vale.value}, nil
	}

	// i am not a tail node
	if vale.isDirty() {
		fmt.Printf("Got uncommited value %s for key %s. Waiting for commit...\n", vale.value, key.Value)
		//wait for commit
		subs := r.publisher.SubscribeOnce(key.Value)
		for {
			select {
			case read := <-subs:
				{
					//when pendingVersion == commitedVersion, we have a commited value
					if read.commitedVersion == vale.pendingVersion {
						fmt.Printf("Got commited value %s for key %s\n", read.value, key.Value)
						return &rpc.Entry{Key: key.Value, Value: read.value}, nil
					}
					fmt.Printf("Got commited value %s for key %s (version %d), but was waiting for version %d\n", read.value, read.key, read.commitedVersion, vale.pendingVersion)
				}
			case <-ctx.Done():
				return nil, errors.New(myerr("timeout"))
			}
		}
	} else {
		fmt.Printf("Got commited value %s for key %s\n", vale.value, key.Value)
		return &rpc.Entry{Key: key.Value, Value: vale.value}, nil
	}

}

func (r *replicatorNode) mustEmbedUnimplementedReadProviderServer() {
	panic("mustEmbedUnimplementedReadProviderServer")
}
