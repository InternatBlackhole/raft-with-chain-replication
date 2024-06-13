##Description

This project was created as a part of a subject on distributed systems. 

It is composed of three programs:
- replication controller which uses the Raft consensus algorithm to maintain state and control
the nodes of a replication chain.
- replicator that uses chain replication of a key-value store
- client to interact with the chain


To run all these programs on local machine run `source utils.sh` in shell to import utility functions for running the cluster.

Every program has a `-h` option to get help for using that program

##Building

This project uses Go, so have the Golang compiler installed.

To build run `make`
