package controller

// Inspiration from https://github.com/Jille/raft-grpc-example/

import (
	"context"
	"flag"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"
	"tkNaloga04/rpc"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	myAddr = flag.String("addr", "localhost:40000", "Address of this node")
	raftId = flag.String("raftId", "localhost:", "Node id")
	//raftDir       = flag.String("raftDataDir", "", "raft dir")
	raftBootstrap    = flag.Bool("rb", false, "Bootstrap the raft cluster?")
	state            = raftState{}
	leadershipEvents = make(chan raft.Observation, 3)
	observer         *raft.Observer
)

var this *controllerNode //this node

func main() {
	flag.Parse()

	if *raftId == "" {
		panic("raftId not set")
	}

	_, port, err := net.SplitHostPort(*myAddr)
	if err != nil {
		panic(err)
	}

	rpcSock, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on all interfaces on port: ", port)

	hbSock, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		panic(err)
	}
	fmt.Println("Heartbeat port: ", hbSock.LocalAddr().(*net.UDPAddr).Port)

	s := grpc.NewServer()

	rft, tm, err := createCluster(context.Background(), *raftId, *myAddr, &state)
	if err != nil {
		panic(err)
	}

	observer = raft.NewObserver(leadershipEvents, false, func(e *raft.Observation) bool {
		return reflect.TypeOf(*e) == reflect.TypeOf(raft.LeaderObservation{})
	})

	this = newControllerNode(hbSock.LocalAddr().(*net.UDPAddr).Port, rft, &state)

	rpc.RegisterControllerServer(s, this)
	tm.Register(s)
	raftadmin.Register(s, rft)

	go myLeadershipObserver(rft, hbSock)
	go leadershipObserver(leadershipEvents)

	err = s.Serve(rpcSock)
	if err != nil {
		panic(err)
	}
}

// createCluster creates a new raft cluster with the given id and address.
func createCluster(ctx context.Context, id, address string, state raft.FSM) (*raft.Raft, *transport.Manager, error) {
	d := raft.DefaultConfig()
	d.LocalID = raft.ServerID(id)

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()
	trans := transport.New(raft.ServerAddress(address), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})

	r, err := raft.NewRaft(d, state, logStore, stableStore, snapshotStore, trans.Transport())
	if err != nil {
		return nil, nil, err
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       d.LocalID,
					Address:  raft.ServerAddress(address),
				},
			},
		}
		fu := r.BootstrapCluster(cfg)
		if err = fu.Error(); err != nil {
			return nil, nil, fmt.Errorf("bootstrap error: %w", err)
		}
	}
	return r, trans, nil
}

func deathReporter(node string) {
	r := this.raft
	if r.State() != raft.Leader {
		fmt.Println("Not leader, not reporting death, why am I even here?")
		return
	}
	fu := r.ApplyLog(raft.Log{Data: []byte(node), Extensions: []byte(NodeRemove)}, 1*time.Second)
	if err := fu.Error(); err != nil {
		fmt.Println("Error applying log: ", err)
	}
}

// valid nodes is a list of node hostnames that are allowed to send heartbeats
func beatRecvController(quit *bool, hbSock *net.UDPConn, deathReport func(string)) {
	// nodes is a map of node hostnames to last heartbeat time
	nodes := make(map[string]time.Time)
	buf := make([]byte, 1024)

	validLock := &this.cmtx
	validNodes := &this.chain

	for !*quit {
		hbSock.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		_, addr, err := hbSock.ReadFromUDP(buf)
		if err != nil && err.Error() != "i/o timeout" {
			fmt.Println("Error reading from UDP: ", err)
			continue
		}

		validLock.RLock()
		if validNodes.ContainsHostname(addr.String()) {
			validLock.RUnlock()
			if _, ok := nodes[addr.String()]; !ok {
				fmt.Println("Received first heartbeat from: ", addr.String())
			} else {
				fmt.Println("Received heartbeat from: ", addr.String())
			}
			nodes[addr.String()] = time.Now()
		} else {
			validLock.RUnlock()
			fmt.Println("Received heartbeat from invalid node: ", addr.String())
			continue
		}

		for k, v := range nodes {
			if time.Since(v) > 100*time.Millisecond {
				fmt.Println("Node ", k, " is dead, removning from list")
				delete(nodes, k)
				go deathReport(k)
			}
		}

		fmt.Println("Received heartbeat from: ", addr)
	}
}

func myLeadershipObserver(rft *raft.Raft, sock *net.UDPConn) {
	quit := false
	for {
		obs := <-rft.LeaderCh()
		if obs {
			if !quit {
				continue
			}
			//start heartbeat receiver
			fmt.Println("I am the leader")
			quit = false
			go beatRecvController(&quit, sock, deathReporter)
		} else {
			//stop heartbeat receiver
			fmt.Println("I am not the leader")
			quit = true
		}
	}
}

func leadershipObserver(c <-chan raft.Observation) {
	for {
		obs := <-c
		if this.raft.State() != raft.Leader {
			//do nothing if not leader
			continue
		}
		data := obs.Data.(raft.LeaderObservation)
		host, p, err := net.SplitHostPort(string(data.LeaderAddr))
		if err != nil {
			fmt.Println("Error splitting host and port: ", err)
			continue
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			fmt.Println("Error converting port to int: ", err)
			continue
		}
		this.cmtx.RLock()
		for val := this.chain.Front(); val != nil; val = val.Next() {
			node := val.Value.(*replicationNode)
			conn, err := node.lazyDial()
			if err != nil {
				fmt.Println("Error connecting to node: ", err)
				continue
			}
			_, err = conn.LeaderChanged(context.Background(), &rpc.Node{Address: host, Port: uint32(port)})
			if err != nil {
				fmt.Println("Error calling LeaderChanged: ", err)
				continue
			}
		}
		this.cmtx.RUnlock()
	}
}
