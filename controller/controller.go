package main

// Inspiration from https://github.com/Jille/raft-grpc-example/

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"timkr.si/ps-izziv/controller/rpc"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	myAddr  string
	raftId  string
	raftDir string
	//could also use the AddVoter method
	raftBootstrap bool
	raftJoin      []string
	state         raftState
	clusterEvents = make(chan raft.Observation, 3)
	observer      *raft.Observer
	hbSock        *net.UDPConn
	mport         string
)

func init() {
	flag.StringVar(&myAddr, "addr", "localhost:40000", "Address of this node")
	flag.StringVar(&raftId, "raftId", "localhost:", "Node id")
	flag.StringVar(&raftDir, "raftDataDir", "", "raft data directory")
	flag.BoolVar(&raftBootstrap, "rb", false, "Bootstrap the raft cluster?")
}

var this *controllerNode //this node

func main() {
	flag.Parse()

	if raftId == "" {
		panic("raftId not set")
	}
	raftJoin = flag.Args()
	if !raftBootstrap && len(raftJoin) == 0 {
		panic("enter address of nodes in cluster")
	}

	if raftDir == "" {
		panic("raftDataDir not set")
	}

	var err error
	_, mport, err = net.SplitHostPort(myAddr)
	if err != nil {
		panic(err)
	}

	rpcSock, err := net.Listen("tcp", ":"+mport)
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on all interfaces on port: ", mport)

	hbSock, err = net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		panic(err)
	}
	fmt.Println("Heartbeat port: ", hbSock.LocalAddr().(*net.UDPAddr).Port)

	s := grpc.NewServer()

	state = *newRaftState()

	rft, tm, err := createCluster(context.Background(), raftId, myAddr, &state)
	if err != nil {
		panic(err)
	}

	observer = raft.NewObserver(clusterEvents, false, func(e *raft.Observation) bool {
		_, ok1 := e.Data.(raft.LeaderObservation)
		_, ok2 := e.Data.(raft.PeerObservation)
		return ok1 || ok2
	})
	rft.RegisterObserver(observer)
	go observerFunc()

	this = newControllerNode(hbSock.LocalAddr().(*net.UDPAddr).Port, rft, &state)

	rpc.RegisterControllerServer(s, this)
	tm.Register(s)
	raftadmin.Register(s, rft)
	fmt.Println("Registered servers")

	go myLeadershipObserver(rft, hbSock)

	err = s.Serve(rpcSock)
	if err != nil {
		panic(err)
	}
}

func grpcDialOptions(nobuffer bool) []grpc.DialOption {
	arr := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if nobuffer {
		arr = append(arr, grpc.WithWriteBufferSize(0), grpc.WithReadBufferSize(1024))
	}
	return arr
}

// createCluster creates a new raft cluster with the given id and address.
func createCluster(ctx context.Context, id, address string, state raft.FSM) (*raft.Raft, *transport.Manager, error) {
	d := raft.DefaultConfig()
	d.LocalID = raft.ServerID(id)
	d.LogOutput = os.Stdout
	d.LogLevel = "WARN"

	//logStore := raft.NewInmemStore()
	//stableStore := raft.NewInmemStore()
	//snapshotStore := raft.NewInmemSnapshotStore()

	logStore, err := boltdb.NewBoltStore(filepath.Join(raftDir, "/log"))
	if err != nil {
		panic(err)
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(raftDir, "/stable"))
	if err != nil {
		panic(err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stdout)
	if err != nil {
		panic(err)
	}

	trans := transport.New(raft.ServerAddress(address), grpcDialOptions(true))

	r, err := raft.NewRaft(d, state, logStore, stableStore, snapshotStore, trans.Transport())
	if err != nil {
		panic(err)
	}

	if raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(id),
					Address:  raft.ServerAddress(address),
				},
			},
		}
		fu := r.BootstrapCluster(cfg)
		if err = fu.Error(); err != nil {
			fmt.Println("(ignoring) Error bootstrapping cluster: ", err)
		}
	} else {
		l := strings.Split(raftJoin[0], ";")
		conn, err := grpc.Dial(l[0], grpcDialOptions(true)...)
		if err != nil {
			panic(err)
		}
		leader := rpc.NewControllerClient(conn)

		host, port, err := net.SplitHostPort(address)
		if err != nil {
			panic(err)
		}

		p, err := strconv.Atoi(port)
		if err != nil {
			panic(err)
		}

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = leader.RegisterAsController(ctx, &rpc.Node{Address: host, Port: uint32(p), Id: &id})
		if err != nil {
			//panic(err)
			fmt.Println("Error registering as controller: ", err)
		}
		cancel()

		fmt.Println("Joined cluster")
	}

	fmt.Println("Bootstrapped cluster")
	return r, trans, nil
}

func deathReporter(node *replicationNode) {
	r := this.raft
	if r.State() != raft.Leader {
		fmt.Println("Not leader, not reporting death, why am I even here?")
		return
	}

	if node == nil {
		fmt.Println("Node is nil, not reporting death (invesigate why it is nil)")
		return
	}

	//remove node from chain
	data, err := proto.Marshal(node.ToNode())
	if err != nil {
		fmt.Println("Error marshalling node: ", err)
		return
	}
	fu := r.ApplyLog(raft.Log{Data: data, Extensions: []byte(NodeRemove)}, 1*time.Second)
	if err := fu.Error(); err != nil {
		fmt.Println("Error applying log: ", err)
	}
	arr, ok := fu.Response().([]*replicationNode)
	if !ok {
		fmt.Println("Unknown return type: ", fu.Response())
		return
	}
	go this.replNodeRemoved(arr[0], arr[1], arr[2])
}

// valid nodes is a list of node hostnames that are allowed to send heartbeats
func beatRecvController(quit *bool, hbSock *net.UDPConn, deathReport func(*replicationNode)) {
	fmt.Println("Starting heartbeat receiver")
	// nodes is a map of node hostnames to last heartbeat time
	nodes := make(map[string]time.Time)

	validLock := &this.state.mtx
	validNodes := &this.state.chain
	hbSock.SetReadBuffer(1 << 16)

	for !*quit {
		buf := make([]byte, 128)
		hbSock.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		n, addr, err := hbSock.ReadFromUDP(buf)
		var id string
		if err == io.EOF {
			fmt.Println("Heartbeat receiver closed")
			break
		}

		//ewww
		if err != nil && strings.Contains(err.Error(), "timeout") {
			goto check
		}

		if err != nil {
			fmt.Println("Error reading from UDP: ", err)
			goto check
		}

		id = string(buf[:n])

		validLock.RLock()
		if validNodes.ContainsId(id) {
			validLock.RUnlock()
			if _, ok := nodes[id]; !ok {
				fmt.Println("Received first heartbeat from: ", addr, " with id: ", id)
			}
			nodes[id] = time.Now()
		} else {
			validLock.RUnlock()
			continue
		}

	check:

		for k, v := range nodes {
			elapsed := time.Since(v)
			if elapsed > 200*time.Millisecond {
				fmt.Println("Node with id ", k, " is dead, removning from list, no heartbeat for ", elapsed.Milliseconds(), " ms")
				delete(nodes, k)
				deathReport(this.state.chain.GetNodeById(k))
			}
		}
	}
}

func myLeadershipObserver(rft *raft.Raft, sock *net.UDPConn) {
	quit := false
	if rft.State() == raft.Leader {
		go beatRecvController(&quit, sock, deathReporter)
	}
	for {
		obs := <-rft.LeaderCh()
		if obs {
			//start heartbeat receiver
			fmt.Println("I am the leader")
			quit = false
			go beatRecvController(&quit, sock, deathReporter)
			//now tell all replicators in chain that leadership changed
			laddr, _ := rft.LeaderWithID() //is me
			this.state.mtx.RLock()
			for val := this.state.chain.Front(); val != nil; val = val.Next() {
				conn, err := val.Value.lazyDial()
				if err != nil {
					fmt.Println("Error connecting to node: ", err)
					continue
				}
				host, port, err := net.SplitHostPort(string(laddr))
				if err != nil {
					fmt.Println("Error splitting host and port: ", err)
					continue
				}
				p, err := strconv.Atoi(port)
				if err != nil {
					fmt.Println("Error converting port to int: ", err)
					continue
				}
				ctx, cancel := ctxTimeout()
				_, err = conn.LeaderChanged(ctx, &rpc.Node{Address: host, Port: uint32(p)})
				cancel()
				if err != nil {
					fmt.Println("Error calling LeaderChanged: ", err)
					continue
				}
			}
			this.state.mtx.RUnlock()
		} else {
			//i am no longer leader
			//stop heartbeat receiver
			fmt.Println("I am not the leader")
			quit = true
			//report that leadership has changed
			//NOW HANDLED BY NEW LEADER
		}
	}
}

func observerFunc() {
	for {
		obs := <-clusterEvents
		leader, ok1 := obs.Data.(raft.LeaderObservation)
		peer, ok2 := obs.Data.(raft.PeerObservation)
		if ok1 {
			fmt.Println("Leadership changed", leader.LeaderAddr, leader.LeaderID)
		}
		if ok2 {
			fmt.Println("Peer changed", peer.Removed, peer.Peer)
		}
	}
}
