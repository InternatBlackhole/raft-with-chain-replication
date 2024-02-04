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
	raftJoin      string
	state         raftState
	//leadershipEvents = make(chan raft.Observation, 3)
	//observer         *raft.Observer
	hbSock *net.UDPConn
	mport  string
)

func init() {
	flag.StringVar(&myAddr, "addr", "localhost:40000", "Address of this node")
	flag.StringVar(&raftId, "raftId", "localhost:", "Node id")
	flag.StringVar(&raftDir, "raftDataDir", "", "raft data directory")
	flag.BoolVar(&raftBootstrap, "rb", false, "Bootstrap the raft cluster?")
	flag.StringVar(&raftJoin, "cl", "", "Join an existing cluster, <hostname:port;id> a comma separated list of nodes")
}

var this *controllerNode //this node

func main() {
	flag.Parse()

	if raftId == "" {
		panic("raftId not set")
	}

	if /*!raftBootstrap &&*/ raftJoin == "" {
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

	/*observer = raft.NewObserver(leadershipEvents, false, func(e *raft.Observation) bool {
		return reflect.TypeOf(*e) == reflect.TypeOf(raft.LeaderObservation{})
	})*/

	this = newControllerNode(hbSock.LocalAddr().(*net.UDPAddr).Port, rft, &state)

	rpc.RegisterControllerServer(s, this)
	tm.Register(s)
	raftadmin.Register(s, rft)
	fmt.Println("Registered servers")

	//go leadershipObserver(leadershipEvents)
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

	//fmt.Println("config: ", d)

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
		/*split := strings.Split(raftJoin, ",")
		servers := make([]raft.Server, 0, len(split))
		for i, s := range split {
			if !strings.Contains(s, ";") {
				panic("Invalid server id")
			}
			split2 := strings.Split(s, ";")
			if len(split2) != 2 {
				panic("Invalid server id format, should be <hostname:port;id>")
			}
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(split2[1]),
				Address:  raft.ServerAddress(split2[0]),
			})
			fmt.Println("Added server: ", servers[i])
		}
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(address),
		})*/

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
			//panic(err)
		}
	} else {
		if raftJoin == "" {
			return nil, nil, fmt.Errorf("no leader to join")
		}
		conn, err := grpc.Dial(raftJoin, grpcDialOptions(true)...)
		if err != nil {
			panic(err)
		}
		leader := rpc.NewControllerClient(conn)

		p, err := strconv.Atoi(mport)
		if err != nil {
			panic(err)
		}

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = leader.RegisterAsController(ctx, &rpc.Node{Address: address, Port: uint32(p), Id: &id})
		if err != nil {
			panic(err)
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
}

// valid nodes is a list of node hostnames that are allowed to send heartbeats
func beatRecvController(quit *bool, hbSock *net.UDPConn, deathReport func(*replicationNode)) {
	fmt.Println("Starting heartbeat receiver")
	// nodes is a map of node hostnames to last heartbeat time
	nodes := make(map[string]time.Time)

	validLock := &this.state.mtx
	validNodes := &this.state.chain

	for !*quit {
		buf := make([]byte, 1024)
		hbSock.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, addr, err := hbSock.ReadFromUDP(buf)
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
			continue
		}

	check:

		id := string(buf[:n])
		validLock.RLock()
		if validNodes.ContainsId(id) {
			validLock.RUnlock()
			if _, ok := nodes[id]; !ok {
				fmt.Println("Received first heartbeat from: ", addr, " with id: ", id)
			}
			nodes[id] = time.Now()
		} else {
			validLock.RUnlock()
			//fmt.Println("Received heartbeat from invalid node: ", addr)
			continue
		}

		//TODO: reenable heartbeart death reporting
		/*for k, v := range nodes {
			elapsed := time.Since(v)
			if elapsed > 500*time.Millisecond {
				fmt.Println("Node with id ", k, " is dead, removning from list, no heartbeat for ", elapsed.Milliseconds(), " ms")
				deathReport(this.state.chain.GetNodeById(k))
				delete(nodes, k)
			}
		}*/

		//fmt.Println("Received heartbeat from: ", addr)
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
		} else {
			//i am no leader
			//stop heartbeat receiver
			fmt.Println("I am not the leader")
			quit = true
			//report that leadership has changed
			laddr, lid := rft.LeaderWithID()
			if lid == "" {
				fmt.Println("no leader, maybe elections are ongoing?")
				continue
			}
			host, port, err := net.SplitHostPort(string(laddr))
			if err != nil {
				fmt.Println("Error splitting host and port: ", err)
				continue
			}
			this.state.mtx.RLock()
			for val := this.state.chain.Front(); val != nil; val = val.Next() {
				node := val.Value
				conn, err := node.lazyDial()
				if err != nil {
					fmt.Println("Error connecting to node: ", err)
					continue
				}
				p, err := strconv.Atoi(port)
				if err != nil {
					fmt.Println("Error converting port to int: ", err)
					continue
				}
				_, err = conn.LeaderChanged(context.Background(), &rpc.Node{Address: host, Port: uint32(p)})
				if err != nil {
					fmt.Println("Error calling LeaderChanged: ", err)
					continue
				}
			}
			this.state.mtx.RUnlock()
		}
	}
}

/*func leadershipObserver(c <-chan raft.Observation) {
	for {
		obs := <-c
		if this.raft.State() != raft.Leader {
			//do nothing if not leader
			continue
		}
		//data := obs.Data.(raft.LeaderObservation)
		go myLeadershipObserver(this.raft, hbSock)
	}
}*/
