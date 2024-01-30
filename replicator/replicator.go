package replicator

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var myPort, meId int
var controllerHostname string
var leader controllerInfo = controllerInfo{ctrl: nil}
var next, prev replicatorInfo = replicatorInfo{repl: nil}, replicatorInfo{repl: nil}

type replicatorInfo struct {
	repl pb.ReplicationProviderClient
	mtx  sync.Mutex
}

type controllerInfo struct {
	ctrl pb.ControllerClient
	mtx  sync.Mutex
}

func myNext() pb.ReplicationProviderClient {
	next.mtx.Lock()
	defer next.mtx.Unlock()
	return next.repl
}

func myPrev() pb.ReplicationProviderClient {
	prev.mtx.Lock()
	defer prev.mtx.Unlock()
	return prev.repl
}

func serverMain() {
	// Connect to the controller node to get leader info
	ctrl := getControllerNode(controllerHostname)

	leader, err := ctrl.GetLeader(context.Background(), &emptypb.Empty{})
	if err != nil {
		panic(errors.Join(errors.New("could not get leader info from controller"), err))
	}

	// Ustvari nov strežnik
	s := grpc.NewServer()

	// Registriraj Replicator strežnik
	pb.RegisterReplicationProviderServer(s, NewReplicatorNode(myPrev, myNext))
	lst, err := net.Listen("tcp", "localhost:"+strconv.Itoa(myPort))
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node %d started on port %d\n", meId+1, myPort)
	fmt.Println("Waiting 100 ms for other nodes to start...")
	time.Sleep(100 * time.Millisecond)
	//i hope all processes are up and running thus far

	// get next and prev
	if myPort != startPort {
		conn, err := grpc.Dial("localhost:"+strconv.Itoa(myPort-1), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		prev = pb.NewReplicationProviderClient(conn)
		fmt.Println("Connected to previous node")
	}

	if myPort != startPort+totalNodes-1 {
		conn, err := grpc.Dial("localhost:"+strconv.Itoa(myPort+1), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		next = pb.NewReplicationProviderClient(conn)
		fmt.Println("Connected to next node")
	}

	err = s.Serve(lst)
	if err != nil {
		//fatal error on Serve
		panic(err)
	}
}

func getNode(hostname string) pb.ReplicationProviderClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewReplicationProviderClient(conn)
}

func getControllerNode(hostname string) pb.ControllerClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewControllerClient(conn)
}

func writeMyConnectInfo() {
	file, err := os.Create("connectInfo_" + strconv.Itoa(meId) + ".txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	str := fmt.Sprintf("%s:%d\n", host, myPort)
	file.WriteString(str)
}

func main() {
	flag.StringVar(&controllerHostname, "controller", "", "controller hostname:port combination")
	flag.IntVar(&myPort, "port", 0, "port to listen on")
	flag.IntVar(&meId, "meId", -1, "my id")

	flag.Parse()

	if controllerHostname == "" {
		panic("controller hostname not set")
	}

	if myPort == 0 {
		panic("port not set")
	}

	if meId <= -1 {
		panic("meId not set")
	}

	fmt.Printf("Starting node %d on port %d\n", meId, myPort)
	serverMain()
}
