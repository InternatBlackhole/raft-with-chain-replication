package replicator

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var startPort, myPort, meId int
var next, prev pb.ReplicationProviderClient = nil, nil
var nextLock, prevLock sync.Mutex

func myNext() pb.ReplicationProviderClient {
	nextLock.Lock()
	defer nextLock.Unlock()
	return next
}

func myPrev() pb.ReplicationProviderClient {
	prevLock.Lock()
	defer prevLock.Unlock()
	return prev
}

func serverMain() {
	// Connect to the "first" controller node to get leader info

	// Ustvari nov strežnik
	s := grpc.NewServer()

	// Registriraj Replicator strežnik
	pb.RegisterReplicationProviderServer(s, &ReplicatorNode{next: myNext, prev: myPrev})
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

func clientMain() {

}

func getNodeOnPort(port int) pb.ReplicationProviderClient {
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewReplicationProviderClient(conn)
}

func main() {
	sPort := flag.Int("sp", 27000, "Port of the first node")
	nodeNum := flag.Int("i", 0, "Node number")

	flag.Parse()

	startPort = *sPort
	meId = *nodeNum
	myPort = startPort + meId

	fmt.Printf("Starting node %d on port %d\n", meId+1, myPort)
	serverMain()
}
