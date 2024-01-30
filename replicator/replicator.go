package replicator

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var chain *chainControl

func serverMain(controllerHostname string, myPort int, meId int) {
	// Connect to the controller node to get leader info
	ctrl := getControllerNode(controllerHostname)

	leaderNode, err := ctrl.GetLeader(context.Background(), &emptypb.Empty{})
	if err != nil {
		panic(errors.Join(errors.New("could not get leader info from controller"), err))
	}

	chainControl, initDone := newChainControl(leaderNode)

	s := grpc.NewServer()

	// Registration of the replication provider
	node := NewReplicatorNode(chainControl.prevGetter(), chainControl.nextGetter())
	pb.RegisterReplicationProviderServer(s, node)
	pb.RegisterPutProviderServer(s, node)
	pb.RegisterReadProviderServer(s, node)

	pb.RegisterControllerEventsServer(s, chainControl)
	fmt.Println("Registered Servers")

	//listen on all addresses
	lst, err := net.Listen("tcp", ":"+strconv.Itoa(myPort))
	errPanic(err)

	err = s.Serve(lst)
	if err != nil {
		//fatal error on Serve
		panic(err)
	}
	fmt.Printf("Node %d started on port %d\n", meId+1, myPort)

	fmt.Println("Registering with controller...")
	hostname, err := os.Hostname()
	errPanic(err)
	chainControl.mtx.RLock()
	//register also reports next and prev node info
	_, err = chainControl.leader.Register(context.Background(), &pb.Node{Address: hostname, Port: uint32(myPort)})
	if err != nil {
		errors.Join(errors.New("could not register with controller"), err)
	}
	chainControl.mtx.RUnlock()

	fmt.Println("Waiting for chain info from controller...")
	<-initDone
	fmt.Println("Chain info received from controller")
	fmt.Println("Replicator node running")

	c := make(chan os.Signal, 1)
	signal.Notify(c)

	for {
		s := <-c
		if s == os.Interrupt || s == syscall.SIGTERM {
			fmt.Println("Interrupted, exiting...")
			os.Exit(0)
		}
		fmt.Println("Got signal:", s)
	}
}

func errPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func getControllerNode(hostname string) pb.ControllerClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	errPanic(err)
	return pb.NewControllerClient(conn)
}

func writeMyConnectInfo(meId int, myPort int) {
	file, err := os.Create("connectInfo_" + strconv.Itoa(meId) + ".txt")
	errPanic(err)
	defer file.Close()

	host, err := os.Hostname()
	errPanic(err)

	str := fmt.Sprintf("%s:%d\n", host, myPort)
	file.WriteString(str)
}

func main() {
	hostname := flag.String("controller", "", "controller hostname:port combination")
	myport := flag.Int("port", 0, "port to listen on")
	id := flag.Int("meId", -1, "my id")

	flag.Parse()

	if *hostname == "" {
		panic("controller hostname not set")
	}

	if *myport == 0 {
		panic("port not set")
	}

	if *id <= -1 {
		panic("meId not set")
	}

	fmt.Printf("Starting node %d on port %d\n", *id, *myport)
	writeMyConnectInfo(*id, *myport)
	serverMain(*hostname, *myport, *id)
}
