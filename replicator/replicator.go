package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	pb "timkr.si/ps-izziv/replicator/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	controllerHostname string
	myPort             int
	meId               string
	controllers        []string
)

func init() {
	flag.StringVar(&controllerHostname, "controller", "", "controller hostname:port combination")
	flag.IntVar(&myPort, "port", 0, "port to listen on")
	flag.StringVar(&meId, "meId", "", "my id")
}

func errPanic(err error) {
	if err != nil {
		panic(err)
	}
}

func getControllerNode(hostname string) (*grpc.ClientConn, error) {
	return grpc.Dial(hostname, grpcDialOptions(true)...)
}

func main() {
	flag.Parse()

	if controllerHostname == "" {
		panic("controller hostname not set")
	}

	if myPort == 0 {
		panic("port not set")
	}

	if meId == "" {
		panic("meId not set")
	}
	controllers = flag.Args()

	storage := new(sync.Map)

	fmt.Printf("Starting node %s on port %d\n", meId, myPort)

	host, err := "localhost", error(nil) //os.Hostname()
	errPanic(err)
	fmt.Printf("%s:%d\n", host, myPort)

	// Connect to the controller node to get leader info
	conn, err := getControllerNode(controllerHostname)
	errPanic(err)
	ctrl := pb.NewControllerClient(conn)

	ctx, cancel := ctxTimeout()
	leaderNode, err := ctrl.GetLeader(ctx, &emptypb.Empty{})
	cancel()
	if err != nil {
		panic(errors.Join(errors.New("could not get leader info from controller"), err))
	}

	fmt.Println("Leader info received from controller: ", leaderNode)

	chainControl, initDone := newChainControl(leaderNode, storage)

	s := grpc.NewServer()
	ready := make(chan struct{})

	//listen on all addresses
	lst, err := net.Listen("tcp", ":"+strconv.Itoa(myPort))
	errPanic(err)

	go func() {
		fmt.Println("Serving on port ", myPort)
		close(ready)
		err = s.Serve(lst)
		if err != nil {
			//fatal error on Serve
			panic(err)
		}
	}()

	// Registration of the replication provider
	node := NewReplicatorNode(chainControl.prevGetter(), chainControl.nextGetter(), storage)
	pb.RegisterReplicationProviderServer(s, node)
	pb.RegisterPutProviderServer(s, node)
	pb.RegisterReadProviderServer(s, node)

	pb.RegisterControllerEventsServer(s, chainControl)
	fmt.Println("Registered Servers")

	fmt.Printf("Node %s started on port %d\n", meId, myPort)

	<-ready
	fmt.Println("Registering with controller...")
	//register also reports next and prev node info
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	neighs, err := chainControl.RegisterAsReplicator(ctx, &pb.Node{Address: host, Port: uint32(myPort), Id: &meId})
	if err != nil {
		fmt.Println("could not register with controller: ", err)
	}
	cancel()

	fmt.Println("Registered with controller")
	if neighs.Prev != nil && neighs.Prev.Address != "" {
		fmt.Println("Prev: ", neighs.Prev.Address, ":", neighs.Prev.Port)
		chainControl.prev = getNode(net.JoinHostPort(neighs.Prev.Address, strconv.Itoa(int(neighs.Prev.Port))))
	} else {
		fmt.Println("No prev node")
		chainControl.prev = nil
	}
	if neighs.Next != nil && neighs.Next.Address != "" {
		fmt.Println("Next: ", neighs.Next.Address, ":", neighs.Next.Port)
		chainControl.next = getNode(net.JoinHostPort(neighs.Next.Address, strconv.Itoa(int(neighs.Next.Port))))
	} else {
		fmt.Println("No next node")
		chainControl.next = nil
	}

	fmt.Println("Waiting for chain info from controller... (waiting for initDone)")
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
		if s == syscall.SIGIO || s == syscall.SIGURG {
			continue
		}
		fmt.Println("Got signal:", s)
	}
}

func grpcDialOptions(nobuffer bool) []grpc.DialOption {
	arr := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if nobuffer {
		arr = append(arr, grpc.WithWriteBufferSize(0), grpc.WithReadBufferSize(1024))
	}
	return arr
}

func ctxTimeout() (context.Context, context.CancelFunc) {
	//TODO: change timeout to 1 second
	return context.WithTimeout(context.Background(), 10*time.Second)
}
