package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"
	"time"

	pb "timkr.si/ps-izziv/client/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	head     pb.PutProviderClient
	read     pb.ReadProviderClient
	ctrlAddr string
)

func init() {
	flag.StringVar(&ctrlAddr, "controller", "", "controller hostname:port combination")
}

func timeoutCtx() (context.Context, context.CancelFunc) {
	//TODO: change timeout to 1 seconds
	return context.WithTimeout(context.Background(), 100*time.Second)
}

func main() {
	flag.Parse()

	if ctrlAddr == "" {
		panic("controller hostname not set")
	}
	printUsage()

	conn, err := grpc.Dial(ctrlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	controller := pb.NewControllerClient(conn)

	ctx, cancel := timeoutCtx()
	h, err := controller.GetHead(ctx, &emptypb.Empty{})
	cancel()
	if err != nil {
		panic(err)
	}
	p := strconv.Itoa(int(h.Port))
	addr := net.JoinHostPort(h.Address, p)
	fmt.Println("Connected to head node: ", addr)
	head, err = getPutProvider(addr)
	if err != nil {
		panic(err)
	}

	ctx, cancel = timeoutCtx()
	r, err := controller.GetTail(ctx, &emptypb.Empty{})
	cancel()
	if err != nil {
		panic(err)
	}
	p = strconv.Itoa(int(r.Port))
	addr = net.JoinHostPort(r.Address, p)
	fmt.Println("Connected to read node: ", addr)
	read, err = getReadProvider(addr)
	if err != nil {
		panic(err)
	}

	for {
		var cmd string
		fmt.Scan(&cmd)
		switch cmd {
		case "put":
			if head == nil {
				fmt.Println("Not connected to head node. Connect to a head node first.")
				continue
			}
			var key, value string
			fmt.Scanln(&key, &value)
			ctx, cancel := timeoutCtx()
			_, err := head.Put(ctx, &pb.Entry{Key: key, Value: value})
			cancel()
			if err != nil {
				fmt.Println("Error: ", err)
				continue
			}
		case "get":
			if read == nil {
				fmt.Println("Not connected to read node. Connect to a read node first.")
				continue
			}
			var key string
			fmt.Scanln(&key)
			ctx, cancel := timeoutCtx()
			ent, err := read.Get(ctx, &wrapperspb.StringValue{Value: key})
			cancel()
			if err != nil {
				fmt.Println("Error: ", err)
				continue
			}
			fmt.Println("Value: ", ent.Value)
		case "quit":
			return
		case "ch":
			if head != nil {
				fmt.Println("Already connected to head node. Disconnecting.")
			}
			var hostname string
			fmt.Scanln(&hostname)
			head, err = getPutProvider(hostname)
			if err != nil {
				fmt.Println("Error: ", err)
				continue
			}
			fmt.Println("Connected to head node: ", hostname)
		case "cn":
			if read != nil {
				fmt.Println("Already connected to read node. Disconnecting.")
			}
			var hostname string
			fmt.Scanln(&hostname)
			read, err = getReadProvider(hostname)
			if err != nil {
				fmt.Println("Error: ", err)
				continue
			}
			fmt.Println("Connected to read node: ", hostname)
		default:
			fmt.Println("Unknown command", cmd)
		}
	}
}

func printUsage() {
	fmt.Println(
		"Usage:\n\tch <hostname:port> - connect to head node\n\tcn <hostname:port> - connect to node for reading" +
			"\n\tput <key> <value>\n\tget <key>\n\tquit - exit the program")
}

func getPutProvider(hostname string) (pb.PutProviderClient, error) {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewPutProviderClient(conn), nil
}

func getReadProvider(hostname string) (pb.ReadProviderClient, error) {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewReadProviderClient(conn), nil
}

func getControllerNode(hostname string) (pb.ControllerClient, error) {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewControllerClient(conn), nil
}
