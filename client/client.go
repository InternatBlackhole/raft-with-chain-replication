package client

import (
	"context"
	"fmt"
	"time"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	head pb.PutProviderClient
	read pb.ReadProviderClient
)

func main() {
	printUsage()
	//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	var err error
	for {
		var cmd string
		fmt.Scanln(&cmd)
		switch cmd {
		case "put":
			if head == nil {
				fmt.Println("Not connected to head node. Connect to a head node first.")
				continue
			}
			var key, value string
			fmt.Scanln(&key, &value)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_, err := head.Put(ctx, &pb.Entry{Key: key, Value: value})
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
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			ent, err := read.Get(ctx, &wrapperspb.StringValue{Value: key})
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
			fmt.Println("Unknown command")
		}
	}
}

func printUsage() {
	fmt.Print(
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
