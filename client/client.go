package client

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"time"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	hName := flag.String("s", "", "Hostname of replication provider")

	flag.Parse()

	hostname := *hName

	if hostname == "" {
		fmt.Println("Hostname not provided")
		return
	}

	fmt.Println("Client waiting 200 ms...")
	time.Sleep(200 * time.Millisecond)

	fmt.Println("Client starting...")

	fmt.Printf("Connecting to head node on port %d and tail node in port %d\n", startPort, startPort+totalNodes-1)

	head, tail := getReplicatorNode(startPort), getReplicatorNode(startPort+totalNodes-1)

	fmt.Println("Connected to head and tail node")
	fmt.Println("Sending put request to head node")

	ncon := context.Background()
	const toGen = 20

	for i := 0; i < toGen; i++ {
		//generate 10 key value pairs
		_, err := head.Put(ncon, &pb.Entry{Key: "key" + strconv.Itoa(i), Value: "value" + strconv.Itoa(i)})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Put key: %s, value: %s\n", "key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}

	fmt.Println()
	fmt.Println("Put requests sent to head node")
	fmt.Println("Sending get request to tail node")

	for i := 0; i < toGen/2; i++ {
		ent, err := tail.Get(ncon, &pb.Entry{Key: "key" + strconv.Itoa(rand.Intn(toGen/2))})
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("Got key: %s, value: %s\n", ent.Key, ent.Value)
		}
	}

	fmt.Println()
	fmt.Println("Sending modify put request to head node")

	for i := 0; i < toGen/2; i++ {
		key, val := "key"+strconv.Itoa(rand.Intn(toGen)), "value"+strconv.Itoa(rand.Intn(toGen))
		_, err := head.Put(ncon, &pb.Entry{Key: key, Value: val})
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("Modified key: %s, to value: %s\n", key, val)
	}

	fmt.Println()
	fmt.Println("Sending get request to tail node")
	for i := 0; i < toGen/2; i++ {
		ent, err := tail.Get(ncon, &pb.Entry{Key: "key" + strconv.Itoa(rand.Intn(toGen/2))})
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("Got key: %s, value: %s\n", ent.Key, ent.Value)
		}
	}

	fmt.Println("Client done")
}

func getPutProvider(hostname string) pb.PutProviderClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewPutProviderClient(conn)
}

func getReadProvider(hostname string) pb.ReadProviderClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewReadProviderClient(conn)
}

func getReplicatorNode(hostname string) pb.ReplicationProviderClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pb.NewReplicationProviderClient(conn)
}
