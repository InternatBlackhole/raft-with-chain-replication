package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"timkr.si/ps-izziv/replicator/rpc"
	pb "timkr.si/ps-izziv/replicator/rpc"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type chainControl struct {
	mtx  sync.RWMutex
	next *grpc.ClientConn
	prev *grpc.ClientConn

	//leader controllerInfo
	controllerConn *grpc.ClientConn
	pb.ControllerClient
	leaderHb    *net.UDPConn
	leaderClose chan bool

	initFinish chan struct{}
	leaderInit bool

	storage *sync.Map

	pb.UnimplementedControllerEventsServer
}

// chan bool is used to signal that all initializations are done
func newChainControl(leaderNode *rpc.Node, storage *sync.Map) (*chainControl, <-chan struct{}) {
	done := make(chan struct{})
	this := &chainControl{leaderClose: make(chan bool), initFinish: done, storage: storage}
	initialLeader, err := getControllerNode(leaderNode.Address + ":" + strconv.Itoa(int(leaderNode.Port)))
	if err != nil {
		panic(errors.Join(errors.New("could not connect to leader "), err))
	}
	this.leaderChanged(initialLeader, leaderNode)
	return this, done
}

func (c *chainControl) nextGetter() func() pb.ReplicationProviderClient {
	return func() pb.ReplicationProviderClient {
		c.mtx.RLock()
		defer c.mtx.RUnlock()
		if c.next == nil {
			return nil
		}
		return pb.NewReplicationProviderClient(c.next)
	}
}

func (c *chainControl) prevGetter() func() pb.ReplicationProviderClient {
	return func() pb.ReplicationProviderClient {
		c.mtx.RLock()
		defer c.mtx.RUnlock()
		if c.prev == nil {
			return nil
		}
		return pb.NewReplicationProviderClient(c.prev)
	}
}

// received from leader node
func (c *chainControl) NextChanged(ctx context.Context, next *pb.Node) (*emptypb.Empty, error) {
	c.mtx.Lock()
	if c.next != nil {
		c.next.Close()
		c.next = nil
	}
	if next.Address == "" {
		c.next = nil
	} else {
		c.next = getNode(next.Address + ":" + strconv.Itoa(int(next.Port)))
	}
	fmt.Println("Next changed to: ", next.Address, ":", next.Port)
	c.mtx.Unlock()
	//c.InitiateTransfer(ctx, next)

	return &emptypb.Empty{}, nil
}

// received from leader node
func (c *chainControl) PrevChanged(ctx context.Context, prev *pb.Node) (*emptypb.Empty, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.prev != nil {
		c.prev.Close()
	}
	if prev.Address == "" {
		c.prev = nil
	} else {
		c.prev = getNode(prev.Address + ":" + strconv.Itoa(int(prev.Port)))
	}
	fmt.Println("Prev changed to: ", prev.Address, ":", prev.Port)
	return &emptypb.Empty{}, nil
}

// received from ex leader node
func (c *chainControl) LeaderChanged(ctx context.Context, leader *pb.Node) (*emptypb.Empty, error) {
	conn, err := getControllerNode(leader.Address + ":" + strconv.Itoa(int(leader.Port)))
	if err != nil {
		panic(errors.Join(errors.New("could not connect to leader "), err))
	}
	c.leaderChanged(conn, leader)
	return &emptypb.Empty{}, nil
}

func (c *chainControl) InitiateTransfer(ctx context.Context, in *pb.Node) (*emptypb.Empty, error) {
	if in == nil {
		return nil, errors.New("node not set")
	}
	fmt.Println("Sending storage to new next...")
	conn, err := grpc.Dial(in.Address+":"+strconv.Itoa(int(in.Port)), grpcDialOptions(false)...)
	if err != nil {
		return nil, errors.Join(errors.New("could not connect to requested next"), err)
	}
	prov := pb.NewReplicationProviderClient(conn)
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	str, err := prov.StreamEntries(ctx)
	if err != nil {
		fmt.Println("Error streaming entries to new next: ", err)
		//cancel()
		return nil, err
	}
	iserror := false
	syncDone := make(chan struct{})
	go func() {
		//receiver
		defer close(syncDone)
		for {
			val, err := str.Recv()
			if err == io.EOF {
				return
			}
			fmt.Println("New tail added entry: ", val.Key, "commitedVersion:", val.Version)
		}
	}()
	c.storage.Range(func(key, value interface{}) bool {
		entry := value.(entry)
		fmt.Println("Sending entry: ", key, ":", entry.value, ":", entry.commitedVersion)
		err = str.Send(&pb.InternalEntry{Key: key.(string), Value: entry.value, Version: entry.commitedVersion})
		iserror = err != nil
		return !iserror
	})
	if iserror {
		fmt.Println("Error sending entries to requested: ", err)
		return nil, err
	}
	err = str.CloseSend()
	if err != nil {
		fmt.Println("Error closing stream to requested: ", err)
		return nil, err
	}
	fmt.Println("Waiting for receiver to finish...")
	<-syncDone
	fmt.Println("Storage sent to requested, reporting done")
	//ctx, cancel = ctxTimeout()
	c.mtx.RLock()
	_, err = c.MarkTransferDone(ctx, in)
	c.mtx.RUnlock()
	//cancel()
	if err != nil {
		fmt.Println("Error in MarkTransferDone: ", err)
	}
	return &emptypb.Empty{}, nil
}

func getNode(hostname string) *grpc.ClientConn {
	conn, err := grpc.Dial(hostname, grpcDialOptions(false)...)
	if err != nil {
		panic(errors.Join(errors.New("could not connect to node "), err))
	}
	return conn
}

func (c *chainControl) leaderChanged(newLeader *grpc.ClientConn, lNode *pb.Node) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.leaderHb != nil {
		//blocks until heartbeat is stopped
		c.leaderClose <- true
		err := c.leaderHb.Close()
		if err != nil {
			fmt.Println("Error closing heartbeat connection: ", err)
		}
		c.leaderHb = nil
	}

	// Connect to the leader node
	c.controllerConn = newLeader
	c.ControllerClient = pb.NewControllerClient(newLeader)

	ctx, _ := ctxTimeout()
	hbPortV, err := c.GetHeartbeatEndpoint(ctx, &emptypb.Empty{})
	if err != nil {
		panic(errors.Join(errors.New("could not get heartbeat port from controller "), err))
	}
	hbPort := hbPortV.Value
	fmt.Println("Leader heartbeat port: ", hbPort)

	// Start heartbeat
	hbAddr, err := net.ResolveUDPAddr("udp", lNode.Address+":"+strconv.Itoa(int(hbPort)))
	if err != nil {
		panic(errors.Join(errors.New("could not resolve heartbeat endpoint "), err))
	}
	hbConn, err := net.DialUDP("udp", nil, hbAddr)
	if err != nil {
		panic(errors.Join(errors.New("could not connect to heartbeat endpoint "), err))
	}
	c.leaderHb = hbConn

	//TODO: more stuff needs to happen
	fmt.Println("Leader changed to: ", lNode.Address, ":", lNode.Port)

	go c.heartbeat(info.id)

	c.leaderInit = true
	c.triggerInitDone()
}

// if there is no leader because maybe elections are going on, this will try call every half second for 5 seconds

func (c *chainControl) triggerInitDone() {
	if c.leaderInit && c.initFinish != nil {
		close(c.initFinish)
		c.initFinish = nil
	}
}

func (c *chainControl) heartbeat(id string) {
	fmt.Println("Starting heartbeat")
	//beats := 0
	for {
		c.mtx.RLock()
		c.leaderHb.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
		_, err := c.leaderHb.Write([]byte(id))
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				c.mtx.RUnlock()
				continue
			}
			//the leader died or something, get new leader
			fmt.Println("Heartbeat failed. Error: ", err)
			fmt.Println("Waiting for new leader...")
			c.leaderHb = nil
			c.mtx.RUnlock()
			return
		} else {
			c.mtx.RUnlock()
		}
		select {
		case val := <-c.leaderClose:
			if val {
				return
			}
		case <-time.After(100 * time.Millisecond):
			select {
			case val := <-c.leaderClose:
				if val {
					return
				}
			default:
				// do nothing
			}
		}
	}
}
