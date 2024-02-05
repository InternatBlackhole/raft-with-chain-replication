package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "timkr.si/ps-izziv/replicator/rpc"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

/*type controllerInfo struct {
	ctrl  pb.ControllerClient
	mtx   sync.Mutex
	hb    *net.UDPConn
	close chan bool
}*/

type chainControl struct {
	mtx  sync.RWMutex
	next *grpc.ClientConn
	prev *grpc.ClientConn

	//leader controllerInfo
	pb.ControllerClient
	leaderHb    *net.UDPConn
	leaderClose chan bool

	initFinish chan struct{}
	leaderInit bool

	storage *sync.Map

	pb.UnimplementedControllerEventsServer
}

/*func newControllerInfo(ctrl pb.ControllerClient) *controllerInfo {
	return &controllerInfo{ctrl: ctrl, close: make(chan bool, 0)}
}*/

// chan bool is used to signal that all initializations are done
func newChainControl(initialLeader *pb.Node, storage *sync.Map) (*chainControl, <-chan struct{}) {
	done := make(chan struct{})
	this := &chainControl{leaderClose: make(chan bool), initFinish: done, storage: storage}
	this.leaderChanged(initialLeader)
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
	defer c.mtx.Unlock()
	wasTail := c.next == nil
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

	if wasTail {
		fmt.Println("Sending storage to new next...")
		go func() {
			prov := pb.NewReplicationProviderClient(c.next)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			str, err := prov.StreamEntries(ctx)
			if err != nil {
				fmt.Println("Error streaming entries to new next: ", err)
				cancel()
				return
			}
			iserror := false
			c.storage.Range(func(key, value interface{}) bool {
				entry := value.(entry)
				fmt.Println("Sending entry: ", key, ":", entry.value, ":", entry.pendingVersion)
				err = str.Send(&pb.InternalEntry{Key: key.(string), Value: entry.value, Version: entry.pendingVersion})
				iserror = err != nil
				return !iserror
			})
			cancel()
			if iserror {
				fmt.Println("Error sending entries to new next: ", err)
				return
			}
			err = str.CloseSend()
			if err != nil {
				fmt.Println("Error closing stream to new next: ", err)
				return
			}
		}()
	}

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
	/*c.prevInit = true
	c.triggerInitDone()*/
	return &emptypb.Empty{}, nil
}

// received from ex leader node
func (c *chainControl) LeaderChanged(ctx context.Context, leader *pb.Node) (*emptypb.Empty, error) {
	c.leaderChanged(leader)
	return &emptypb.Empty{}, nil
}

func getNode(hostname string) *grpc.ClientConn {
	conn, err := grpc.Dial(hostname, grpcDialOptions(false)...)
	if err != nil {
		panic(errors.Join(errors.New("could not connect to node "), err))
	}
	return conn
}

func (c *chainControl) leaderChanged(newLeader *pb.Node) {
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
	c.ControllerClient = getControllerNode(newLeader.Address + ":" + strconv.Itoa(int(newLeader.Port)))

	ctx, _ := ctxTimeout()
	hbPortV, err := c.GetHeartbeatEndpoint(ctx, &emptypb.Empty{})
	if err != nil {
		panic(errors.Join(errors.New("could not get heartbeat port from controller "), err))
	}
	hbPort := hbPortV.Value
	fmt.Println("Leader heartbeat port: ", hbPort)

	// Start heartbeat
	hbAddr, err := net.ResolveUDPAddr("udp", newLeader.Address+":"+strconv.Itoa(int(hbPort)))
	if err != nil {
		panic(errors.Join(errors.New("could not resolve heartbeat endpoint "), err))
	}
	hbConn, err := net.DialUDP("udp", nil, hbAddr)
	if err != nil {
		panic(errors.Join(errors.New("could not connect to heartbeat endpoint "), err))
	}
	c.leaderHb = hbConn

	//TODO: more stuff needs to happen
	fmt.Println("Leader changed to: ", newLeader.Address, ":", newLeader.Port)

	go c.heartbeat(meId)

	c.leaderInit = true
	c.triggerInitDone()
}

// if there is no leader because maybe elections are going on, this will try call every half second for 5 seconds
func (c *chainControl) GetLeader(ctx context.Context, in *emptypb.Empty) (*pb.Node, error) {
	node, err := c.ControllerClient.GetLeader(ctx, in)
	if err != nil {
		for i := 0; strings.Contains(err.Error(), "elections ongoing") && i < 9; i++ {
			time.Sleep(500 * time.Millisecond)
			node, err := c.ControllerClient.GetLeader(ctx, in)
			if err == nil {
				return node, nil
			}
		}
		if !strings.Contains(err.Error(), "elections ongoing") {
			return nil, err
		}
	}
	return node, nil
}

func (c *chainControl) triggerInitDone() {
	if /*c.nextInit && c.prevInit &&*/ c.leaderInit && c.initFinish != nil {
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
			fmt.Println("Getting new leader...")
			c.leaderHb = nil
			c.mtx.RUnlock()
			ctx, cancel := ctxTimeout()
			newLeader, err := c.GetLeader(ctx, &emptypb.Empty{})
			cancel()
			if err != nil {
				panic(err)
			}
			//will launch new heartbeat, this one quits
			c.leaderChanged(newLeader)
			return
		}
		c.mtx.RUnlock()
		/*beats++
		if beats%100 == 0 {
			fmt.Println("100 Heartbeats sent")
		}*/
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
