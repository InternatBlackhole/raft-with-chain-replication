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
	//nextInit   bool
	//prevInit   bool
	leaderInit bool

	pb.UnimplementedControllerEventsServer
}

/*func newControllerInfo(ctrl pb.ControllerClient) *controllerInfo {
	return &controllerInfo{ctrl: ctrl, close: make(chan bool, 0)}
}*/

// chan bool is used to signal that all initializations are done
func newChainControl(initialLeader *pb.Node) (*chainControl, <-chan struct{}) {
	done := make(chan struct{})
	this := &chainControl{leaderClose: make(chan bool), initFinish: done}
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
	//wasTail := c.next == nil
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.next != nil {
		c.next.Close()
	}
	if next.Address == "" {
		c.next = nil
	} else {
		c.next = getNode(next.Address + ":" + strconv.Itoa(int(next.Port)))
	}
	fmt.Println("Next changed to: ", next.Address, ":", next.Port)
	//c.nextInit = true
	//c.triggerInitDone()
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
			fmt.Println("Heartbeat failed. Error: ", err)
			c.leaderHb = nil
			c.mtx.RUnlock()
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
