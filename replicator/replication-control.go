package replicator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
	pb "tkNaloga04/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	next pb.ReplicationProviderClient
	prev pb.ReplicationProviderClient

	//leader controllerInfo
	leader      pb.ControllerClient
	leaderHb    *net.UDPConn
	leaderClose chan bool

	initFinish chan struct{}
	nextInit   bool
	prevInit   bool
	leaderInit bool

	pb.UnimplementedControllerEventsServer
}

/*func newControllerInfo(ctrl pb.ControllerClient) *controllerInfo {
	return &controllerInfo{ctrl: ctrl, close: make(chan bool, 0)}
}*/

// chan bool is used to signal that all initializations are done
func newChainControl(initialLeader *pb.Node) (*chainControl, <-chan struct{}) {
	done := make(chan struct{})
	this := &chainControl{leaderClose: make(chan bool, 0), initFinish: done}
	this.leaderChanged(initialLeader)
	return this, done
}

func (c *chainControl) nextGetter() func() pb.ReplicationProviderClient {
	return func() pb.ReplicationProviderClient {
		c.mtx.RLock()
		defer c.mtx.RUnlock()
		return c.next
	}
}

func (c *chainControl) prevGetter() func() pb.ReplicationProviderClient {
	return func() pb.ReplicationProviderClient {
		c.mtx.RLock()
		defer c.mtx.RUnlock()
		return c.prev
	}
}

// received from leader node
func (c *chainControl) NextChanged(ctx context.Context, next *pb.Node) (*emptypb.Empty, error) {
	//wasTail := c.next == nil
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if next.Address == "" {
		c.next = nil
	} else {
		c.next = getNode(next.Address + ":" + strconv.Itoa(int(next.Port)))
	}
	fmt.Println("Next changed to: ", next.Address, ":", next.Port)
	c.nextInit = true
	c.triggerInitDone()
	return &emptypb.Empty{}, nil
}

// received from leader node
func (c *chainControl) PrevChanged(ctx context.Context, prev *pb.Node) (*emptypb.Empty, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if prev.Address == "" {
		c.prev = nil
	} else {
		c.prev = getNode(prev.Address + ":" + strconv.Itoa(int(prev.Port)))
	}
	fmt.Println("Prev changed to: ", prev.Address, ":", prev.Port)
	c.prevInit = true
	c.triggerInitDone()
	return &emptypb.Empty{}, nil
}

// received from ex leader node
func (c *chainControl) LeaderChanged(ctx context.Context, leader *pb.Node) (*emptypb.Empty, error) {
	c.leaderChanged(leader)
	return &emptypb.Empty{}, nil
}

func getNode(hostname string) pb.ReplicationProviderClient {
	conn, err := grpc.Dial(hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(errors.Join(errors.New("could not connect to node"), err))
	}
	return pb.NewReplicationProviderClient(conn)
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
	c.leader = getControllerNode(newLeader.Address + ":" + strconv.Itoa(int(newLeader.Port)))

	hbPortV, err := c.leader.GetHeartbeatEndpoint(context.Background(), &emptypb.Empty{})
	if err != nil {
		panic(errors.Join(errors.New("could not get heartbeat port from controller"), err))
	}
	hbPort := hbPortV.Value

	// Start heartbeat
	hbAddr, err := net.ResolveUDPAddr("udp", newLeader.Address+":"+strconv.Itoa(int(hbPort)))
	if err != nil {
		panic(errors.Join(errors.New("could not resolve heartbeat endpoint"), err))
	}
	hbConn, err := net.DialUDP("udp", nil, hbAddr)
	if err != nil {
		panic(errors.Join(errors.New("could not connect to heartbeat endpoint"), err))
	}
	c.leaderHb = hbConn

	//TODO: more stuff needs to happen
	fmt.Println("Leader changed to: ", newLeader.Address, ":", newLeader.Port)

	go c.heartbeat()

	c.leaderInit = true
	c.triggerInitDone()
}

func (c *chainControl) triggerInitDone() {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	if c.nextInit && c.prevInit && c.leaderInit {
		close(c.initFinish)
	}
}

func (c *chainControl) heartbeat() {
	for {
		c.mtx.RLock()
		_, err := c.leaderHb.Write([]byte("heartbeat"))
		if err != nil {
			fmt.Println("Heartbeat failed. Error: ", err)
			c.leaderHb = nil
			return
		}
		c.mtx.RUnlock()
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
