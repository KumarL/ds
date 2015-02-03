package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "math"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  seqProposerStates map[int]PeerState
  seqAcceptorStates map[int]PeerState
  decidedStates map[int]DecidedState
}

type PeerState struct {
    n_p int // proposer's highest number seen so far
    n_a int // acceptor's highest number received so far
    v_a interface{} // value that proposer wants to proposer.
}
    
type PrepareArg struct {
    s int // paxos instance sequence number
    n int // highest number that proposer has seen so far
}

type PrepareReply struct {
    ignore bool
    s int // paxos instance sequence number
    n_a int // Acceptor's highest number that it has seen
    v_a interface {} // Acceptor's accepted value
}

type AcceptArg struct {
    s int // paxos instance sequence number
    n int // n_p
    v interface {} // value from the highest recieved n_a
}

type AcceptReply struct {
    s int // paxos instance sequence number
    accepted bool
}

type DecidedArg struct {
    s int
    n int
    v inteface{}
}

type DecidedReply struct {
    result bool
}

type DecidedState struct {
    decided bool
    v interface{}
}    

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) IsMajority(n int) bool {
    return n >= (math.Floor(n/2) + 1)
}

func (px *Paxos) Prepare(seq int, n_p int) (bool, int, inteface{}) {
    result := false
    n_a := math.MinInt32
    var v_a interface{}

    arg := PrepareArg{}
    arg.s = seq
    arg.n = n_p

    var reply PrepareReply
    num_responses_received := 0

    var mutex  = &sync.Mutex{}

    var wg sync.WaitGroup
    for server := range px.peers {
        wg.Add(1)
        go func() {
            ok := call(server, "Paxos.PrepareHandler", &arg, &reply)
            if ok && !reply.ignore {
                num_responses_received++
                if reply.n_a > n_a {
                    mutex.Lock()
                    if reply.n_a > n_a {
                        n_a = reply.n_a
                        v_a = reply.v_a
                    }
                    mutex.Unlock()
                }
            }
        }()
    }
    wg.Wait()

    if px.IsMajority(num_responses_received) {
        // We received responses from a majority of acceptors
        // We can accept what they sent us back
        result = true
    }

    return result, n_a, v_a
}

func (px *Paxos) UpdateAcceptorState(state *PeerState) {
    acceptorStateLock.Lock()
    px.seqAcceptorStates[arg.s] = *acceptorState
    acceptorStateLock.Unlock()
}

func (px *Paxos) PrepareHandler(arg *PrepareArg, reply *PrepareReply) {
    acceptorState, ok = px.seqAcceptorStates[arg.s]
    reply.ignore = true

    if ok {
        // Is this number actually greater that existing accepted state
        if arg.n > acceptorState.n_p {
            // update the local state
            acceptorState.n_p = arg.n
            UpdateAcceptorState(&acceptorState)

            reply.ignore = false
            reply.s = arg.s
            reply.n_a = arg.peerState.n_a
            reply.v_a = arg.peerState.v_a
        }
    } else {
        acceptorState = px.PeerState{}
        acceptorState.n_p = arg.n
        acceptorState.n_a = -1
        UpdateAcceptorState(&acceptorState)

        reply.ignore = false
        reply.s = arg.s
        reply.n_a = acceptorState.n_a
    }
}

func (px *Paxos) Accept(seq int, n int, v interface{}) bool {
    result := false

    arg := AcceptArg{}
    arg.s = seq
    arg.n = n
    arg.v = v

    var reply AcceptReply

    var mutex = &sync.Mutex{}

    var wg sync.WaitGroup
    for server := range px.peers {
        wg.Add(1)
        go func() {
            ok := call(server, "Paxos.AcceptHandler", &arg, &reply)
            if ok && arg.accepted {
                mutex.Lock()
                num_responses_received++
                mutex.Unlock()
            }
        }()
    }
    wg.Wait()

    return paxos.IsMajority(num_responses_received)
}

func (px *Paxos) AcceptHandler(arg *AcceptArg, reply *AcceptReply) {
    reply.accepted = false

    acceptorState, ok := px.seqAcceptorStates[arg.s]

    if !ok {
        acceptorState = &px.PeerState{}
        acceptorState.n_p = -1
    }

    if arg.n >= acceptorState.n_p {
        acceptorState.n_p = arg.n
        acceptorState.n_a = arg.n
        acceptorState.v = arg.v
        UpdateAcceptorState(&acceptorState)
        reply.accepted = true
    }
}

func (px *Paxos) Decided(seq int, n int, v interface{}) bool {
    decided := DecidedArg{}
    decided.s = seq
    decided.n = n
    decided.v = v

    for server := range px.peers {
        go func() {
            var reply DecidedReply
            ok := call(server, "Paxos.DecidedHandler", &decided, &reply)
        }()
    }
    return true
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
    go func(seq int, v interface{}) {
        proposerState, ok := px.seqProposerStates[seq]

        if !ok {
            proposerState.n_p = -1
            proposerState.n_a = -1
        }

        result := false
        for !result {
            result, n_a, v_a := px.Prepare(seq, proposerState.n_p)
        }

        // The proposal has been accepted
        accepted := px.Accept(seq, n_a, v_a)
        if accepted {
            px.Decided(seq, n_a, v_a)
        }
    }(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
    delete(px.DecidedState, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
    // Your code here.
    seq := -1
    for decided_seq := range px.DecidedState {
        if decided_seq > seq {
            seq = decided_seq
        }
    }
    return seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
    min_seq := math.MaxInt32
    for seq := range px.DecidedState {
        if seq < min_seq {
            min_seq = seq
        }
    }
    return min_seq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
    // Your code here.
    decidedState, decided := px.DecidedState[seq]
    v = nil

    if ok {
        v = decidedState.v
    }

    return decided, v
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.seqProposerStates = make(map[int]PeerState)
  px.seqAcceptorStates = make(map[int]PeerState)
  px.decidedStates = make(map[int]DecidedState)
  px.acceptorStateLock = &sync.Mutex{}

  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
