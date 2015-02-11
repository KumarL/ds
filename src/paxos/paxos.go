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
  acceptorStateLock sync.Mutex
}

type PeerState struct {
    N_P int // proposer's highest number seen so far
    N_A int // acceptor's highest number received so far
    V_A interface{} // value that proposer wants to proposer.
}
    
type PrepareArg struct {
    S int // paxos instance sequence number
    N int // highest number that proposer has seen so far
}

type PrepareReply struct {
    Ignore bool
    S int // paxos instance sequence number
    N_A int // Acceptor's highest number that it has seen
    V_A interface{} // Acceptor's accepted value
}

type AcceptArg struct {
    S int // paxos instance sequence number
    N int // n_p
    V interface{} // value from the highest recieved n_a
}

type AcceptReply struct {
    S int // paxos instance sequence number
    Accepted bool
}

type DecidedArg struct {
    S int
    N int
    V interface{}
}

type DecidedReply struct {
    Result bool
}

type DecidedState struct {
    Decided bool
    V interface{}
}    

const InitialN_A = -1

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
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
    return n >= (n/2 + 1)
}

func (px *Paxos) Prepare(seq int, n_p int, self_value interface{}) (bool, int, interface{}) {
    result := false
    n_a := math.MinInt32
    var v_a interface{}

    arg := PrepareArg{}
    arg.S = seq
    arg.N = n_p

    var reply PrepareReply
    num_responses_received := 0

    var mutex  = &sync.Mutex{}

    var wg sync.WaitGroup
    for idx := range px.peers {
        server := px.peers[idx]
        wg.Add(1)
        go func() {
            defer wg.Done()
            ok := call(server, "Paxos.PrepareHandler", &arg, &reply)
            DPrintf("[%s] PrepareHandler returned from server=[%s] with ok=[%d], Ignore=[%d], N_A=[%d], V_A=[%s]\n", px.peers[px.me], server, ok, reply.Ignore, reply.N_A, reply.V_A)
            if ok && !reply.Ignore {
                num_responses_received++
                if reply.N_A > n_a {
                    mutex.Lock()
                    if reply.N_A > n_a {
                        n_a = reply.N_A
                        if n_a == InitialN_A {
                            v_a = self_value
                        } else {
                            v_a = reply.V_A
                        }
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

    DPrintf("[%s] Prepare returning result=%d, n_a = %d, v_a = %d\n", px.peers[px.me], result, n_a, v_a)
    return result, n_a, v_a
}

func (px *Paxos) UpdateAcceptorState(seq int, state *PeerState) {
    px.acceptorStateLock.Lock()
    px.seqAcceptorStates[seq] = *state
    px.acceptorStateLock.Unlock()
}

func (px *Paxos) PrepareHandler(arg *PrepareArg, reply *PrepareReply) error {
    acceptorState, ok := px.seqAcceptorStates[arg.S]
    reply.Ignore = true

    if ok {
        // Is this number actually greater that existing accepted state
        if arg.N > acceptorState.N_P {
            // update the local state
            DPrintf("[%s] PrepareHandler: Local n_p=[%d] state.", px.peers[px.me], arg.N)
            acceptorState.N_P = arg.N
            px.UpdateAcceptorState(arg.S, &acceptorState)

            reply.Ignore = false
            reply.S = arg.S
            reply.N_A = acceptorState.N_A
            reply.V_A = acceptorState.V_A
        }
    } else {
        DPrintf("[%s] PrepareHandler: Initialize n_p state.\n", px.peers[px.me]) 
        acceptorState := PeerState{}
        acceptorState.N_P = arg.N
        acceptorState.N_A = InitialN_A
        px.UpdateAcceptorState(arg.S, &acceptorState)

        reply.Ignore = false
        reply.S = arg.S
        reply.N_A = acceptorState.N_A
    }
    return nil
}

func (px *Paxos) Accept(seq int, n int, v interface{}) bool {
    arg := AcceptArg{}
    arg.S = seq
    arg.N = n
    arg.V = v

    var reply AcceptReply

    num_responses_received := 0
    mutex := &sync.Mutex{}

    var wg sync.WaitGroup
    for idx := range px.peers {
        server := px.peers[idx]
        wg.Add(1)
        go func() {
            defer wg.Done()
            ok := call(server, "Paxos.AcceptHandler", &arg, &reply)
            DPrintf("[%s] AcceptHandler returned with accepted=%d\n", px.peers[px.me], reply.Accepted)
            if ok && reply.Accepted {
                mutex.Lock()
                num_responses_received++
                mutex.Unlock()
            }
        }()
    }
    wg.Wait()

    return px.IsMajority(num_responses_received)
}

func (px *Paxos) AcceptHandler(arg *AcceptArg, reply *AcceptReply) error {
    reply.Accepted = false

    acceptorState, ok := px.seqAcceptorStates[arg.S]

    if !ok {
        acceptorState := &PeerState{}
        acceptorState.N_P = -1
    }

    if arg.N >= acceptorState.N_P {
        acceptorState.N_P = arg.N
        acceptorState.N_A = arg.N
        acceptorState.V_A = arg.V
        px.UpdateAcceptorState(arg.S, &acceptorState)
        reply.Accepted = true
    }
    return nil
}

func (px *Paxos) Decided(seq int, n int, v interface{}) bool {
    decided := DecidedArg{}
    decided.S = seq
    decided.N = n
    decided.V = v

    for idx := range px.peers {
        server := px.peers[idx]
        go func() {
            var reply DecidedReply
            call(server, "Paxos.DecidedHandler", &decided, &reply)
        }()
    }
    return true
}

func (px *Paxos) DecidedHandler(arg *DecidedArg, reply *DecidedReply) error {
    DPrintf("[%s] Starting DecidedHandler:\n", px.peers[px.me])
    decidedState, ok := px.decidedStates[arg.S]

    if !ok {
        decidedState = DecidedState{}
    }

    decidedState.Decided = true
    decidedState.V = arg.V
    px.decidedStates[arg.S] = decidedState

    reply.Result = true
    return nil
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
            proposerState.N_P = -1
            proposerState.N_A = -1
        }

        result := false
        n_a := -1
        var v_a interface{}

        for !result {
            result, n_a, v_a = px.Prepare(seq, proposerState.N_P, v)
        }

        DPrintf("[%s] Results received from Start: result=[%d], n_a=[%d], v_a=[%d]\n", px.peers[px.me], result, n_a, v_a)

        // The proposal has been accepted
        accepted := px.Accept(seq, n_a, v_a)
        DPrintf("[%s] Received %d from Accept\n", px.peers[px.me], accepted)
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
    delete(px.decidedStates, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
    // Your code here.
    seq := -1
    for decided_seq := range px.decidedStates {
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
    for seq := range px.decidedStates {
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
    decidedState, decided := px.decidedStates[seq]
    var v interface{}

    if decided {
        v = decidedState.V
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
  px.acceptorStateLock = sync.Mutex{}

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
