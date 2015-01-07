
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  now *View
  pending_ack *View
  ticksPrimaryCount uint
  ticksBackupCount uint
}

func IsViewEmpty(v *View) bool {
  return v.Viewnum == 0
}

func SetPrimary(primaryName string, v *View) {
    v.Viewnum++
    v.Primary = primaryName
}

func SetBackup(backupName string, v *View) {
    v.Viewnum++
    v.Backup = backupName
}

func ZeroView(v *View) {
    v.Viewnum = 0
    v.Primary = ""
    v.Backup = ""
}

func CopyView(to *View, from *View) {
    to.Viewnum = from.Viewnum
    to.Primary = from.Primary
    to.Backup = from.Backup
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  var returnView *View
  fmt.Printf("Received ping from server:%s with viewnum: %d\n", args.Me, args.Viewnum)

  if (IsViewEmpty(vs.now)) {
    fmt.Printf("This is an empty view server currently.\n")
    if (IsViewEmpty(vs.pending_ack)) {
        fmt.Printf("The pending ack view is empty too.\n")
        SetPrimary(args.Me, vs.pending_ack)
        returnView = vs.pending_ack
    } else if (vs.pending_ack.Primary == args.Me) {
        fmt.Printf("Promote the pending_ack to primary\n")
        CopyView(vs.now, vs.pending_ack)
        ZeroView(vs.pending_ack)
        returnView = vs.now
    } else {
        fmt.Printf("Can't accept more pings till the primary server responds\n")
        returnView = vs.now
    }
  } else {
    // We already have a view.
    // Is this primary pinging us?
    if (args.Me == vs.now.Primary) {
        vs.ticksPrimaryCount = 0
        returnView = vs.now
    } else if (args.Me == vs.now.Backup) {
        vs.ticksBackupCount = 0
        returnView = vs.now
    } else {
        // Let's see what we can do now
        if (vs.now.Backup == "") {
            // We can use this client as backup server
            vs.now.Viewnum++
            vs.now.Backup = args.Me
            returnView = vs.now
        }
    }
  }

  reply.View = *returnView
  fmt.Printf("Returning viewnum: %d, Primary: %s, Backup: %s\n", reply.View.Viewnum, reply.View.Primary, reply.View.Backup)
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  if (vs.pending_ack.Viewnum > vs.now.Viewnum) {
    reply.View = *vs.pending_ack
  } else {
    reply.View = *vs.now
  }

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.ticksPrimaryCount++
  vs.ticksBackupCount++
  
  if (vs.ticksPrimaryCount == DeadPings) {
    fmt.Printf("The primary server has died\n")
    vs.now.Viewnum++
    vs.now.Primary = vs.now.Backup
    vs.now.Backup = "" 
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func MakeDefaultView() *View {
  v := new(View)
  v.Viewnum = 0
  v.Primary = ""
  v.Backup = ""
  return v
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.now = MakeDefaultView()
  vs.pending_ack = MakeDefaultView()

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
