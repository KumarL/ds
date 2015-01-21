
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
  pending_newprimary *View
  fAwaitingNewPrimaryAck bool
  fPrimaryAcked bool
  ticksPrimaryCount uint
  ticksBackupCount uint
  ticksIdleServerCount map[string]uint
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
    vs.now.Viewnum++
    vs.now.Primary = args.Me
    vs.fPrimaryAcked = false
    returnView = vs.now
  } else {
    // We already have a view.
    // Is this primary pinging us?

    if (vs.fAwaitingNewPrimaryAck && vs.pending_newprimary.Primary == args.Me) {
        // We want to promote backup to be the new primary now
        if (vs.pending_newprimary.Viewnum - 1 == args.Viewnum) {
            fmt.Printf("Received ping from server that we want to promote as primary\n")
            returnView = vs.pending_newprimary
        } else if (vs.pending_newprimary.Viewnum == args.Viewnum) {
            fmt.Printf("Primary has accepted requested to be primary\n")
            CopyView(vs.now, vs.pending_newprimary)
            ZeroView(vs.pending_newprimary)
            vs.fAwaitingNewPrimaryAck = false
            returnView = vs.now
        } else {
            returnView = vs.now
        }
    } else if (args.Me == vs.now.Primary) {
        // Is all well with Primary?
        if (args.Viewnum == vs.now.Viewnum) {
            vs.ticksPrimaryCount = 0
            vs.fPrimaryAcked = true
            returnView = vs.now
        } else if (args.Viewnum < vs.now.Viewnum) {
            fmt.Printf("Primary is behind %d view. Send it the latest view\n", vs.now.Viewnum - args.Viewnum)
            returnView = vs.now
        } else if (args.Viewnum == 0) {
            fmt.Printf("Primary sent us a distress signal. It probably crashed and restarted\n")
            vs.now.Viewnum++
            vs.now.Primary = vs.now.Backup
            vs.now.Backup = args.Me
            vs.fPrimaryAcked = false
            returnView = vs.now
        }
    } else if (args.Me == vs.now.Backup) {
           vs.ticksBackupCount = 0
           returnView = vs.now
    } else {
        // Let's see what we can do now
        if (vs.now.Backup == "") {
            // We can use this client as backup server
            vs.now.Viewnum++
            vs.now.Backup = args.Me
            vs.fPrimaryAcked = false
            returnView = vs.now
        } else if (vs.pending_newprimary.Primary != args.Me && vs.pending_newprimary.Backup != args.Me) {
            // add it to the list of idle servers
            fmt.Printf("Received tick from an idle server: %s\n", args.Me)
            vs.ticksIdleServerCount[args.Me] = 0
            returnView = vs.now
        } else {
            returnView = vs.now
        }
    }
  }

  if returnView == nil {
    fmt.Printf("Nothing to pass to server. Going to crash\n")
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
  reply.View = *vs.now

  return nil
}

func (vs *ViewServer) GetLiveIdleServer() string {
  returnVal := ""
  for key, value := range vs.ticksIdleServerCount {
   if (value < DeadPings) {
     returnVal = key
   }
  }
  return returnVal
}

func (vs *ViewServer) ReplaceBackupWithIdleServer() {
    possibleIdleServer := vs.GetLiveIdleServer()
    if (possibleIdleServer != "") {
        fmt.Printf("Found an idle server to replace backup: %s\n", possibleIdleServer)
        delete(vs.ticksIdleServerCount, possibleIdleServer)
        vs.fPrimaryAcked = false
    }
    vs.now.Backup = possibleIdleServer
}

func (vs *ViewServer) DropPrimaryAndPromoteBackup() {
    possibleIdleServer := vs.GetLiveIdleServer()
/*
    if (possibleIdleServer != "") {
        vs.pending_newprimary.Primary = vs.now.Backup
        fmt.Printf("Found an idle server to replace backup: %s\n", possibleIdleServer)
        delete(vs.ticksIdleServerCount, possibleIdleServer)
        vs.pending_newprimary.Backup = possibleIdleServer
        vs.pending_newprimary.Viewnum = vs.now.Viewnum
        vs.pending_newprimary.Viewnum++
        fmt.Printf("Awaiting ack of new view: %d\n", vs.pending_newprimary.Viewnum)
        vs.fAwaitingNewPrimaryAck = true
*/
    if (vs.fPrimaryAcked) {
        fmt.Printf("Moving the view ahead since Primary had acknowledged\n")
        vs.now.Viewnum++
        vs.now.Primary = vs.now.Backup
        vs.now.Backup = possibleIdleServer
        if (possibleIdleServer != "") {
            delete(vs.ticksIdleServerCount, possibleIdleServer)
        }
        vs.fPrimaryAcked = false
    }
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
    vs.DropPrimaryAndPromoteBackup()
  } else if (vs.ticksBackupCount == DeadPings) {
    fmt.Printf("The backup server has died.\n")
    vs.now.Viewnum++
    vs.ReplaceBackupWithIdleServer()
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
  vs.pending_newprimary = MakeDefaultView()
  vs.fAwaitingNewPrimaryAck = false
  vs.ticksIdleServerCount = make(map[string]uint)

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
