package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "errors"

import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type OpReply struct {
    err Err
    previousValue string // For Put
    value string // For Get
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  view *viewservice.View
  vsserver string
  kvstore map[string]string
  kvstoreLock sync.RWMutex
  fIsPrimary bool
  fIsBackup bool
  oldUIDs map[string]OpReply
  oldUIDsLock sync.RWMutex
}

func (pb *PBServer) PutKVStore(key string, value string) {
    pb.kvstoreLock.Lock()
    defer pb.kvstoreLock.Unlock()
    pb.kvstore[key] = value
} 

func (pb *PBServer) GetKVStore(key string) (string, bool) {
    pb.kvstoreLock.RLock()
    defer pb.kvstoreLock.RUnlock()
    value, err := pb.kvstore[key]
    return value, err
}

func (pb *PBServer) PutUIDsStore(key string, value OpReply) {
    pb.oldUIDsLock.Lock()
    defer pb.oldUIDsLock.Unlock()
    pb.oldUIDs[key] = value
}

func (pb *PBServer) GetUIDStore(key string) (OpReply, bool) {
    pb.oldUIDsLock.RLock()
    defer pb.oldUIDsLock.RUnlock()
    op, err := pb.oldUIDs[key]
    return op, err
}

func (pb *PBServer) PutInt(args *PutArgs) (string, Err) {
    if args.DoHash {
        previousvalue, err := pb.GetInt(args.Key)
        if err == ErrNoKey {
            previousvalue = ""
        }
        hash_args := previousvalue + args.Value
        DPrintf("Computing hash value of %s\n", hash_args)
        hash_val := hash(hash_args)
        hash_val_str := strconv.Itoa(int(hash_val))
        DPrintf("Computed hash value: %s\n", hash_val_str)
        pb.PutKVStore(args.Key, hash_val_str)
        return previousvalue, OK
    } else {
        pb.PutKVStore(args.Key, args.Value)
        return "", OK
    }
}

func (pb *PBServer) UIDExists(uid_str string) bool {
    _, ok := pb.GetUIDStore(uid_str)
    return ok
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
    DPrintf("Server:%s received Put of: key = %s\tvalue = %s\n", pb.me, args.Key, args.Value)
    if pb.UIDExists(args.UID_str) {
        //DPrintf("Server: %s, Put operation. Received duplicate: %s. Bailing out\n", pb.me, args.UID_str)
        reply.Err = pb.oldUIDs[args.UID_str].err
        reply.PreviousValue = pb.oldUIDs[args.UID_str].previousValue
        return nil
    }

    if pb.fIsPrimary {
        if args.PrimaryCaller {
            reply.Err = ErrWrongServer
        } else {
            if pb.view.Backup != "" {
                args_fwd := *args
                args_fwd.PrimaryCaller = true
                ok := call(pb.view.Backup, "PBServer.Put", &args_fwd, reply)
                if ok {
                    if reply.Err == ErrWrongServer {
                        DPrintf("Received ErrWrongServer from the backup\n")
                        return nil
                    } else {
                        reply.PreviousValue, reply.Err = pb.PutInt(args)
                    }
                } else {
                    // TODO: we will add retry logic here
                    return errors.New("Call to backup failed\n")
                }
            } else {
                reply.PreviousValue, reply.Err = pb.PutInt(args)
            }
        }
    } else if pb.fIsBackup{
        if args.PrimaryCaller {
            reply.PreviousValue, reply.Err = pb.PutInt(args)
        } else {
            reply.Err = ErrWrongServer
        }
    } else {
        reply.Err = ErrWrongServer
    }

    if reply.Err == OK {
        op_reply := OpReply{}
        op_reply.err = reply.Err
        op_reply.previousValue = reply.PreviousValue
        pb.PutUIDsStore(args.UID_str, op_reply)
    }

    return nil
}

func (pb *PBServer) GetInt(key string) (string, Err) {
    var err Err 
    err = OK
    value, ok := pb.GetKVStore(key)
    if !ok {
        err = ErrNoKey
    }
    return value, err
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    // Your code here.
    DPrintf("Server:%s received Get of: key = %s\n", pb.me, args.Key)
    if pb.UIDExists(args.UID_str) {
        //DPrintf("Server: %s, Put operation. Received duplicate: %s. Bailing out\n", pb.me, args.UID_str)
        reply.Err = pb.oldUIDs[args.UID_str].err
        reply.Value = pb.oldUIDs[args.UID_str].value
        return nil
    }

    reply.Err = ErrNoKey
    if pb.fIsPrimary {
        if args.PrimaryCaller {
            reply.Err = ErrWrongServer
        } else {
            if pb.view.Backup != "" {
                args_fwd := *args
                args_fwd.PrimaryCaller = true
                ok := call(pb.view.Backup, "PBServer.Get", args_fwd, reply)
                if ok {
                    if reply.Err == ErrWrongServer {
                        DPrintf("Received ErrWrongServer from the backup\n")
                        return errors.New("Get: Received ErrWrongServer from the backup\n")
                    }
                } else {
                    return errors.New("Get: Call to backup failed\n")
                }
            } else {
                reply.Value, reply.Err = pb.GetInt(args.Key)
            }
        }
    } else if pb.fIsBackup {
        if args.PrimaryCaller {
            reply.Value, reply.Err = pb.GetInt(args.Key)
        } else {
            reply.Err = ErrWrongServer
        }
    } else {
        reply.Err = ErrWrongServer
    }

    if reply.Err == OK || reply.Err == ErrNoKey {
        op_reply := OpReply{}
        op_reply.err = reply.Err
        op_reply.value = reply.Value
        pb.PutUIDsStore(args.UID_str, op_reply)
    }

    return nil
}

func PrettyPrintView(view *viewservice.View) string {
    return fmt.Sprintf("View num:%d, Primary:%s, Backup:%s", view.Viewnum, view.Primary, view.Backup);
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
    args := &viewservice.PingArgs{}
    args.Me = pb.me
    args.Viewnum = pb.view.Viewnum
    var reply viewservice.PingReply
    ok := call(pb.vsserver, "ViewServer.Ping", args, &reply)
    if ok {
        DPrintf("Received view from the viewserver:%s\n", PrettyPrintView(&reply.View))
        // Check for the case when the server is already aware of being Primar,
        // a new Backup has been added
        fViewHasNewBackup := pb.fIsPrimary && (reply.View.Primary == pb.me) && (pb.view.Backup != reply.View.Backup) && (reply.View.Backup != "")

        if fViewHasNewBackup {
            transfer_args := TransferArgs{}
            transfer_args.KeyValueStore = pb.kvstore
            var transfer_reply TransferReply
            transfer_ok := call(reply.View.Backup, "PBServer.TransferBulk", &transfer_args, &transfer_reply)
            fmt.Printf("Finished calling TransferBulk with: %d\n", transfer_ok)
            if transfer_ok {
                viewservice.CopyView(pb.view, &reply.View) 
                pb.fIsPrimary = (pb.view.Primary == pb.me)
                pb.fIsBackup = (pb.view.Backup == pb.me)
            }
        } else { 
            viewservice.CopyView(pb.view, &reply.View) 
            pb.fIsPrimary = (pb.view.Primary == pb.me)
            pb.fIsBackup = (pb.view.Backup == pb.me)
        }
    }
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}

func (pb *PBServer) TransferBulk(args *TransferArgs, reply *TransferReply) error {
    DPrintf("Received a transfer bulk of %d kv-pairs\n", len(args.KeyValueStore))
    for key, value := range args.KeyValueStore {
        DPrintf("TransferBulk: %s\t%s\n", key, value)
        pb.PutKVStore(key, value)
    }

    reply.Err = "OK"
    return nil
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.MakeDefaultView()
  pb.vsserver = vshost
  pb.kvstore = make(map[string]string)
  pb.oldUIDs = make(map[string]OpReply)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            DPrintf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        DPrintf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
