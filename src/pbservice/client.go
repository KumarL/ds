package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  view *viewservice.View
  vshost string
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  ck.view = viewservice.MakeDefaultView()
  ck.vshost = vshost

  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

    // Your code here.
    shouldAttempt := true
    firstAttempt := true
    uid := ck.GetUID()

    retval := ""
    for shouldAttempt {
        if !firstAttempt || ck.view.Viewnum == 0 {
            ck.FindView()
        }

        args := GetArgs{}
        args.Key = key
        args.PrimaryCaller = false
        args.UID_str = uid.String()

        var reply GetReply

        success := call(ck.view.Primary, "PBServer.Get", &args, &reply)
        if success && (reply.Err == OK || reply.Err == ErrNoKey) {
            retval = reply.Value
            shouldAttempt = false
        }

        //fmt.Printf("Clerk's Get call received Err: %s\n", reply.Err)
        firstAttempt = false
        time.Sleep(viewservice.PingInterval)
    }

    return retval
}

func (ck *Clerk) FindView() {
    args := viewservice.GetArgs{}

    var reply viewservice.GetReply
    ok := call(ck.vshost, "ViewServer.Get", &args, &reply)
    if ok {
        viewservice.CopyView(ck.view, &reply.View)
    }
}

func (ck *Clerk) GetUID() *big.Int {
    var n *big.Int
    max := *big.NewInt(99999999999)
    n, _ = rand.Int(rand.Reader, &max)
    return n
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

  // Your code here.
  shouldAttempt := true
  firstAttempt := true
  uid := ck.GetUID()

  retval := ""
  for shouldAttempt {
        if !firstAttempt || ck.view.Viewnum == 0 {
            ck.FindView()
        }
        
        args := PutArgs{}
        args.Key = key
        args.Value = value
        args.DoHash = dohash
        args.PrimaryCaller = false
        args.UID_str = uid.String()

        var reply PutReply

        success := call(ck.view.Primary, "PBServer.Put", &args, &reply)
        if success && reply.Err == OK {
            retval = reply.PreviousValue
            shouldAttempt = false
        }
        //fmt.Printf("Clerk's Put call received Err: %s\n", reply.Err)
        firstAttempt = false
        time.Sleep(viewservice.PingInterval)
  }

  return retval
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
