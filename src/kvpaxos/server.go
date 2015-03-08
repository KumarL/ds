package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "container/list"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT     = "Put"
	GET     = "Get"
	PUTHASH = "PutHash"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action   string
	Putargss PutArgs
	Getargss GetArgs
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	lastSequenceNumber     int
	lastSequenceNumberJump int
	lastSequenceNumberLock sync.RWMutex

	servers []string

	logLock sync.RWMutex
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{}
	op.Action = GET
	op.Getargss = *args

	seq := kv.GetNextSequenceNumber()
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		decided, _ := kv.px.Status(seq)
		if decided {
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

	reply.Value = kv.GetLastPutLog(args.Key, kv.px.GetOpLogs())
	reply.Err = ""

	return nil
}

func (kv *KVPaxos) GetLastPutLog(searchkey string, log *list.List) string {
	value := ""

	DPrintf("[%s] GetLastPutLog: Searching for the key: %d", kv.servers[kv.me], searchkey)

	for e := log.Back(); e != nil; e = e.Prev() {
		op, ok := e.Value.(Op)
		if ok && op.Action == PUT && op.Putargss.Key == searchkey {
			value = op.Putargss.Value
			break
		}
	}

	return value
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	op := Op{}
	op.Action = PUT
	op.Putargss = *args

	if args.DoHash {
		reply.PreviousValue = kv.GetLastPutLog(args.Key, kv.px.GetOpLogs())
	}

	seq := kv.GetNextSequenceNumber()
	DPrintf("[%s] Put: Starting a sequence=%d with %s operation\n", kv.servers[kv.me], seq, PUT)
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		decided, _ := kv.px.Status(seq)
		if decided {
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

	DPrintf("[%s] Put: Operation for key=%s, value=%s agreed\n", kv.servers[kv.me], args.Key, args.Value)

	if op.Action != PUT {
		DPrintf("[%s] Put: Decided value is not the same as requested\n")
	} else {
		reply.Err = ""
	}

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) GetNextSequenceNumber() int {
	defer kv.lastSequenceNumberLock.Unlock()
	kv.lastSequenceNumberLock.Lock()

	kv.lastSequenceNumber += kv.lastSequenceNumberJump
	return kv.lastSequenceNumber
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.lastSequenceNumber = me
	kv.lastSequenceNumberJump = len(servers)

	kv.servers = servers

	kv.logLock = sync.RWMutex{}
	/*
		kv.log = list.New()
		kv.logLock = sync.RWMutex{}
	*/
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
