package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op      string // "Get" or "Put" or "Append"
	Key     string
	Value   string
	Ck      int64
	CkIndex int
}

type OpRes struct {
	index     int
	term      int
	replyCond chan string
	err       Err
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	appliedId int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	resQueue []*OpRes
	ckLastId map[int64]int
	ckIdStat map[int64]map[int]bool // state of PutAppend RPC. for those index less then lastId, true for haven't received, false (missing) in the map for process done
	KVdata   map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Op = "Get"
	op.Key = args.Key
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.replyCond = make(chan string)
	kv.resQueue = append(kv.resQueue, &opReply)
	kv.mu.Unlock()

	DPrintf("[ServerGet]\t%d received Get id=%d, term=%d; for key=%v", kv.me, index, term, args.Key)
	reply.Value = <-opReply.replyCond
	reply.Err = opReply.err
	reply.ServerId = kv.me
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Op = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Ck = args.Ck
	op.CkIndex = args.Index
	kv.mu.Lock()

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	opReply := OpRes{}
	opReply.index = index
	opReply.term = term
	opReply.replyCond = make(chan string)
	kv.resQueue = append(kv.resQueue, &opReply)
	kv.mu.Unlock()
	DPrintf("[ServerPutAppend]\t%d received PutAppend from %d, ckid=%d, id=%d, term=%d; for %v key=%v, value=%v",
		kv.me, args.Ck%23, args.Index, index, term, args.Op, args.Key, args.Value)

	<-opReply.replyCond
	reply.Err = opReply.err
	reply.ServerId = kv.me
	DPrintf("[ServerPutAppendDone]\t%d received from %d, ckid=%d, id=%d, term=%d; for %v key=%v, value=%v",
		kv.me, args.Ck%23, args.Index, index, term, args.Op, args.Key, args.Value)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) coordinator() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.SnapshotValid && msg.Snapshot != nil && len(msg.Snapshot) > 0 {
			snapshot := msg.Snapshot
			r := bytes.NewBuffer(snapshot)
			d := labgob.NewDecoder(r)
			var ckLastId map[int64]int
			var ckIdStat map[int64]map[int]bool
			var KVdata map[string]string
			if d.Decode(&ckLastId) != nil ||
				d.Decode(&ckIdStat) != nil ||
				d.Decode(&KVdata) != nil {
				//   error...
				log.Fatalf("kv missing snapshot")
			} else {
				kv.ckLastId = ckLastId
				kv.ckIdStat = ckIdStat
				kv.KVdata = KVdata
			}
			DPrintf("[ApplySnapshot]\t%d applying snapshot id=%d, term=%d", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.appliedId = msg.SnapshotIndex
			for len(kv.resQueue) > 0 {
				kv.resQueue[0].err = ErrTryAgain
				if msg.SnapshotIndex < kv.resQueue[0].index {
					break
				}
				go func(reply *OpRes, res string) {
					// DPrintf("[ServerReply]\t%d replying index=%d, term=%d", kv.me, reply.index, reply.term)
					reply.replyCond <- res
					DPrintf("[ServerDropReply]\t%d dropped reply index=%d, term=%d", kv.me, reply.index, reply.term)
				}(kv.resQueue[0], "")
				kv.resQueue = kv.resQueue[1:]
			}
		}
		if msg.CommandValid {
			value, ok := "", false
			DPrintf("[MsgApply]\t%d applying id=%d, term=%d", kv.me, msg.CommandIndex, msg.CommandTerm)
			kv.appliedId++
			if msg.CommandIndex != kv.appliedId {
				log.Fatalf("server %d should apply %d, but it was applying %d", kv.me, kv.appliedId, msg.CommandIndex)
			}
			cmd := msg.Command.(Op)
			op := cmd.Op
			if op == "Get" {
				value, ok = kv.KVdata[cmd.Key]
			} else {
				// check whether PutAppend are repeated
				repeated := false
				if cmd.CkIndex <= kv.ckLastId[cmd.Ck] {
					if !kv.ckIdStat[cmd.Ck][cmd.CkIndex] {
						repeated = true
					} else {
						delete(kv.ckIdStat[cmd.Ck], cmd.CkIndex)
					}
				} else {
					if _, ok := kv.ckIdStat[cmd.Ck]; !ok {
						kv.ckIdStat[cmd.Ck] = make(map[int]bool)
					}
					for i := kv.ckLastId[cmd.Ck] + 1; i < cmd.CkIndex; i++ {
						kv.ckIdStat[cmd.Ck][i] = true
					}
					kv.ckLastId[cmd.Ck] = cmd.CkIndex
				}
				if !repeated {
					switch op {
					case "Put":
						kv.KVdata[cmd.Key] = cmd.Value
					case "Append":
						kv.KVdata[cmd.Key] += cmd.Value
					}
				}
			}
			for len(kv.resQueue) > 0 {
				kv.resQueue[0].err = ErrTryAgain
				if msg.CommandIndex < kv.resQueue[0].index {
					break
				} else if msg.CommandIndex == kv.resQueue[0].index && msg.CommandTerm == kv.resQueue[0].term {
					kv.resQueue[0].err = OK
					if op == "Get" && !ok {
						kv.resQueue[0].err = ErrNoKey
					}
				}
				go func(reply *OpRes, res string) {
					// DPrintf("[ServerReply]\t%d replying index=%d, term=%d", kv.me, reply.index, reply.term)
					reply.replyCond <- res
					DPrintf("[ServerReplyDone]\t%d replyed index=%d, term=%d", kv.me, reply.index, reply.term)
				}(kv.resQueue[0], value)
				kv.resQueue = kv.resQueue[1:]
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				// take snapshot
				index := msg.CommandIndex
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.ckLastId)
				e.Encode(kv.ckIdStat)
				e.Encode(kv.KVdata)
				snapshot := w.Bytes()
				kv.rf.Snapshot(index, snapshot)
			}
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ckLastId = make(map[int64]int)
	kv.ckIdStat = make(map[int64]map[int]bool)
	kv.KVdata = make(map[string]string)

	// You may need initialization code here.
	go kv.coordinator()

	return kv
}