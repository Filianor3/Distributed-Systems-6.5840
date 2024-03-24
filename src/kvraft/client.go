package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	me         int64
	lastLeader int
	lastIndex  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	DPrintf("[NewCk]\t%d, %d", ck.me, ck.me%23)
	return ck
}

type GetRes struct {
	ok    bool
	value string
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	value := ""
	args := GetArgs{}
	// args.Ck = ck.me
	args.Key = key
	ck.mu.Lock()
	// ck.lastIndex++
	// args.Index = ck.lastIndex
	lastLeader := ck.lastLeader
	serverNum := len(ck.servers)
	ck.mu.Unlock()
	for cont := true; cont; {
		done := make(chan GetRes)
		quit := make(chan bool)
		go func(leader int, args GetArgs, done chan GetRes, quit chan bool) {
			res := GetRes{}
			reply := GetReply{}
			ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
			// if reply.Err == ErrNoKey || reply.Err == OK {
			DPrintf("[CkGet]\tck %d to server %d, ok=%v, key=%v, Err=%v, value=%v", ck.me%23, reply.ServerId, ok, key, reply.Err, reply.Value)
			// }
			if reply.Err == ErrNoKey {
				res.ok = true
			} else if reply.Err == OK {
				res.ok = true
				res.value = reply.Value
			} // else ErrTryAgain
			select {
			case done <- res:
				return
			case <-quit:
				return
			}
		}(lastLeader, args, done, quit)
		select {
		case res := <-done:
			value = res.value
			cont = !res.ok
		case <-time.After(time.Duration(100) * time.Millisecond):
			close(quit)
		}
		if cont {
			lastLeader = (lastLeader + 1) % serverNum
		}
	}
	ck.mu.Lock()
	ck.lastLeader = lastLeader
	ck.mu.Unlock()
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Ck = ck.me
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	ck.lastIndex++
	args.Index = ck.lastIndex
	lastLeader := ck.lastLeader
	serverNum := len(ck.servers)
	ck.mu.Unlock()
	for cont := true; cont; {
		done := make(chan bool)
		quit := make(chan bool)
		go func(leader int, args PutAppendArgs, done chan bool, quit chan bool) {
			reply := PutAppendReply{}
			ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
			// if reply.Err == OK {
			DPrintf("[CkPutAppend]\tck %d to server %d, ckId=%v, ok=%v, Err=%v, key=%v, val=%v, op=%v",
				ck.me%23, reply.ServerId, args.Index, ok, reply.Err, key, value, op)
			// }
			select {
			case done <- (reply.Err == OK):
				return
			case <-quit:
				return
			}
		}(lastLeader, args, done, quit)
		select {
		case ok := <-done:
			cont = !ok
		case <-time.After(time.Duration(100) * time.Millisecond):
			close(quit)
		}
		if cont {
			lastLeader = (lastLeader + 1) % serverNum
		}
	}
	ck.mu.Lock()
	ck.lastLeader = lastLeader
	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}