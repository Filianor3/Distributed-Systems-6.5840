package shardkv

import (
	"6.5840/shardctrler"
	"strconv"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type CommandType string

const (
	GetCommand          CommandType = "Get"
	PutCommand          CommandType = "Put"
	AppendCommand       CommandType = "Append"
	UpdateConfigCommand CommandType = "Updtcmd"
	MergeDBCommand      CommandType = "MergeDB"
)

const Debug = true
const SHOW_BIT = 100000

var debugStart time.Time


func DeepCopyMap(db map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range db {
		res[k] = v
	}
	return res
}

func DeepCopyList(ls []int) []int {
	var res []int
	for _, v := range ls {
		res = append(res, v)
	}
	return res
}

func ListTostring(ls []int) string {
	res := ""
	for _, v := range ls {
		res = res + strconv.Itoa(v) + " "
	}
	return res
}

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID int64 // for duplicate detection
}

type PutAppendReply struct {
	Err Err
	ID  int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID int64
}

type GetReply struct {
	Err   Err
	Value string
	ID    int64
}

type PullShardArgs struct {
	Gid    int // sender's gid
	Shard  int // request shard number
	MyGen  int // my generation for pull
	Server int // puller's id
}

type PullShardReply struct {
	Err      Err
	Database map[string]string
}

type PullDoneArgs struct {
	Gid     int // puller's gid
	PullGen int // oldConfig's gen
}

type PullDoneReply struct {
	Err Err
}

type PushShardArgs struct {
	Gid      int                // sender's gid
	Shards   []int              // sender's shards ready to sync
	Database map[string]string  // sender's db
	ConfigX  shardctrler.Config // sender's newer config
	Id       int64
}

type PushShardReply struct {
	Err Err
}