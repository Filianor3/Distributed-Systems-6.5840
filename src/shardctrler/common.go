package shardctrler

import (
	"fmt"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

var Debug = false
var debugStart time.Time

// The number of shards.
const NShards = 10
const SHOW_BIT = 10000

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// ConfigDeepCopy deep copy an config from an old config
// the number will be old number + 1
func ConfigDeepCopy(config1 Config, increase bool) Config {
	res := Config{}
	res.Groups = make(map[int][]string)
	if increase {
		res.Num = config1.Num + 1
	} else {
		res.Num = config1.Num
	}
	for i := 0; i < NShards; i++ {
		res.Shards[i] = config1.Shards[i]
	}
	for k, v := range config1.Groups {
		// v is shared reference
		// deep copy v to vc
		vc := make([]string, len(v))
		copy(vc, v)
		res.Groups[k] = vc
	}
	return res
}

func ArrayToString(gids []int) string {
	res := "Groups(gid):"
	for _, v := range gids {
		res += fmt.Sprintf(" %d ", v)
	}
	return res
}

func ServerToString(server map[int][]string) string {
	res := "Groups(gid):"
	for k, _ := range server {
		res += fmt.Sprintf(" %d ", k)
	}
	return res
}

// ConfigToString trans an config to string for debug
func ConfigToString(config2 Config) string {
	res := ""
	res += fmt.Sprintf("cfn:%d ", config2.Num)
	res += ServerToString(config2.Groups)
	res += "Shards:(shard->gid)"
	for i := 0; i < NShards; i++ {
		res += fmt.Sprintf(" %d->%d ", i, config2.Shards[i])
	}
	return res
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ID      int64            // id for duplicate detection
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ID   int64 // id for duplicate detection
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ID    int64 // id for duplicate detection
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int   // desired config number
	ID  int64 // id for duplicate detection
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}