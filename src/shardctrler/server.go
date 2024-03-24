package shardctrler

import (
	"6.5840/raft"
	"bytes"
	"math/rand"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu        sync.Mutex
	wmu       sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	// Your data here.
	// config number, start from 0, 0 is default configuration
	ConfigNumber int

	Configs []Config // indexed by config num

	DuplicatedMap map[int64]bool // map for duplicated operation
}

type CommandType string

const (
	JoinCOMMAND  CommandType = "join"
	LeaveCOMMAND CommandType = "leave"
	MoveCOMMAND  CommandType = "move"
	QueryCOMMAND CommandType = "query"
)

// Op command for raft to make agreement
// captital first letter
type Op struct {
	// Your data here.
	ConfigX Config      // for join/leave/move
	CId     int         // especially for query
	ID      int64       // Id for duplicate operation
	Kind    CommandType // commandType
}

func (sc *ShardCtrler) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.ConfigNumber)
	for i := 0; i < len(sc.Configs); i++ {
		// persist num
		e.Encode(sc.Configs[i].Num)
		for j := 0; j < NShards; j++ {
			e.Encode(sc.Configs[i].Shards[j])
		}
		e.Encode(len(sc.Configs[i].Groups))
		for k, v := range sc.Configs[i].Groups {
			// gid -> []string
			e.Encode(k)      // gid
			e.Encode(len(v)) // len([])
			for _, it := range v {
				e.Encode(it)
			}
		}
	}
	for k, _ := range sc.DuplicatedMap {
		e.Encode(k) // int64
	}
	return w.Bytes()
}

func (sc *ShardCtrler) RestoreSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	sc.DuplicatedMap = make(map[int64]bool)
	sc.Configs = sc.Configs[:0] // clear?

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cfn int
	if d.Decode(&cfn) != nil {
		return
	}
	sc.ConfigNumber = cfn
	for i := 0; i < cfn; i++ { // for each config
		var num int
		if d.Decode(&num) != nil {
			return
		}
		curConfig := Config{}
		curConfig.Num = num
		curConfig.Groups = make(map[int][]string)
		for j := 0; j < NShards; j++ { // shards
			var sd int
			if d.Decode(&sd) != nil {
				return
			}
			curConfig.Shards[j] = sd
		}
		var gpn int
		if d.Decode(&gpn) != nil {
			return
		}
		for j := 0; j < gpn; j++ { // for each group
			var k, le int
			var strs []string
			if d.Decode(&k) != nil || d.Decode(&le) != nil { // key with len
				return
			}
			for jj := 0; jj < le; jj++ {
				var it string
				if d.Decode(&it) != nil {
					return
				}
				strs = append(strs, it)
			}
			curConfig.Groups[k] = strs
		}
		sc.Configs = append(sc.Configs, curConfig)
	}
}

// ScheduleShard a determistic method to generate shards
func ScheduleShard(config2 Config) [NShards]int {
	var res [NShards]int
	// got all GIDS
	var gids []int
	for k, _ := range config2.Groups {
		gids = append(gids, k)
	}
	if len(gids) > 0 {
		// use RR strategy to shard, may not the least change
		for i := 0; i < NShards; i++ { // GID
			res[i] = gids[i%len(gids)]
		}
	} else { // use default gid
		for i := 0; i < NShards; i++ {
			res[i] = 0
		}
	}
	return res
}

// WaitAndApply it may use when locked, and the return status is still locked...
// if leader, wait until agreement or return not leader if timeout
// if follower, return immediately
// @args:  cmd
// @reply : wrongleader
func (sc *ShardCtrler) WaitAndApply(cmd Op) (bool, Config) {
	wrongLeader := false
	_, _, ok := sc.rf.Start(cmd)
	if ok { // leader, wait a moment for agreement
		startTime := time.Now()
		for time.Now().Sub(startTime) < 2*time.Second {
			if sc.DuplicatedMap[cmd.ID] {
				if cmd.Kind == QueryCOMMAND { // query
					if cmd.CId >= 0 && cmd.CId < len(sc.Configs) { // normal
						resConfig := ConfigDeepCopy(sc.Configs[cmd.CId], false)
						return false, resConfig
					} else { // read latest
						
						resConfig := ConfigDeepCopy(sc.Configs[len(sc.Configs)-1], false)
						return false, resConfig
					}
				} else {
					return false, Config{}
				}
			}

			// wait some time and re-check
			sc.mu.Unlock()
			ms := 10 + rand.Int()%10
			time.Sleep(time.Duration(ms) * time.Millisecond)
			sc.mu.Lock()
		}
		// timeout, return false
		// that rely on idempotence..
		wrongLeader = true
	} else { // wrong leader, return
		wrongLeader = true
	}
	return wrongLeader, Config{}
}

// Join (servers) -- add a set of groups (gid -> server-list mapping).
// will increase a new config finally
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	if sc.DuplicatedMap[args.ID] { // duplicated, ignore it and return
		reply.WrongLeader = false
		return
	}

	// the config's place is decide here.
	// if retry, it may change another number
	newConfig := ConfigDeepCopy(sc.Configs[sc.ConfigNumber], true) // copy config
	for k, v := range args.Servers {                               // join gid->list pairs, shadow copy here
		newConfig.Groups[k] = v
	}
	newConfig.Shards = ScheduleShard(newConfig) // reschedule the shard after change groups..

	cmd := Op{
		ConfigX: newConfig,
		ID:      args.ID,
		Kind:    JoinCOMMAND,
	}
	reply.WrongLeader, _ = sc.WaitAndApply(cmd)
	return
}

// Leave(gids) -- delete a set of groups.
// will increase a new config finally
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	if sc.DuplicatedMap[args.ID] { // duplicated, ignore it and return
		reply.WrongLeader = false
		return
	}

	// new config
	newConfig := ConfigDeepCopy(sc.Configs[sc.ConfigNumber], true)
	// prove the gids must exist
	for _, vic := range args.GIDs {
		delete(newConfig.Groups, vic) // error?
	}
	newConfig.Shards = ScheduleShard(newConfig)

	cmd := Op{
		ConfigX: newConfig,
		ID:      args.ID,
		Kind:    LeaveCOMMAND,
	}
	reply.WrongLeader, _ = sc.WaitAndApply(cmd)
	return
}

// Move(shard, gid) -- hand off one shard from current owner to gid.
// will increase a new config finally
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.wmu.Lock()
	defer sc.wmu.Unlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	if sc.DuplicatedMap[args.ID] { // duplicated, ignore it and return
		reply.WrongLeader = false
		return
	}

	// new config
	newConfig := ConfigDeepCopy(sc.Configs[sc.ConfigNumber], true)
	newConfig.Shards[args.Shard] = args.GID // change shard->gid
	cmd := Op{
		ConfigX: newConfig,
		ID:      args.ID,
		Kind:    MoveCOMMAND,
	}
	reply.WrongLeader, _ = sc.WaitAndApply(cmd)
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, isLeader := sc.rf.GetState()
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	if sc.DuplicatedMap[args.ID] { // duplicated, ignore it and return
		reply.WrongLeader = false
		return
	}

	cmd := Op{
		CId:  args.Num,
		ID:   args.ID,
		Kind: QueryCOMMAND,
	}
	reply.WrongLeader, reply.Config = sc.WaitAndApply(cmd)
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	//Debug = false
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Applier(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.CommandValid {
			sc.mu.Lock()
			AppliedCmd := msg.Command.(Op)
			if sc.DuplicatedMap[AppliedCmd.ID] == false {
				sc.DuplicatedMap[AppliedCmd.ID] = true
				if AppliedCmd.Kind != QueryCOMMAND { // actually change config
					if len(sc.Configs)-1 >= AppliedCmd.ConfigX.Num { // overwrite
						sc.Configs[AppliedCmd.ConfigX.Num] = ConfigDeepCopy(AppliedCmd.ConfigX, false)
					} else { // append
						for len(sc.Configs) < AppliedCmd.ConfigX.Num {
							sc.Configs = append(sc.Configs, Config{}) // append victims...
						}
						sc.Configs = append(sc.Configs, ConfigDeepCopy(AppliedCmd.ConfigX, false))
					}
					sc.ConfigNumber++
				}
			}
			sc.mu.Unlock()
		}
		if msg.SnapshotValid {
			
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.persister = persister
	sc.Configs = make([]Config, 1)
	sc.Configs[0].Groups = map[int][]string{}
	sc.Configs[0].Num = 0
	for i := 0; i < NShards; i++ { // default map
		sc.Configs[0].Shards[i] = 0
	}
	sc.ConfigNumber = 0
	sc.DuplicatedMap = make(map[int64]bool)
	debugStart = time.Now()
	//sc.RestoreSnapshot(persister.ReadSnapshot())

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	go sc.Applier(sc.applyCh)
	return sc
}