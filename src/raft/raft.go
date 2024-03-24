package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int 

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// each entry contains command for state machine, and term when entry
// was received by leader
type LogEntry struct {
	LogTerm int
	Command interface{}
}

// possible server states
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	nextIndex        []int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex       []int               // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	applyCh          chan ApplyMsg       // applied logs go here
	dead             int32               // set by Kill()
	state            ServerState         // my current state [Follower | Candidate | Leader]
	currentTerm      int                 // latest term server has seen
	didVote          bool                // true if voted in current term
	votedFor         int                 // if didVote: candidateId that received vote in current term
	log              []LogEntry          // log entries (first index is 1)
	timeoutTimestamp time.Time           // timestamp at which next timeout should happen unless something happens before then
	commitCond       sync.Cond           // conditional to wake up on each commmit index update
	commitIndex      int                 //index of highest log entry known to be committed
	lastApplied      int                 //index of highest log entry applied to state machine
	snapshotIndex    int                 // last log index which was included in the snapshot
	snapshot         []byte              // snapshot bytes
}

func (rf *Raft) logLen() int {
	return rf.snapshotIndex + len(rf.log)
}
func (rf *Raft) lastLogIndex() int {
	return rf.logLen() - 1
}
func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

type PersistentState struct {
	CurrentTerm   int
	DidVote       bool
	VotedFor      int
	Log           []LogEntry
	SnapshotIndex int
	Snapshot      []byte
}

// LOCK BEFORE CALL
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	var pstate PersistentState
	pstate.CurrentTerm = rf.currentTerm
	pstate.DidVote = rf.didVote
	pstate.VotedFor = rf.votedFor
	pstate.Log = rf.log
	pstate.SnapshotIndex = rf.snapshotIndex
	//pstate.Snapshot = rf.snapshot
	// DPrintf("%v: persist logLen = %v", rf.me, len(rf.log))
	e.Encode(pstate)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// LOCK BEFORE CALL
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pstate PersistentState
	if d.Decode(&pstate) != nil {
		panic("bad persistent state")
	} else {
		rf.currentTerm = pstate.CurrentTerm
		rf.didVote = pstate.DidVote
		rf.votedFor = pstate.VotedFor
		rf.log = pstate.Log
		rf.snapshotIndex = pstate.SnapshotIndex
	}
	rf.snapshot = snapshot
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("%v: Snapshot(%v)", rf.me, index)
	if index < rf.snapshotIndex {
		return
	}
	newLog := make([]LogEntry, len(rf.log[index-rf.snapshotIndex:]))
	copy(newLog, rf.log[index-rf.snapshotIndex:])
	rf.log = newLog
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int    // candidate’s term
	LeaderId          int    // don't care about this one
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte //  raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tryObserveHigherTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetTimeout()
	// already have larger snapshot
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}
	if args.LastIncludedIndex < rf.logLen() {
		existingEntry := rf.log[args.LastIncludedIndex-rf.snapshotIndex]
		if existingEntry.LogTerm != args.LastIncludedTerm {
			rf.log = make([]LogEntry, 1)
			rf.log[0].LogTerm = args.LastIncludedTerm
		} else {
			newLog := make([]LogEntry, len(rf.log[args.LastIncludedIndex-rf.snapshotIndex:]))
			copy(newLog, rf.log[args.LastIncludedIndex-rf.snapshotIndex:])
			rf.log = newLog
		}
	} else {
		rf.log = make([]LogEntry, 1)
		rf.log[0].LogTerm = args.LastIncludedTerm
	}
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.commitCond.Signal()
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("%v: sendInstallSnapshot(%v)", rf.me, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // candidate’s term
	LeaderId     int        // don't care about this one
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // index of log entry immediately preceding new ones
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tryObserveHigherTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.XLen = rf.logLen()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetTimeout()
	// TODO: handle properly
	if args.PrevLogIndex < rf.snapshotIndex {
		nExtra := rf.snapshotIndex - args.PrevLogIndex
		if nExtra > len(args.Entries) {
			reply.Success = true
			return
		}
		args.PrevLogTerm = args.Entries[nExtra-1].LogTerm
		args.PrevLogIndex = rf.snapshotIndex
		args.Entries = args.Entries[nExtra:]
		// FIXME: idx < snapshot?
		// panic("unimpl")
	}
	if rf.logLen() <= args.PrevLogIndex {
		reply.Success = false
		return
	}
	localPrevLog := rf.log[args.PrevLogIndex-rf.snapshotIndex]
	if localPrevLog.LogTerm != args.PrevLogTerm {
		reply.XTerm = localPrevLog.LogTerm
		for i, e := range rf.log {
			if e.LogTerm == args.Term {
				reply.XIndex = i + rf.snapshotIndex
				break
			}
		}
		reply.Success = false
		return
	}
	defer rf.persist()
	reply.Success = true
	for i := args.PrevLogIndex + 1; i < args.PrevLogIndex+1+len(args.Entries); i++ {
		arg_i := i - args.PrevLogIndex - 1
		// appending at the end
		if i == rf.logLen() {
			rf.log = append(rf.log, args.Entries[arg_i])
		} else {
			if rf.log[i-rf.snapshotIndex].LogTerm != args.Entries[arg_i].LogTerm {
				rf.log = rf.log[:i-rf.snapshotIndex] // truncate log
				rf.log = append(rf.log, args.Entries[arg_i])
			}
		}
	}
	var commitIndex int
	if args.PrevLogIndex+len(args.Entries) <= args.LeaderCommit {
		commitIndex = args.PrevLogIndex + len(args.Entries)
	} else {
		commitIndex = args.LeaderCommit
	}
	if rf.commitIndex < commitIndex {
		rf.commitIndex = commitIndex
		rf.commitCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("%v -> %v: sendAppendEntries(%v) -> %v, %v", rf.me, server, args, ok, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tryObserveHigherTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.lastLogTerm() > args.LastLogTerm || (rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	if !rf.didVote {
		rf.didVote = true
		rf.votedFor = args.CandidateId
		rf.resetTimeout()
		rf.persist()
	}
	reply.VoteGranted = args.CandidateId == rf.votedFor
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%v: sendRequestVote(%v)", rf.me, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == Leader
	var index int
	var term int
	if isLeader {
		index = rf.logLen()
		term = rf.currentTerm

		rf.log = append(rf.log, LogEntry{
			LogTerm: term,
			Command: command,
		})
		rf.persist()
		go rf.syncAllNodes(false)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// LOCK BEFORE CALL
func (rf *Raft) tryObserveHigherTerm(term int) bool {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.state = Follower
		rf.didVote = false
		rf.resetTimeout()
		rf.persist()
		return true
	} else {
		return false
	}
}

func (rf *Raft) nMajority() int {
	return len(rf.peers)/2 + 1
}

// LOCK BEFORE CALL
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logLen()
		rf.matchIndex[i] = 0
	}
	go rf.syncAllNodes(false)
}

// LOCK BEFORE CALL
func (rf *Raft) becomeCandidate() {
	rf.resetTimeout()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.didVote = true
	rf.votedFor = rf.me
	rf.persist()

	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.lastLogIndex()
	args.LastLogTerm = rf.lastLogTerm()

	nVotes := 1 // rf.mu protects this
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// send RPCs in parallel
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.tryObserveHigherTerm(reply.Term) {
				return
			}
			if args.Term != rf.currentTerm || rf.state != Candidate {
				// term or state changed since rpc sent; give up
				return
			}
			if reply.VoteGranted {
				nVotes += 1
			}
			if nVotes >= rf.nMajority() {
				rf.becomeLeader()
			}
		}(i) // copy i
	}
}

// LOCK BEFORE CALL
func (rf *Raft) resetTimeout() {
	ms := 400 + (rand.Int63() % 300)
	newTimeout := time.Now().Add(time.Duration(ms) * time.Millisecond)
	if rf.timeoutTimestamp.Before(newTimeout) { // not a necessary check; prevents setting lower timeout than previously set
		rf.timeoutTimestamp = newTimeout
	}
}

// LOCK BEFORE CALL
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	matchIndexSorted := make([]int, len(rf.matchIndex))
	copy(matchIndexSorted, rf.matchIndex)
	sort.Ints(matchIndexSorted)
	indexReplicatedOnMajority := matchIndexSorted[rf.nMajority()-1]
	if indexReplicatedOnMajority < rf.snapshotIndex {
		// should be okay to do this. if we have snapshot that it *MUST* have been applied
		indexReplicatedOnMajority = rf.snapshotIndex
	}
	if rf.log[indexReplicatedOnMajority-rf.snapshotIndex].LogTerm == rf.currentTerm {
		if rf.commitIndex < indexReplicatedOnMajority {
			rf.commitIndex = indexReplicatedOnMajority
			rf.commitCond.Signal()
		}
	}
}

// LOCK BEFORE CALL
// NON-BLOCKING
func (rf *Raft) syncWithSnapshot(followerId int, heartbeat bool) {
	if rf.state != Leader {
		panic("syncWithSnapshot: not leader")
	}
	var args InstallSnapshotArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshotIndex
	args.LastIncludedTerm = rf.log[0].LogTerm
	args.Data = rf.snapshot
	go func() {
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(followerId, &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			// failed; will retry on next heartbeat
			return
		}
		if rf.tryObserveHigherTerm(reply.Term) {
			return
		}
		if args.Term != rf.currentTerm {
			// term changed since rpc sent; give up
			return
		}
		if rf.state != Leader {
			panic("syncWithSnapshot: handling response; should be leader")
		}

		matchIndex := args.LastIncludedIndex
		if rf.nextIndex[followerId] < matchIndex+1 {
			rf.nextIndex[followerId] = matchIndex + 1
		}
		if rf.matchIndex[followerId] < matchIndex {
			rf.matchIndex[followerId] = matchIndex
			// matchIndex changed. check if we need to commit
			rf.updateCommitIndex()
		}
		if rf.matchIndex[followerId] != rf.lastLogIndex() {
			// more to send; trigger another sync
			rf.syncFollower(followerId, false)
		}
	}()
}

// LOCK BEFORE CALL
// NON-BLOCKING
func (rf *Raft) syncFollower(followerId int, heartbeat bool) {
	if rf.state != Leader {
		panic("syncFollower: not leader")
	}
	if rf.nextIndex[followerId]-1 < rf.snapshotIndex {
		rf.syncWithSnapshot(followerId, heartbeat)
		return
	}
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[followerId] - 1
	if args.PrevLogIndex < rf.snapshotIndex {
		panic("syncFollower: unreachable 1")
	}
	args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.snapshotIndex].LogTerm
	const MAX_LOGS_HEARTBEAT = 100
	const MAX_LOGS_SYNC = 100
	// send max 100 logs each RPC
	var maxLogs int
	if heartbeat {
		maxLogs = MAX_LOGS_HEARTBEAT
	} else {
		maxLogs = MAX_LOGS_SYNC
	}
	nToSend := rf.lastLogIndex() - args.PrevLogIndex
	if nToSend > maxLogs {
		nToSend = maxLogs
	}
	args.Entries = make([]LogEntry, nToSend)
	// will copy len(args.Entries) starting from PrevLogIndex
	copy(args.Entries, rf.log[args.PrevLogIndex+1-rf.snapshotIndex:])
	if len(args.Entries) > maxLogs {
		panic("entries > maxLogs")
	}
	args.LeaderCommit = rf.commitIndex

	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(followerId, &args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			// failed; will retry on next heartbeat
			return
		}
		if rf.tryObserveHigherTerm(reply.Term) {
			return
		}
		if args.Term != rf.currentTerm {
			// term changed since rpc sent; give up
			return
		}
		if rf.state != Leader {
			panic("syncFollower: handling response; should be leader")
		}

		if reply.Success {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			if rf.nextIndex[followerId] < matchIndex+1 {
				rf.nextIndex[followerId] = matchIndex + 1
			}
			if rf.matchIndex[followerId] < matchIndex {
				rf.matchIndex[followerId] = matchIndex
				// matchIndex changed. check if we need to commit
				rf.updateCommitIndex()
			}
			// if way far behind, try to catch up quickly
			if rf.matchIndex[followerId] + MAX_LOGS_SYNC < rf.lastLogIndex() {
				// more to send; trigger another sync
				rf.syncFollower(followerId, false)
			}
		} else {
			var seekToIndex int
			if reply.XTerm != 0 {
				lastXTermIndex := 0
				for i, e := range rf.log {
					if e.LogTerm == reply.XTerm {
						lastXTermIndex = i + rf.snapshotIndex
					}
				}
				if lastXTermIndex == 0 {
					// Case 1: leader doesn't have XTerm:
					//   nextIndex = XIndex
					seekToIndex = reply.XIndex
				} else {
					// Case 2: leader has XTerm:
					//   nextIndex = leader's last entry for XTerm
					seekToIndex = lastXTermIndex
				}
			} else {
				// Case 3: follower's log is too short:
				//   nextIndex = XLen
				seekToIndex = reply.XLen
			}

			// check against matchIndex makes sure prevIndex > 0
			// no need to go back more than matchIndex
			if seekToIndex <= rf.matchIndex[followerId] {
				seekToIndex = rf.matchIndex[followerId] + 1
			}
			if seekToIndex < rf.nextIndex[followerId] {
				rf.nextIndex[followerId] = seekToIndex
			}
			// rf.nextIndex[i] = rf.matchIndex[i] + 1
			// failed; trigger another sync
			rf.syncFollower(followerId, false)
		}
	}()
}

func (rf *Raft) syncAllNodes(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.syncFollower(i, heartbeat)
	}
}

// long runing thread
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		// Check if a leader election should be started.
		if rf.timeoutTimestamp.Before(time.Now()) && rf.state != Leader { // timeout Time has passed
			rf.becomeCandidate()
		}
		if rf.state == Leader {
			rf.resetTimeout()
		}
		// sleep until the next scheduled timeout
		sleepDuration := rf.timeoutTimestamp.Sub(time.Now())
		rf.mu.Unlock()
		time.Sleep(sleepDuration)
	}
}

// long runing thread
func (rf *Raft) commiter() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			rf.commitCond.Wait()
		}
		// rf.lastApplied != rf.commitIndex
		rf.lastApplied += 1
		var msg ApplyMsg
		if rf.lastApplied <= rf.snapshotIndex {
			msg.CommandValid = false
			msg.SnapshotValid = true
			msg.SnapshotTerm = rf.log[0].LogTerm
			msg.SnapshotIndex = rf.snapshotIndex
			msg.Snapshot = rf.snapshot
			rf.lastApplied = rf.snapshotIndex
		} else {
			msg.CommandValid = true
			msg.CommandIndex = rf.lastApplied
			msg.Command = rf.log[rf.lastApplied-rf.snapshotIndex].Command
		}
		rf.mu.Unlock()

		rf.applyCh <- msg
	}
}

// long runing thread
func (rf *Raft) heartbeater() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			go rf.syncAllNodes(true)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	// initialize everything for clarity
	rf.state = Follower
	rf.didVote = false
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 1) // 0-index log entry to ignore
	rf.resetTimeout()
	rf.commitIndex = 0
	rf.commitCond = *sync.NewCond(&rf.mu)
	rf.lastApplied = 0
	rf.snapshotIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeater()
	go rf.commiter()

	return rf
}