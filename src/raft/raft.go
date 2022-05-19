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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	ElectionTimeout  = time.Millisecond * 600 // 选举
	HeartBeatTimeout = time.Millisecond * 300 // leader 发送心跳
	ApplyInterval    = time.Millisecond * 20 // apply log
	RPCTimeout       = time.Millisecond * 150
	MaxLockTime      = time.Millisecond * 10 // debug
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role          int
	currentTerm   int
	votedFor      int
	logEntries    []LogEntry
	applyCh       chan ApplyMsg
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int

	lastSnapshotIndex    int
	lastSnapshotTerm     int
	snapshot             []byte

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer
	notifyApplyCh       chan struct{}
	stopCh              chan struct{}

	DebugLog  bool
	lockStart time.Time
	lockEnd       time.Time
	lockName      string
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.lock("getState")
	defer rf.unlock("getState")
	return rf.currentTerm, rf.role == LEADER
}
func (rf *Raft) getPersistState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	return data
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}



//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor    int
	//var commitIndex int
	var logEntries  []LogEntry
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatalln("read persist wrong")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		//rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logEntries = logEntries
	}
}
func (rf* Raft) saveStateAndSnapshot()  {
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), rf.snapshot)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.lock("condInstallSnapshot")
	defer rf.unlock("condInstallSnapshot")
	if lastIncludedIndex <= rf.commitIndex{
		return false
	}
	if lastIncludedIndex <= rf.getLastIndex() && rf.getLastTerm() == lastIncludedTerm {
		rf.logEntries = append([]LogEntry{}, rf.logEntries[lastIncludedIndex - rf.lastSnapshotIndex:]...)
	} else {
		rf.logEntries = append([]LogEntry{}, LogEntry{
			Term:    lastIncludedTerm,
			Index:   lastIncludedIndex,
			Command: nil,
		})
	}
	rf.lastSnapshotIndex = lastIncludedIndex
	rf.lastSnapshotTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.saveStateAndSnapshot()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("snapshot")
	defer rf.unlock("snapshot")
	rf.log("receive snapshot...index: %d", index)
	defer rf.log("snapshot done")
	if index <= rf.lastSnapshotIndex {
		return
	}
	rf.logEntries = rf.logEntries[index - rf.lastSnapshotIndex:]
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = rf.logEntries[rf.indexToSliceIndex(index)].Term
	rf.snapshot = snapshot
	rf.saveStateAndSnapshot()
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock("start")
	defer rf.unlock("start")
	index := rf.getLogLength()
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.currentTerm,
			Index:   index,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
	}
	rf.resetHeartbeatTimers(0)
	rf.persist()
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.DebugLog = false
	rf.applyCh = applyCh
	rf.role = FOLLOWER
	rf.stopCh = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]LogEntry, 1)

	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.electionTimer = time.NewTimer(getRandomElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	rf.log("restart...")
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatTimeout)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)
	rf.notifyApplyCh = make(chan struct{}, 100)
	go rf.election()
	go rf.runApplyLogs()
	// leader appendEntries
	for i := 0; i < len(peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for  {
				select {
				case <- rf.stopCh:
					return
				case <- rf.appendEntriesTimers[index].C:
					rf.appendEntriesToPeer(index)
				}
			}
		}(i)
	}
	return rf
}

func getRandomElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63()) % ElectionTimeout
}
func (rf *Raft) GetRole() int {
	rf.lock("getRole")
	role := rf.role
	defer rf.unlock("getRole")
	return role
}

func (rf *Raft) HasLogAtCurrentTerm() bool  {
	rf.lock("check")
	flag := rf.getLastTerm() >= rf.currentTerm
	rf.unlock("check")
	return flag
}