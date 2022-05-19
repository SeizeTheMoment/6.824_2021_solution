package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int
	CandidateId     int
	LastLogIndex    int
	LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term           int
	VoteGranted    bool
}

func (rf *Raft) resetElectionTimer()  {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(getRandomElectionTimeout())
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("request_vote")
	defer rf.unlock("request_vote")
	defer rf.persist()
	defer func() {
		rf.log("get request vote, args: %+v, reply:%+v", args, reply)
	}()
	lastTerm, lastIndex := rf.getLastTermAndIndex()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == LEADER {
			return
		}
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		// 当前term 已经投过票了不会再投
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.switchRole(FOLLOWER)
		rf.persist()
	}
	// leader election restriction
	// a candidate can be voted only if it possesses all logs
	if lastTerm > args.LastLogTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.switchRole(FOLLOWER)
	rf.resetElectionTimer()
	rf.persist()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout / 5)
	defer rpcTimer.Stop()
	for  {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout / 5)
		ch := make(chan bool, 1)
		tmpReply := RequestVoteReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &tmpReply)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()
		select {
		case <- t.C:
			return
		case <- rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = tmpReply.Term
				reply.VoteGranted = tmpReply.VoteGranted
				return
			}
		}
	}
}