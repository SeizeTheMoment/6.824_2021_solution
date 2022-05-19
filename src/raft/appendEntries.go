package raft

import (
	"time"
)
type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []LogEntry
	LeaderCommit    int
}
type AppendEntriesReply struct {
	Term            int
	Success         bool
	ConflictIndex   int
	ConflictTerm    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	rf.lock("appendEntries")
	rf.log("get append entries: %+v", *args)
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		rf.unlock("appendEntries")
		return
	}
	rf.currentTerm = args.Term
	rf.switchRole(FOLLOWER)
	rf.persist()
	rf.resetElectionTimer()
	// reply false if log doesn't contain an entry at prevLogIndex
	lastLogIndex := rf.getLastIndex()
	// when the peer doesn't contain log entry of prevLogIndex,
	// let conflictIndex = len(log)
	// let conflictTerm = -1
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		// 因为 lastsnapshotindex 应该已经被 apply，正常情况不该发生
		reply.Success = false
		reply.ConflictIndex = rf.lastSnapshotIndex + 1
	} else if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLogLength()
		reply.ConflictTerm = -1
		// when the peer does contain log entry of prevLogIndex
		// yet the term doesn't match,
		// let conflictTerm = log[prevLogIndex].Term
		// let conflictIndex = firstIndexOf(conflictTerm)
	} else if args.PrevLogIndex != -1 && rf.logEntries[rf.indexToSliceIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		rf.log("prev log term does not match")
		reply.ConflictTerm = rf.logEntries[rf.indexToSliceIndex(args.PrevLogIndex)].Term
		reply.ConflictIndex = rf.getFirstSliceIndexOfTerm(args.PrevLogIndex, reply.ConflictTerm) + rf.lastSnapshotIndex
	} else {
		reply.Success = true
	}
	// commitIndex = min(commitIndex, LeaderCommit)
	if reply.Success {
		rf.updateLogs(args.Entries, args.PrevLogIndex, args.LeaderCommit)
		reply.ConflictIndex = rf.getLogLength()
	}
	//rf.log("get appendEntries: %+v, reply: %+v", *args, *reply)
	rf.unlock("appendEntries")

}

func (rf *Raft) resetHeartbeatTimers(duration time.Duration)  {
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(duration)
	}
}
func (rf *Raft) resetHeartbeatTimer(peer int, duration time.Duration) {
	rf.appendEntriesTimers[peer].Stop()
	rf.appendEntriesTimers[peer].Reset(duration)
}
func (rf *Raft) getAppendLogs(peer int) (prevLogIndex int, prevLogTerm int, newEntries []LogEntry) {
	nextIndex := rf.nextIndex[peer]
	lastTerm, lastIndex := rf.getLastTermAndIndex()
	//there is no entry to send

	if nextIndex <= rf.lastSnapshotIndex || nextIndex > lastIndex {
		prevLogIndex = lastIndex
		prevLogTerm = lastTerm
		newEntries = []LogEntry{}
		return prevLogIndex, prevLogTerm, newEntries
	}
	newEntries = append([]LogEntry{}, rf.logEntries[rf.indexToSliceIndex(nextIndex):]...)
	//prevLogIndex = -1
	//prevLogTerm = -1
	if nextIndex != 0 {
		prevLogIndex = nextIndex - 1
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	return prevLogIndex, prevLogTerm, newEntries
}
func (rf *Raft) newAppendEntriesArgs(peer int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm, newEntries := rf.getAppendLogs(peer)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      newEntries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}
// if there's something wrong, send append entries indefinitely
func (rf *Raft) appendEntriesToPeer(peerIndex int)  {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.lock("appendToPeer1")
		if rf.role != LEADER {
			rf.resetHeartbeatTimer(peerIndex, HeartBeatTimeout)
			rf.unlock("appendToPeer1")
			return
		}
		args := rf.newAppendEntriesArgs(peerIndex)
		rf.resetHeartbeatTimer(peerIndex, HeartBeatTimeout)
		rf.unlock("appendToPeer1")
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		chOk := make(chan bool, 1)
		// new goroutine to call appendEntries
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			chOk <- ok
		}(&args, &reply)
		select {
		case <- rf.stopCh:
			return
		// if timeout, retry it.
		case <- RPCTimer.C:
			//rf.log("append entries time out, peer: %d, args:%+v, ready to retry", peerIndex, args)
			continue
		// if rpc works not well, retry it
		case ok := <- chOk:
			if !ok {
				//rf.log("append entries to peer %d failed, ready to retry", peerIndex)
				continue
			}
		}
		//rf.log("append to peer: %d, args: %+v, reply: %+v", peerIndex, args, reply)
		rf.lock("appendToPeer2")
		//check reply, if the leader possesses old term, switch to follower role
		if reply.Term > rf.currentTerm {
			rf.switchRole(FOLLOWER)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			rf.unlock("appendToPeer2")
			return
		}
		// receive old reply, discard it and return
		if rf.role != LEADER || rf.currentTerm != args.Term {
			rf.unlock("appendToPeer2")
			return
		}
		// if failed, get new nextIndex by conflictTerm and conflictIndex
		if !reply.Success {
			newNextIndex := reply.ConflictIndex
			conflictTerm := reply.ConflictTerm
			for idx := args.PrevLogIndex; rf.indexToSliceIndex(idx) >= 1; idx-- {
				if rf.logEntries[rf.indexToSliceIndex(idx)].Term == conflictTerm {
					newNextIndex = idx + 1
					break
				}
			}
			if newNextIndex <= rf.lastSnapshotIndex {
				rf.unlock("appendToPeer2")
				rf.log("ready to send install snapshot to peer: %d", peerIndex)
				go rf.sendInstallSnapshotToPeer(peerIndex)
			} else {
				rf.nextIndex[peerIndex] = newNextIndex
				rf.unlock("appendToPeer2")
				continue
			}
			return
		} else {
			rf.nextIndex[peerIndex] = reply.ConflictIndex
			rf.matchIndex[peerIndex] = reply.ConflictIndex - 1
			//rf.matchIndex[peerIndex] = args.PrevLogIndex + len(args.Entries)
			rf.updateCommitIndex()
			rf.unlock("appendToPeer2")
			return
		}
	}
}
