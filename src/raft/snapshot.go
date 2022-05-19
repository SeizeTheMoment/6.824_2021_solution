package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term              int
}
func (rf *Raft) sendInstallSnapshotToPeer(peer int)  {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rf.lock("readyToSendSnapshot")
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.snapshot,
	}
	rf.unlock("readyToSendSnapshot")
	for{
		t.Stop()
		t.Reset(RPCTimeout)
		ch := make(chan bool)
		reply := InstallSnapshotReply{}
		go func() {
			ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()
		select {
		case ok := <-ch:
			if !ok {
				continue
			}
		case <- t.C:
			continue
		case <-rf.stopCh:
			return
		}
		rf.lock("sendInstallSnapshot")
		if rf.currentTerm != args.Term || rf.role != LEADER {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.switchRole(FOLLOWER)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex + 1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}

		rf.unlock("sendInstallSnapshot")
		return
	}
}

func (rf* Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	rf.lock("installSnapshot")
	defer rf.unlock("installSnapshot")
	rf.log("receive snapshot, args: %+v", args)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.switchRole(FOLLOWER)
		rf.resetElectionTimer()
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}