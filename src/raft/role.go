package raft

import "time"

const FOLLOWER, CANDIDATE, LEADER = 0, 1, 2

func (rf *Raft) election() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <- rf.electionTimer.C:
			rf.startElection()
		}
	}
}
func (rf *Raft) startElection()  {
	rf.lock("startElection")
	rf.electionTimer.Reset(getRandomElectionTimeout())
	if rf.role == LEADER {
		rf.unlock("startElection")
		return
	}
	rf.switchRole(CANDIDATE)
	rf.persist()
	lastLogTerm, lastLogIndex := rf.getLastTermAndIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.unlock("startElection")
	rf.log("start election")
	chVote := make(chan bool, len(rf.peers))
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{
				Term:        0,
				VoteGranted: false,
			}
			rf.sendRequestVote(peer, &args, &reply)
			chVote <- reply.VoteGranted
			// compare reply.Term and args.Term first
			if reply.Term > args.Term {
				rf.lock("startElectionCompareTerm")
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.switchRole(FOLLOWER)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("startElectionCompareTerm")
			}
		}(peer)
	}
	resCount := 1
	grantedCount := 1
	for true {
		res := <- chVote
		resCount++
		if res == true {
			grantedCount += 1
		}
		if resCount == len(rf.peers) || grantedCount > len(rf.peers) / 2 || resCount - grantedCount > len(rf.peers) / 2 {
			break
		}
	}
	if grantedCount <= len(rf.peers) / 2 {
		rf.log("grantedCount: %d, and it <= 1/2 of len(peers): %d", grantedCount, len(rf.peers))
		rf.lock("")
		rf.switchRole(FOLLOWER)
		rf.unlock("")
		return
	}
	rf.lock("startElectionEnd")
	if rf.currentTerm == args.Term && rf.role == CANDIDATE {
		rf.switchRole(LEADER)
		rf.log("succeed to be leader!")
		rf.persist()
	}
	rf.unlock("startElectionEnd")
}

func (rf *Raft) switchRole(role int)  {
	rf.role = role
	switch role {
	case FOLLOWER:
	case CANDIDATE:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()
	case LEADER:
		lastLogIndex := rf.getLastIndex()
		rf.nextIndex = make([]int, len(rf.peers))
		for peer := 0; peer < len(rf.peers); peer++ {
			rf.nextIndex[peer] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
		rf.resetHeartbeatTimers(time.Duration(0))
	default:
		panic("wrong role!")
	}
}
