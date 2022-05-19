package raft

import "time"

func (rf *Raft) lock(lockName string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = lockName
}

func (rf *Raft) unlock(lockName string){
	rf.lockEnd = time.Now()
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		rf.log("%s locked too long", lockName)
	}
	rf.mu.Unlock()
}