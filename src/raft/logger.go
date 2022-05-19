package raft

import (
	"fmt"
	"log"
)

func (rf *Raft) log(format string, a ...interface{}) {
	if rf.DebugLog == false {
		return
	}
	lastTerm, lastIndex := rf.getLastTermAndIndex()
	r := fmt.Sprintf(format, a...)
	s := fmt.Sprintf("[peer: %d, role: %d, currentTerm: %d, logEntries: %v,commitIndex: %d, lastApplied: %d, " +
		"nextIndex: %+v, matchIndex: %v, lastLogTerm: %d, lastLogIndex: %d, lastSnapshotIndex: %d, lastSnapshotTerm: %d]",
		rf.me, rf.role, rf.currentTerm, rf.logEntries,rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex,
		lastTerm, lastIndex, rf.lastSnapshotIndex, rf.lastSnapshotTerm)
	log.Printf("%s: log: %s\n",s, r)
}

func transfer(entries []LogEntry) []LogEntry {
	bk := make([]LogEntry, len(entries))
	copy(bk, entries)
	for i := 0; i < len(bk); i++ {
		bk[i].Command = "0"
	}
	return bk
}