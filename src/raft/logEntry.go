package raft

import (
	"sort"
)

type LogEntry struct {
	Term        int
	Index       int
	Command     interface{}
}

func (rf *Raft) getLastTerm() int {
	lastIndex := len(rf.logEntries) - 1
	return rf.logEntries[lastIndex].Term
}
func (rf *Raft) getLastIndex() int {
	return rf.lastSnapshotIndex + len(rf.logEntries) - 1
}
func (rf *Raft) getLogLength() int {
	return len(rf.logEntries) + rf.lastSnapshotIndex
}
func (rf *Raft) getLastTermAndIndex() (int, int){
	lastIndex := len(rf.logEntries) - 1
	lastTerm := rf.logEntries[lastIndex].Term
	realLastIndex := lastIndex + rf.lastSnapshotIndex
	return lastTerm, realLastIndex
}

func (rf *Raft) getLogByIndex(idx int) LogEntry {
	return rf.logEntries[idx - rf.lastSnapshotIndex]
}

func (rf *Raft) getFirstSliceIndexOfTerm(beginIndex int, term int) int{
	idx := 0
	if rf.indexToSliceIndex(beginIndex) < 0 {
		return 0
	}
	for i := rf.indexToSliceIndex(beginIndex); i >= 0; i-- {
		if term != rf.logEntries[i].Term {
			idx = i
			break
		}
	}
	return idx
}
func (rf* Raft) indexToSliceIndex(index int) int {
	return index - rf.lastSnapshotIndex
}
func (rf* Raft) sliceIndexToIndex(sliceIndex int) int{
	return rf.lastSnapshotIndex + sliceIndex
}
func (rf *Raft) updateCommitIndex() {
	if rf.role != LEADER {
		return
	}
	matchIndex := make([]int, len(rf.peers))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	length := len(matchIndex)
	median := matchIndex[length / 2]
	//only commit log of current term
	rf.log("ready to update commit index, matchIndex: %v", rf.matchIndex)
	if median > rf.commitIndex && rf.logEntries[rf.indexToSliceIndex(median)].Term == rf.currentTerm {
		rf.commitIndex = median
		rf.notifyApplyCh <- struct{}{}
	}
}



func (rf *Raft) runApplyLogs() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.applyTimer.C:
			rf.notifyApplyCh <- struct{}{}
		case <-rf.notifyApplyCh:
			rf.startApplyLogs()
		}
	}
}

// apply logs per 100 millisecond
func (rf *Raft) startApplyLogs()  {
	defer rf.applyTimer.Reset(ApplyInterval)
	rf.lock("applyLogs1")
	var applyMsg[] ApplyMsg
	if rf.commitIndex <= rf.lastApplied {
		applyMsg = make([]ApplyMsg, 0)
	} else {
		rf.log("ready to apply")
		applyMsg = make([]ApplyMsg, 0, rf.commitIndex - rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex ; i++ {
			applyMsg = append(applyMsg, ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.indexToSliceIndex(i)].Command,
				CommandIndex: i,
			})
			rf.lastApplied = i
		}
	}
	rf.unlock("applyLogs1")
	for _, msg := range applyMsg {
		rf.applyCh <- msg
		rf.lock("logOutput")
		rf.log("send applych idx:%d", msg.CommandIndex)
		rf.unlock("logOutput")
	}
}

func (rf *Raft) updateLogs(newEntries []LogEntry, prevLogIndex int, leaderCommit int) {
	defer rf.persist()
	for x := 0; x < len(newEntries); {
		nowIndex := prevLogIndex + x + 1
		if nowIndex < rf.getLogLength() {
			//back
			selfEntry := rf.logEntries[rf.indexToSliceIndex(nowIndex)]
			leaderEntry := newEntries[x]
			if selfEntry.equals(&leaderEntry) {
				// consistent   next
				// fmt.Println(raftLog.rf.selfPrint(), "encounters consistent log when update log")
				x++
			} else {
				//inconsistent remove it!
				//remove log at nowIndex, and remove all after this point
				//raftLog.entries = append(raftLog.entries[:nowIndex])
				rf.logEntries = append(rf.logEntries[:rf.indexToSliceIndex(nowIndex)], newEntries[x:]...)
			}
		} else {
			rf.logEntries = append(rf.logEntries, newEntries[x:]...)
			break
		}
	}
	//fig 2 AppendEntries RPC 5
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit > rf.commitIndex {
		//oldCommitIndex := raftLog.commitIndex
		rf.commitIndex = min(leaderCommit, rf.getLastIndex())
		rf.notifyApplyCh <- struct{}{}
	}
}
func (entryA *LogEntry) equals(entryB* LogEntry) bool{
	return entryA.Term == entryB.Term && entryA.Index == entryB.Index
}

//snapshotIndex initialized to 0, and increase monotonically
func (rf *Raft) getRealIndex(idx int) int {
	return rf.lastSnapshotIndex + idx
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}