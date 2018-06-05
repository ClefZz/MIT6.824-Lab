package raft

import (
	"math/rand"
	"time"
)

const (
	OperationSucceed = 0
	CallFail         = -1
	AppendRollback   = -2
	VoteDenied       = -3
	Fallback         = -4
)

type commandTask struct {
	command interface{}
	index   chan int
}

func electionTimeOut() <-chan time.Time {
	return time.After(time.Duration(rand.Int()%200+200) * time.Millisecond)
}

func heartBeatTimeOut() <-chan time.Time {
	return time.After(time.Duration(125) * time.Millisecond)
}

type AppendArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendReply struct {
	Term           int
	PrevKnownTerm  int
	PrevKnownIndex int
	Success        bool
}

type ACall struct {
	args  *AppendArgs
	reply chan *AppendReply
}

type RequestArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestReply struct {
	Term        int
	VoteGranted bool
}

type RCall struct {
	args  *RequestArgs
	reply chan *RequestReply
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	args := []interface{}{rf.me, rf.currentTerm}
	args = append(args, a...)
	DPrintf("[%v, %v] "+format, args...)
}

func (rf *Raft) lastLog() *Log {
	if len(rf.logs) <= 0 {
		return nil
	} else {
		return &rf.logs[len(rf.logs)-1]
	}
}

func (rf *Raft) lastIndex() int {
	if lastLog := rf.lastLog(); lastLog != nil {
		return lastLog.Index
	} else {
		return -1
	}
}

func (rf *Raft) drainCommandQueue() {
	for {
		select {
		case <-rf.commandQueue:
		default:
			return
		}
	}
}

func (rf *Raft) commit() {
	if rf.commitIndex > rf.lastAppied {
		if rf.logs[rf.commitIndex].Term < rf.currentTerm {
			return
		}
		//rf.DPrintf("committing logs between %v and %v", rf.lastAppied, rf.commitIndex)
		i, bound := findLogOfIndex(rf, rf.lastAppied)+1, findLogOfIndex(rf, rf.commitIndex)
		for ; i <= bound; i++ {
			log := rf.logs[i]
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index + 1,
			}
			rf.lastAppied = log.Index
		}
	}
}

func populateAppendReply(rf *Raft, args *AppendArgs, asFollower bool) (re *AppendReply) {
	re = &AppendReply{
		Term:           rf.currentTerm,
		PrevKnownIndex: -1,
		PrevKnownTerm:  -1,
		Success:        false,
	}

	if callTerm := args.Term; callTerm >= rf.currentTerm && (asFollower || callTerm != rf.currentTerm) {
		re.Term = callTerm
		re.Success = len(args.Entries) <= 0 ||
			(findLog(rf, args.PrevLogIndex, args.PrevLogTerm) >= -1)

		if !re.Success {
			if lastKnown := findLogOfTerm(rf, args.PrevLogTerm); lastKnown != nil {
				re.PrevKnownIndex = lastKnown.Index
				re.PrevKnownTerm = lastKnown.Term
			}
		}
	}
	return
}

func processAppend(rf *Raft, args *AppendArgs, re *AppendReply) (termChanged bool) {
	if termChanged = (re.Term > rf.currentTerm); termChanged {
		rf.currentTerm = re.Term
		rf.votedFor = -1
		rf.persist()
	}

	if re.Success {
		if len(args.Entries) > 0 {
			// rf.DPrintf("prepare to update log after %v @ %v: %v", args.PrevLogIndex, args.PrevLogTerm, args.Entries)
			updateLog(rf, findLog(rf, args.PrevLogIndex, args.PrevLogTerm)+1, args.Entries)
			rf.persist()
		}

		leaderCommit := args.LeaderCommit
		lastIdx := rf.lastIndex()
		if leaderCommit > rf.commitIndex {
			if leaderCommit > lastIdx {
				rf.commitIndex = lastIdx
			} else {
				rf.commitIndex = leaderCommit
			}
			rf.commit()
		}
	}
	return
}

func populateRequestReply(rf *Raft, args *RequestArgs) (re *RequestReply) {
	re = &RequestReply{rf.currentTerm, false}

	// rf.DPrintf("lastLog: %+v, LastIndex: %v, LastTerm: %v", rf.lastLog(), args.LastLogIndex, args.LastLogTerm)
	if callTerm := args.Term; callTerm >= rf.currentTerm {
		re.Term = callTerm
		if (callTerm > rf.currentTerm ||
			rf.votedFor == -1 ||
			rf.votedFor == args.CandidateId) &&
			compareLog(rf.lastLog(), args.LastLogIndex, args.LastLogTerm) {
			re.VoteGranted = true
		}
	}
	return
}

func postProcessRequest(rf *Raft, args *RequestArgs, re *RequestReply) (termChanged bool) {
	if termChanged = re.Term > rf.currentTerm; termChanged {
		rf.currentTerm = re.Term
	}

	if re.VoteGranted {
		rf.votedFor = args.CandidateId
	} else {
		rf.votedFor = -1
	}
	rf.persist()
	return
}

func findLogOfIndex(rf *Raft, index int) int {
	if index < 0 {
		return -1
	}
	realIndex := index - rf.logBase
	if realIndex < 0 || realIndex >= len(rf.logs) {
		return -2
	}
	return realIndex
}

func findLogOfTerm(rf *Raft, term int) (lastKnown *Log) {
	lastKnown = nil
	for i, log := range rf.logs {
		if log.Term > term {
			break
		}
		if lastKnown == nil || lastKnown.Term < log.Term {
			lastKnown = &rf.logs[i]
		}
	}
	return
}

func findLog(rf *Raft, index, term int) int {
	if index == -1 && term == -1 {
		return -1
	}

	if idx := findLogOfIndex(rf, index); idx >= 0 {
		if rf.logs[idx].Term == term {
			return idx
		}
	}
	return -2
}

func updateLog(rf *Raft, startIdx int, newLogs []Log) {
	oldIdx, newIdx := startIdx, 0
	oldBound, newBound := len(rf.logs), len(newLogs)

	for oldIdx < oldBound && newIdx < newBound {
		rf.logs[oldIdx] = newLogs[newIdx]
		oldIdx++
		newIdx++
	}

	rf.logs = rf.logs[:oldIdx]
	rf.logs = append(rf.logs, newLogs[newIdx:]...)
	//	rf.DPrintf("log updated(from %v), new logs: %v", startIdx, rf.logs)

	rf.persist()
}

func compareLog(lastLog *Log, logIndex int, logTerm int) bool {
	return lastLog == nil ||
		(logTerm > lastLog.Term ||
			(logTerm == lastLog.Term && logIndex >= lastLog.Index))
}
