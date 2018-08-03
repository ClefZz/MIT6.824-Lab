package raft

import (
	"sync/atomic"
	"unsafe"
	"time"
	"math/rand"
	"labrpc"
)

const (
	_MsgAppend      = "append"
	_MsgRequest     = "request"
	_MsgQuery       = "query"
	_MsgGreaterTerm = "greater_term"
	_MsgTimeout     = "timeout"
)

type AppendArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendReply struct {
	Term          int
	BaselineTerm  int
	BaselineIndex int
	Success       bool
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

const _StartWork = -1
const _NetworkTimeout = 250 * time.Millisecond
const _HeartbeatTimeout = 175 * time.Millisecond

func electionTimeOut() time.Duration {
	return time.Duration(rand.Int()%350+350) * time.Millisecond
}

func eventFilter(msg *eventMsg) bool {
	return msg.msgType == _MsgAppend ||
		msg.msgType == _MsgRequest ||
		msg.msgType == _MsgQuery
}

func tryCall(peer *labrpc.ClientEnd, svcMeth string, args interface{}, reply interface{}) bool {
	ch := make(chan bool, 1)
	go func() {
		ch <- peer.Call(svcMeth, args, reply)
	}()

	select {
	case result := <-ch:
		return result
	case <-time.After(_NetworkTimeout):
		return false
	}
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	args := []interface{}{rf.me, rf.currentTerm}
	args = append(args, a...)
	DPrintf("[%v, %v] "+format, args...)
}

func (rf *Raft) nextFakeIndex() int {
	rf.fakeIndex++
	return rf.fakeIndex
}

// Updates fake index as the last valid one in `logs`.
func (rf *Raft) updateFakeIndex() {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if log := rf.logs[i]; log.FakeIndex != 0 {
			rf.fakeIndex = log.FakeIndex
			break
		}
	}
}

func (rf *Raft) setCharacter(c character) {
	atomic.StorePointer(&rf.currentCharacter, unsafe.Pointer(&c))
}

func (rf *Raft) getCharacter() character {
	return *(*character)(atomic.LoadPointer(&rf.currentCharacter))
}

func (rf *Raft) commit() {
	if rf.commitIndex > rf.lastApplied {
		for _, log := range rf.logs[rf.getLogOffsetByIndex(rf.lastApplied)+1 : rf.getLogOffsetByIndex(rf.commitIndex)+1] {
			rf.applyCh <- ApplyMsg{
				CommandValid: log.FakeIndex != 0,
				Command:      log.Command,
				CommandIndex: log.FakeIndex,
			}
			rf.lastApplied = log.Index
		}
	}
}

const (
	_LogBeginningGuard = -1 // log doesn't exist yet
	_LogNotFound       = -2 // invalid log's index
)

func (rf *Raft) getLog(index, term int) *Log {
	if idx := rf.getLogOffset(index, term); idx >= 0 {
		return &rf.logs[idx]
	} else {
		return nil
	}
}

func (rf *Raft) getLogByIndex(index int) *Log {
	if idx := rf.getLogOffsetByIndex(index); idx >= 0 {
		return &rf.logs[idx]
	}
	return nil
}

// Returns the last log whose term <= given `term`
func (rf *Raft) seekBaseline(term int) (baseline *Log) {
	for i, log := range rf.logs {
		if log.Term >= term {
			if i > 0 {
				return &rf.logs[i-1]
			} else {
				return nil
			}
		}
		//if log.Term > term {
		//	break
		//}
		//
		//if baseline == nil || baseline.Term < log.Term {
		//	baseline = &rf.logs[i]
		//}
	}
	return
}

// get the offset in logs by given log's index
//
// if `index` < 0, then return _LogBeginningGuard
//
// if `index` doesn't match any log in current logs collection,
// then return _LogNotFound
func (rf *Raft) getLogOffsetByIndex(index int) int {
	if index < 0 {
		return _LogBeginningGuard
	}
	realIndex := index - rf.baseLogIndex
	if realIndex < 0 || realIndex >= len(rf.logs) {
		return _LogNotFound
	}
	return realIndex
}

func (rf *Raft) getLogOffset(index, term int) int {
	if index == -1 && term == -1 {
		return _LogBeginningGuard
	}

	if idx := rf.getLogOffsetByIndex(index); idx >= 0 {
		if rf.logs[idx].Term == term {
			return idx
		}
	}
	return _LogNotFound
}

func (rf *Raft) compareLog(logIndex int, logTerm int) bool {
	var lastLog *Log = nil
	if lastIndex := len(rf.logs); lastIndex > 0 {
		lastLog = &rf.logs[lastIndex-1]
	}

	return lastLog == nil ||
		(logTerm > lastLog.Term ||
			(logTerm == lastLog.Term && logIndex >= lastLog.Index))
}

func (rf *Raft) lastIndex() int {
	if logsNum := len(rf.logs); logsNum > 0 {
		return rf.logs[logsNum-1].Index
	} else {
		return -1
	}
}

type character interface {
	loop()
	issueAppend(*AppendArgs) *AppendReply
	issueRequest(*RequestArgs) *RequestReply
	issueQuery() int
	stop()
}

type commandCall struct {
	command  interface{}
	retrying bool
	resultCh chan int
}

type appendCall struct {
	args    *AppendArgs
	replyCh chan *AppendReply
}

type requestCall struct {
	args    *RequestArgs
	replyCh chan *RequestReply
}

type queryCall chan int
