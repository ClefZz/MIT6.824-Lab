// raft_leader
package raft

import (
	"bytes"
	list "container/list"
	"fmt"
	"labrpc"
	"sync"
	"time"
)

func (rf *Raft) runLeader() {
	for {
		select {
		case <-rf.stateChan[Quit]:
			return
		case <-rf.stateChan[Leader]:
			//			rf.DPrintf("leader %v", rf.logs)
			rf.performLeader()
		}
	}

}

func (rf *Raft) performLeader() {
	leader := rf.makeLeader()
	go leader.loop()

	select {
	case <-rf.stateChan[Quit]:
		leader.terminate()

	case <-leader.fallbackChan: // now leader has terminated itself
		rf.drainCommandQueue()
		rf.stateChan[Follower] <- true
	}
}

type replicationRecord struct {
	logIndex int
	replicas int
}

type majorityCounter struct {
	l         *list.List
	majority  int
	threshold int
}

func (counter *majorityCounter) show(tag string, rf *Raft) {
	var buf bytes.Buffer

	for e := counter.l.Front(); e != nil; e = e.Next() {
		buf.WriteString(fmt.Sprintf("%v ", e.Value))
	}
	rf.DPrintf("[%v] majority counter: threshold: %v, %v", tag, counter.threshold, buf.String())
}

func (counter *majorityCounter) count(idx int) int {
	if idx <= counter.threshold {
		return -1
	}

	reg := counter.l
	e := reg.Front()

	canCommit := -1

	for e := reg.Front(); e != nil; e = e.Next() {
		record := e.Value.(*replicationRecord)

		if record.logIndex < idx {
			record.replicas++
			if record.replicas >= counter.majority {
				canCommit = record.logIndex
			}
		} else {
			break
		}
	}

	if e == nil {
		reg.PushBack(&replicationRecord{idx, 1})
	} else if record := e.Value.(*replicationRecord); record.logIndex == idx {
		record.replicas++
		if record.replicas >= counter.majority {
			canCommit = record.logIndex
		}
	} else {
		reg.InsertBefore(&replicationRecord{idx, 1}, e)
	}

	if canCommit != -1 {
		counter.truncate(canCommit)
	}

	return canCommit
}

func (counter *majorityCounter) truncate(threshold int) {
	counter.threshold = threshold

	for e := counter.l.Front(); e != nil; {
		if e.Value.(*replicationRecord).logIndex <= threshold {
			next := e.Next()
			counter.l.Remove(e)
			e = next
		} else {
			break
		}
	}
}

type leader struct {
	rf *Raft

	mu  sync.RWMutex
	sig *sync.Cond

	counter        *majorityCounter
	heartBeatTimer <-chan time.Time

	fallbackChan chan int
	reportChan   chan int
	stop         chan bool
}

func (rf *Raft) makeLeader() *leader {
	leader := &leader{
		rf:           rf,
		counter:      &majorityCounter{list.New(), calcMajority(len(rf.peers)), -1},
		reportChan:   make(chan int, len(rf.peers)),
		fallbackChan: make(chan int, 1),
		stop:         make(chan bool),
	}
	leader.sig = sync.NewCond(leader.mu.RLocker())
	return leader
}

func (leader *leader) isTerminated() bool {
	select {
	case <-leader.stop:
		return true
	default:
		return false
	}
}

func (leader *leader) terminate() {
	select {
	case <-leader.stop:
	default:
		close(leader.stop)
		leader.mu.Lock()
		leader.sig.Broadcast()
		leader.mu.Unlock()
	}
}

func (leader *leader) fallback(term int, votedFor int) {
	leader.terminate()
	leader.rf.currentTerm = term
	leader.rf.votedFor = votedFor
	leader.rf.persist()
	close(leader.fallbackChan)
}

func (leader *leader) resetTimer() {
	leader.heartBeatTimer = heartBeatTimeOut()
}

func (leader *leader) prepareCommand(cmd *commandTask) {
	rf := leader.rf

	leader.mu.Lock()

	newIdx := rf.lastIndex() + 1
	leader.counter.count(newIdx)

	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Index: newIdx, Command: cmd.command})
	rf.persist()

	leader.sig.Broadcast()
	leader.mu.Unlock()

	leader.resetTimer()
	cmd.index <- newIdx
}

func (leader *leader) handleReport(report int) {
	if report > 0 { // response with greater term
		leader.fallback(report, -1)
	} else { // success response, update commitIndex
		leader.tryCommit(-report)
	}
}

func (leader *leader) handleAppendCall(call *ACall) {
	rf := leader.rf
	args := call.args
	re := populateAppendReply(rf, args, false)
	call.reply <- re

	if processAppend(rf, args, re) {
		leader.fallback(args.Term, -1)
	}
}

func (leader *leader) handleRequestCall(call *RCall) {
	rf := leader.rf
	args := call.args
	re := populateRequestReply(rf, args)
	call.reply <- re
	// assertion can be made that never is request with same term to be granted

	if postProcessRequest(rf, args, re) {
		leader.fallback(args.Term, rf.votedFor)
	}
}

func (leader *leader) tryCommit(idx int) {
	rf := leader.rf
	if canCommit := leader.counter.count(idx); canCommit >= 0 {
		leader.mu.Lock()
		rf.commitIndex = canCommit
		leader.mu.Unlock()

		if rf.logs[findLogOfIndex(rf, canCommit)].Term == rf.currentTerm {
			// rf.DPrintf("logs up to %v can be committed", canCommit)
			rf.commit()
		}
	}
}

func (leader *leader) report(value int) {
	select {
	case <-leader.stop:
	default:
		leader.reportChan <- value
	}
}

func (leader *leader) runSenders() {
	rf, lastIndex := leader.rf, leader.rf.lastIndex()
	for i, _ := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
		if i != rf.me {
			go leader.runSenderOf(i)
		}
	}
}

func (leader *leader) runSenderOf(peer int) {
	rf := leader.rf
	for !leader.isTerminated() {
		leader.mu.RLock()

		for rf.nextIndex[peer] > rf.lastIndex() { // todo is exhausted, wait for signal
			leader.sig.Wait()
			if leader.isTerminated() {
				return
			}
		}
		args, logEnd := leader.makeAppendArgs(peer)

		leader.mu.RUnlock()

		if leader.isTerminated() {
			return
		}
		// rf.DPrintf("prepare task[%v]: %v - %v | commitIndex: %v, prevLog: %v | all logs: %v", peer, realStart, realEnd, commitIndex, prevLog, rf.logs)
		status, value := doSendAppend(args, rf.peers[peer])

		// rf.DPrintf("task[%v] done | %v", peer, result)
		if leader.isTerminated() {
			return
		} else if status == OperationSucceed {
			// rf.DPrintf("task[%v] done | %v", peer, rf.logs[realStart:realEnd+1])
			rf.nextIndex[peer] = logEnd + 1
			rf.matchIndex[peer] = logEnd

			leader.report(-logEnd)

		} else if status == AppendRollback {
			followerLastKnown := value.(Log)
			newStart := findLog(rf, followerLastKnown.Index, followerLastKnown.Term) + 1
			if newStart == -1 {
				if lastKnown := findLogOfTerm(rf, followerLastKnown.Term); lastKnown != nil {
					newStart = lastKnown.Index
				} else {
					newStart = 0
				}
			}
			rf.nextIndex[peer] = newStart // sync failure, decrease nextIdx[peer] and retry

		} else if status == Fallback {
			// rf.DPrintf("task[%v] receive fallback", peer)
			leader.report(value.(int))
			return
		} // otherwise network failure, just retry
	}
}

func (leader *leader) makeAppendArgs(peer int) (args *AppendArgs, lastIndex int) {
	rf := leader.rf
	logStart, logEnd := rf.nextIndex[peer], rf.lastIndex()
	realStart, realEnd := findLogOfIndex(rf, logStart), findLogOfIndex(rf, logEnd)
	if realStart < 0 || realEnd >= len(rf.logs) {
		panic(fmt.Sprintf("out of range: logStart: %v, logEnd: %v, realStart: %v, realEnd: %v", logStart, logEnd, realStart, realEnd))
	}

	prevIndex, prevTerm := -1, -1
	if realStart > 0 {
		prevLog := &rf.logs[realStart-1]
		prevIndex, prevTerm = prevLog.Index, prevLog.Term
	}

	return &AppendArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      rf.logs[realStart : realEnd+1],
	}, logEnd
}

func (leader *leader) heartBeat() {
	if leader.isTerminated() {
		return
	}

	rf := leader.rf
	args := &AppendArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,

		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      nil,
	}

	for i, peer := range rf.peers {
		if i != rf.me {
			go func(i int, peer *labrpc.ClientEnd) {
				if status, value := doSendAppend(args, peer); status == Fallback {
					// rf.DPrintf("heartbeat fallback: %v, %v", i, result)
					leader.report(value.(int))
				}
			}(i, peer)
		}
	}
	leader.resetTimer()
}

func (leader *leader) loop() {
	leader.runSenders()
	leader.heartBeat()

	for !leader.isTerminated() {
		select {
		case <-leader.stop:
			return

		case <-leader.heartBeatTimer:
			leader.heartBeat()

		case q := <-leader.rf.query:
			q.ch <- leader.rf.currentTerm
			q.ch <- Leader

		case cmd := <-leader.rf.commandQueue: // new task arrived, update task
			leader.prepareCommand(cmd)

		case call := <-leader.rf.appendCallQueue:
			leader.handleAppendCall(call)

		case call := <-leader.rf.requestCallQueue:
			leader.handleRequestCall(call)

		case report := <-leader.reportChan:
			leader.handleReport(report)
		}
	}
}

func doSendAppend(args *AppendArgs, peer *labrpc.ClientEnd) (int, interface{}) {
	re := &AppendReply{}
	if peer.Call("Raft.AppendEntries", args, re) {
		if re.Success {
			return OperationSucceed, nil
		} else {
			if re.Term > args.Term {
				return Fallback, re.Term
			} else {
				return AppendRollback, Log{Term: re.PrevKnownTerm, Index: re.PrevKnownIndex}
			}
		}
	} else {
		return CallFail, nil
	}
}
