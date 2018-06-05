package raft

import "time"

func (rf *Raft) runFollower() {
	for {
		select {
		case <-rf.stateChan[Quit]:
			return
		case <-rf.stateChan[Follower]:
			//			rf.DPrintf("follower %v", rf.logs)
			rf.performFollower()
		}
	}
}

func (rf *Raft) performFollower() {
	follower := rf.makeFollower()
	go follower.loop()

	select {
	case <-rf.stateChan[Quit]:
		follower.terminate()

	case <-follower.promoteChan:
		rf.stateChan[Candidate] <- true
	}
}

type follower struct {
	rf *Raft

	promoteChan chan bool

	electionTimer <-chan time.Time
	stop          chan bool
}

func (rf *Raft) makeFollower() *follower {
	return &follower{
		rf:          rf,
		promoteChan: make(chan bool),
		stop:        make(chan bool),
	}
}

func (follower *follower) isTerminated() bool {
	select {
	case <-follower.stop:
		return true
	default:
		return false
	}
}

func (follower *follower) terminate() {
	select {
	case <-follower.stop:
	default:
		close(follower.stop)
	}
}

func (follower *follower) promote() {
	follower.terminate()
	close(follower.promoteChan)
}

func (follower *follower) handleAppendCall(call *ACall) {
	rf, args := follower.rf, call.args
	re := populateAppendReply(rf, args, true)
	processAppend(rf, args, re)
	call.reply <- re

	if re.Term >= rf.currentTerm {
		follower.resetTimer()
	}
}

func (follower *follower) handleRequestCall(call *RCall) {
	rf, args := follower.rf, call.args
	re := populateRequestReply(rf, args)
	postProcessRequest(rf, args, re)
	call.reply <- re

	if re.VoteGranted {
		follower.resetTimer()
	}
}

func (follower *follower) resetTimer() {
	follower.electionTimer = electionTimeOut()
}

func (follower *follower) loop() {
	follower.resetTimer()

	for !follower.isTerminated() {
		select {
		case <-follower.stop:
			return

		case q := <-follower.rf.query:
			q.ch <- follower.rf.currentTerm
			q.ch <- Follower

		case <-follower.electionTimer:
			follower.promote()

		case call := <-follower.rf.requestCallQueue:
			follower.handleRequestCall(call)

		case call := <-follower.rf.appendCallQueue:
			follower.handleAppendCall(call)
		}

	}

}
