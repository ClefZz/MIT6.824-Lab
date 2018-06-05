package raft

import (
	"labrpc"
	"time"
)

func (rf *Raft) runCandidate() {
	for {
		select {
		case <-rf.stateChan[Quit]:
			return

		case <-rf.stateChan[Candidate]:
			//			rf.DPrintf("candidate %v", rf.logs)
			rf.performCandidate()
		}
	}
}

func (rf *Raft) performCandidate() {
	candidate := rf.makeCandidate()
	go candidate.loop()

	select {
	case <-rf.stateChan[Quit]:

	case <-candidate.fallbackChan:
		rf.stateChan[Follower] <- true

	case <-candidate.promoteChan:
		rf.stateChan[Leader] <- true
	}
}

type candidate struct {
	rf *Raft

	currentTask *electionTask

	waitTimer    <-chan time.Time
	fallbackChan chan bool
	promoteChan  chan bool

	stop chan bool
}

func (rf *Raft) makeCandidate() *candidate {
	return &candidate{
		rf:           rf,
		fallbackChan: make(chan bool, 1),
		promoteChan:  make(chan bool, 1),
		stop:         make(chan bool),
	}
}

func (candidate *candidate) resetTimer() {
	candidate.waitTimer = electionTimeOut()
}

func (candidate *candidate) makeRequestArgs() (args *RequestArgs) {
	rf := candidate.rf
	args = &RequestArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}

	if lastLog := rf.lastLog(); lastLog != nil {
		args.LastLogIndex = lastLog.Index
		args.LastLogTerm = lastLog.Term
	}
	return
}

func (candidate *candidate) isTerminated() bool {
	select {
	case <-candidate.stop:
		return true
	default:
		return false
	}
}

func (candidate *candidate) terminate() {
	if curTask := candidate.currentTask; curTask != nil {
		curTask.terminate()
	}
	close(candidate.stop)
}

func (candidate *candidate) promote() {
	candidate.terminate()
	close(candidate.promoteChan)
}

func (candidate *candidate) fallback(term, votedFor int) {
	candidate.terminate()
	candidate.rf.currentTerm = term
	candidate.rf.votedFor = votedFor
	candidate.rf.persist()
	close(candidate.fallbackChan)
}

func (candidate *candidate) launchElection() {
	if oldTask := candidate.currentTask; oldTask != nil {
		oldTask.terminate()
	}

	rf := candidate.rf
	rf.currentTerm++
	rf.votedFor = rf.me

	candidate.currentTask = &electionTask{
		rf:         rf,
		args:       candidate.makeRequestArgs(),
		granted:    1,
		respChan:   make(chan int, len(rf.peers)),
		reportChan: make(chan int, 1),
		stop:       make(chan bool),
	}
	candidate.currentTask.runSenders()
	go candidate.currentTask.waitForResult()

	candidate.resetTimer()
}

func (candidate *candidate) handleReport(report int) {
	if report == OperationSucceed {
		candidate.promote()
	} else {
		candidate.fallback(report, -1)
	}
}

func (candidate *candidate) handleRequestCall(call *RCall) {
	rf := candidate.rf
	args := call.args
	re := populateRequestReply(rf, args)
	call.reply <- re

	// assertion can be made that never is request with same term granted
	if postProcessRequest(rf, args, re) {
		candidate.fallback(args.Term, rf.votedFor)
	}
}

func (candidate *candidate) handleAppendCall(call *ACall) {
	rf := candidate.rf
	args := call.args
	re := populateAppendReply(rf, args, false)
	call.reply <- re

	if processAppend(rf, args, re) {
		candidate.fallback(args.Term, -1)
	}
}

func (candidate *candidate) loop() {
	candidate.launchElection()

	for !candidate.isTerminated() {
		select {
		case <-candidate.stop:
			return

		case q := <-candidate.rf.query:
			q.ch <- candidate.rf.currentTerm
			q.ch <- Candidate

		case <-candidate.waitTimer:
			candidate.launchElection()

		case report := <-candidate.currentTask.reportChan:
			candidate.handleReport(report)

		case call := <-candidate.rf.appendCallQueue:
			candidate.handleAppendCall(call)

		case call := <-candidate.rf.requestCallQueue:
			candidate.handleRequestCall(call)
		}
	}

}

type electionTask struct {
	rf *Raft

	args    *RequestArgs
	granted int

	respChan   chan int
	reportChan chan int
	stop       chan bool
}

func (task *electionTask) terminate() {
	select {
	case <-task.stop:
	default:
		close(task.stop)
	}
}

func (task *electionTask) isTerminated() bool {
	select {
	case <-task.stop:
		return true
	default:
		return false
	}
}

func (task *electionTask) report(report int) {
	task.terminate()
	task.reportChan <- report
}

func (task *electionTask) runSenders() {
	rf := task.rf
	for i, peer := range rf.peers {
		if i != rf.me {
			go func(i int, peer *labrpc.ClientEnd) {
				result := doSendRequest(task.args, peer)
				select {
				case <-task.stop:
				case task.respChan <- result:
				}
			}(i, peer)
		}
	}
}

func (task *electionTask) waitForResult() {
	majority := calcMajority(len(task.rf.peers))
	for !task.isTerminated() {
		select {
		case <-task.stop:
			return

		case resp := <-task.respChan:
			if resp == OperationSucceed {
				task.granted++
				if task.granted >= majority {
					task.report(OperationSucceed)
				}
			} else if resp > 0 {
				task.report(resp)
			}
		}
	}
}

func doSendRequest(args *RequestArgs, peer *labrpc.ClientEnd) int {
	re := &RequestReply{}
	if result := peer.Call("Raft.RequestVote", args, re); result {
		if re.VoteGranted {
			return OperationSucceed
		} else {
			if re.Term > args.Term {
				return re.Term
			} else {
				return VoteDenied
			}
		}
	} else {
		return CallFail
	}
}
