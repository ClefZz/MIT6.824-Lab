package raft

import (
	"time"
	"labrpc"
)

type candidate struct {
	*Raft
	*eventLoop

	grantedNumber   int
	electionTimeout *time.Timer
}

func (candidate *candidate) issueAppend(args *AppendArgs) (re *AppendReply) {
	call := &appendCall{args, make(chan *AppendReply, 1)}
	if candidate.fireEvent(_MsgAppend, call) {
		return <-call.replyCh
	} else { // indicates candidate has stopped
		return nil
	}
}

func (candidate *candidate) issueRequest(args *RequestArgs) (re *RequestReply) {
	call := &requestCall{args, make(chan *RequestReply, 1)}
	if candidate.fireEvent(_MsgRequest, call) {
		return <-call.replyCh
	} else { // indicates candidate has stopped
		return nil
	}
}

func (candidate *candidate) issueQuery() int {
	var query queryCall = make(chan int, 1)
	if candidate.fireEvent(_MsgQuery, query) {
		return <-query
	} else {
		return -1
	}
}

type election struct {
	*candidate

	args RequestArgs
}

const (
	_MsgVoteGranted = "granted"
)

func (rf *Raft) makeCandidate() (c *candidate) {
	rf.votedFor = rf.me
	c = &candidate{
		Raft:      rf,
		eventLoop: makeEventLoop(len(rf.peers) * 2),

		grantedNumber: 0,
	}

	c.registerHandler(_MsgAppend, func(value interface{}) {
		c.handleAppend(value.(*appendCall))
	})

	c.registerHandler(_MsgRequest, func(value interface{}) {
		c.handleRequest(value.(*requestCall))
	})

	c.registerHandler(_MsgQuery, func(value interface{}) {
		value.(queryCall) <- c.currentTerm
	})

	c.registerHandler(_MsgVoteGranted, func(value interface{}) {
		c.handleVoteGranted(value.(int))
	})

	c.registerHandler(_MsgTimeout, func(value interface{}) {
		if value.(int) == c.currentTerm { // Check if this signal is from current term.
			c.launchElection()
		}
	})

	c.registerHandler(_MsgGreaterTerm, func(value interface{}) {
		futureTerm := value.(int)
		// check if we have changed our term even greater after the GreaterTerm signal had been issued
		if futureTerm >= c.currentTerm {
			c.stop()
			rf.currentTerm = futureTerm
			rf.votedFor = -1 // DON'T forget reset votedFor here!
			rf.persist()

			// switch to follower
			follower := rf.makeFollower()

			c.transferEventTo(follower.eventLoop, eventFilter)

			rf.setCharacter(follower)
			follower.loop()
		}
	})

	return
}

func (candidate *candidate) loop() {
	//candidate.DPrintf("- candidate -")
	candidate.launchElection() // start election at beginning
	candidate.start()
}

func (candidate *candidate) handleAppend(call *appendCall) {
	// NOTE: please notice the condition of fallback on rpc:
	// AppendEntries with term >= currentTerm, while
	// RequestVote with term > currentTerm
	if args := call.args; args.Term >= candidate.currentTerm {
		candidate.fireEvent(_MsgGreaterTerm, args.Term)
		candidate.fireEvent(_MsgAppend, call) // re-enqueue this append args to let it handled by follower
	} else {
		call.replyCh <- &AppendReply{
			Term:    candidate.currentTerm,
			Success: false,
		}
	}
}

func (candidate *candidate) handleRequest(call *requestCall) {
	if args := call.args; args.Term > candidate.currentTerm {
		candidate.fireEvent(_MsgGreaterTerm, args.Term)
		candidate.fireEvent(_MsgRequest, call) // re-enqueue this append args to let it handled follower
	} else {
		call.replyCh <- &RequestReply{
			Term:        candidate.currentTerm,
			VoteGranted: false,
		}
	}
}

func (candidate *candidate) handleVoteGranted(fromTerm int) {
	if fromTerm == candidate.currentTerm { // Check if the vote is from current term.
		candidate.grantedNumber++

		// Check if we have won the election.
		if candidate.grantedNumber >= candidate.majority {
			candidate.stop()

			rf := candidate.Raft
			leader := rf.makeLeader()
			// Switch to leader
			candidate.transferEventTo(leader.eventLoop, eventFilter)

			candidate.Raft.setCharacter(leader)
			leader.loop()
		}
	}
}

func (candidate *candidate) makeElection() (e *election) {
	// Prepare RequestVote's argument
	// Find the last log entry. {-1, -1} if no log ever exists.
	var lastLog *Log = nil
	lastLogIndex, lastLogTerm := -1, -1
	if logsNum := len(candidate.logs); logsNum > 0 {
		lastLog = &candidate.logs[logsNum-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}

	return &election{
		candidate: candidate,

		args: RequestArgs{
			Term:         candidate.currentTerm,
			CandidateId:  candidate.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: lastLogIndex,
		},
	}
}

func (candidate *candidate) launchElection() {
	// Start the election.
	candidate.currentTerm++
	candidate.persist() // Term changing, persist now.

	candidate.grantedNumber = 1 // granted number start from 1 (we have granted vote to ourselves)

	candidate.makeElection().start()
	candidate.resetTimer()
}

func (candidate *candidate) resetTimer() {
	if candidate.electionTimeout != nil {
		candidate.electionTimeout.Stop()
	}
	currentTerm := candidate.currentTerm // extract currentTerm to capture-by-value by the anonymous function
	candidate.electionTimeout = time.AfterFunc(electionTimeOut(), func() {
		candidate.fireEvent(_MsgTimeout, currentTerm)
	})
}

func (e *election) start() {
	for peerIndex, peer := range e.peers {
		if peerIndex != e.me {
			go e.sendTo(peer)
		}
	}
}

func (e *election) sendTo(peer *labrpc.ClientEnd) {
	re := &RequestReply{}

	if result := tryCall(peer, "Raft.RequestVote", &e.args, re); result {
		if re.VoteGranted {
			e.fireEvent(_MsgVoteGranted, e.args.Term)
		} else if re.Term > e.args.Term {
			e.fireEvent(_MsgGreaterTerm, re.Term)
		}
	} // we don't have to handle the network error.
}
