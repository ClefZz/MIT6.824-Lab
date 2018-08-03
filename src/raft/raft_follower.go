package raft

import (
	"time"
)

type follower struct {
	*Raft
	*eventLoop

	electionTimeout *time.Timer
	timerId         int // to distinguish the current valid timer
}

func (follower *follower) issueAppend(args *AppendArgs) (re *AppendReply) {
	call := &appendCall{args, make(chan *AppendReply, 1)}
	if follower.fireEvent(_MsgAppend, call) {
		return <-call.replyCh
	} else {
		return nil
	}
}

func (follower *follower) issueRequest(args *RequestArgs) (re *RequestReply) {
	call := &requestCall{args, make(chan *RequestReply, 1)}
	if follower.fireEvent(_MsgRequest, call) {
		return <-call.replyCh
	} else {
		return nil
	}
}

func (follower *follower) issueQuery() int {
	var query queryCall = make(chan int)
	if follower.fireEvent(_MsgQuery, query) {
		return <-query
	} else {
		return -1
	}
}

func (rf *Raft) makeFollower() (f *follower) {
	f = &follower{
		Raft: rf,
		// common buffer size, to avoid block during `transferEventTo` when changing character
		eventLoop: makeEventLoop(len(rf.peers) * 2),
		timerId:   -1,
	}

	f.registerHandler(_MsgAppend, func(value interface{}) {
		f.handleAppend(value.(*appendCall))
	})

	f.registerHandler(_MsgRequest, func(value interface{}) {
		f.handleRequest(value.(*requestCall))
	})

	f.registerHandler(_MsgQuery, func(value interface{}) {
		value.(queryCall) <- f.currentTerm
	})

	f.registerHandler(_MsgTimeout, func(value interface{}) {
		f.handlePromotion(value.(int))
	})

	return
}

func (follower *follower) loop() {
	// follower.DPrintf("- follower -")
	follower.resetTimer()
	follower.start()
}

func (follower *follower) handleAppend(call *appendCall) {
	args := call.args
	re := &AppendReply{
		Term:          args.Term,
		BaselineIndex: -1,
		BaselineTerm:  -1,
		Success:       false,
	}

	// 1. Is it stale or do we need to change our term?
	if args.Term >= follower.currentTerm {
		//if args.Term > follower.currentTerm {
		//	follower.DPrintf("Term Change | %v -> %v, Cause: Append", follower.currentTerm, args.Term)
		//}
		follower.currentTerm = args.Term // 1.1 update term
	} else {
		re.Term = follower.currentTerm // 1.2 respond failure to a stale request immediately
		return
	}

	// 2. Check arguments and react.
	if len(args.Entries) <= 0 { // 2.1 heartbeat message, succeed
		re.Success = true
	} else // 2.2 the previous log entry matches, succeed (-1 means it's from the very beginning)
	if offset := follower.getLogOffset(args.PrevLogIndex, args.PrevLogTerm); offset >= _LogBeginningGuard {
		re.Success = true

		// 2.2.1 update logs
		follower.logs = append(follower.logs[0:offset+1], args.Entries...)
		follower.updateFakeIndex()
	} else // 3.3 the previous log entry mismatches, fail the request.
	// Here is the fast rollback at follower side.
	// Find an earlier log owned by us.
	if baseline := follower.seekBaseline(args.PrevLogTerm); baseline != nil {
		re.BaselineTerm = baseline.Index
		re.BaselineTerm = baseline.Term
	}

	// 4. commit if necessary
	if re.Success &&
		args.LeaderCommit > follower.commitIndex &&
		follower.lastIndex() > follower.commitIndex {
		follower.commitIndex = min(follower.lastIndex(), args.LeaderCommit)
		//follower.DPrintf("Follower Commit [%v]", follower.commitIndex)
		follower.commit()
	}

	if re.Term == args.Term {
		//follower.DPrintf("Timer Reset | Id: %v, Cause: Append", follower.timerId+1)
		follower.resetTimer()
	}

	follower.persist() // persist before replying

	call.replyCh <- re
}

func (follower *follower) handleRequest(call *requestCall) {
	args := call.args
	re := &RequestReply{args.Term, false}

	// Should we change our term?
	if args.Term > follower.currentTerm {
		//if args.Term > follower.currentTerm {
		//	follower.DPrintf("Term Change | %v -> %v, Cause: Request", follower.currentTerm, args.Term)
		//}
		follower.currentTerm = args.Term
		follower.votedFor = -1 // don't forget reset votedFor
	}

	// Should we grant our vote?
	if args.Term >= follower.currentTerm &&
		(follower.votedFor == -1 ||
			follower.votedFor == args.CandidateId) &&
		follower.compareLog(args.LastLogIndex, args.LastLogTerm) {
		re.VoteGranted = true
		follower.votedFor = args.CandidateId
	}

	if re.VoteGranted {
		//follower.DPrintf("Timer Reset | Id: %v, Cause: Request", follower.timerId+1)
		follower.resetTimer()
	}

	follower.persist() // persist before replying

	call.replyCh <- re
}

func (follower *follower) handlePromotion(timerId int) {
	if timerId == follower.timerId { // make sure it isn't a stale signal.
		follower.stop()
		//follower.DPrintf("Timer Trigger | Id: %v", timerId)

		// switch to candidate
		rf := follower.Raft
		candidate := rf.makeCandidate()

		follower.transferEventTo(candidate.eventLoop, eventFilter)

		rf.setCharacter(candidate)
		candidate.loop()
	}
}

func (follower *follower) resetTimer() {
	if follower.electionTimeout != nil {
		follower.electionTimeout.Stop()
	}

	// New timer's launch expires the old one, but the old one's signal may still be
	// issued because of concurrency.
	// So, instead of using lock or CAS, we adopt a unique timerId to prevent a stale
	// signal from taking effect.
	//
	// NOTE: Identifying a timer by term is not ok, since term can change without launching
	// a new timer (caused by failed AppendEntries & RequestVote with greater term). In that
	// situation, the timer launched in previous term is still valid.
	follower.timerId++
	id := follower.timerId // extract follower.timerId to capture-by-value by anonymous function
	follower.electionTimeout = time.AfterFunc(electionTimeOut(), func() {
		follower.fireEvent(_MsgTimeout, id)
	})
}
