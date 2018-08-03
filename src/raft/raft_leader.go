package raft

import (
	"labrpc"
	"time"
	"sync/atomic"
)

type leader struct {
	*Raft
	*eventLoop

	tasks      []*appendTask
	readyTasks chan *appendTask
	counter    *replicaCounter
}

func (leader *leader) issueAppend(args *AppendArgs) (re *AppendReply) {
	call := &appendCall{args, make(chan *AppendReply, 1)}
	if leader.fireEvent(_MsgAppend, call) {
		return <-call.replyCh
	} else {
		return nil
	}
}

func (leader *leader) issueRequest(args *RequestArgs) (re *RequestReply) {
	call := &requestCall{args, make(chan *RequestReply, 1)}
	if leader.fireEvent(_MsgRequest, call) {
		return <-call.replyCh
	} else {
		return nil
	}
}

func (leader *leader) issueQuery() int {
	var query queryCall = make(chan int, 1)
	if leader.fireEvent(_MsgQuery, query) {
		return <-query
	} else {
		return -1
	}
}

func (leader *leader) issueCommand(cmd interface{}) int {
	//leader.DPrintf("Issue Command | [%v]", cmd)
	resultCh := make(chan int)
	if leader.fireEvent(_MsgCommand, &commandCall{cmd, false, resultCh}) {
		return <-resultCh
	} else {
		return -1
	}
}

//   We know that there can be more than one leaders during a short time
// after a partitioned leader is reconnected. Then, in the test case, the
// following command can be issued to the false leader (the partitioned one).
// In that condition the correctness still holds. However, this causes retry,
// and as a result, much more time consumption (2 seconds per retry).
//   So we adopt such an optimization:
//   1. The leader record how many times we have failed to send rpc to a certain peer.
//   2. If failure numbers exceeds the `_DisconnectedThreshold`, we assume the peer is
//      disconnected with us.
//   3. If the number of disconnected peers exceeds the majority, we assume that we are
//      offline
//   4. When we are offline, before accepting the command issued to us, we send heartbeat
//      to peers to confirm if we are still the leader. If some reply results in fallback
//      or we remain offline, then reject the command.
const _DisconnectedThreshold = 4

// Task manages the rpc sending of a certain peer.
// It is a state machine, driven by the event loop.
//                                    pend()
//                    |-------------------------------------------|
//       pend()      \|/    handlePending()            <set>      |
// Idle --------> Pending <-----------------> Running ------> Completed
// /|\               /|\                         |                |
//  |                 |-------- Retrying <-------|                |
//  |                   pend()             <set>                  |
//  |-------------------------------------------------------------|
//                         handleCompletion()
//   Logically, there is a pending task queue and a completed task queue.
//   1. pend() method always alter task state to Pending. If the previous
//      state is Idle or Retrying, it meanwhile enqueues task to the pending
//      queue.
//   2. The event loop polls task from the pending queue, build the argument,
//      alter its state to running, and start its rpc call in a pooled goroutine.
//   3. The rpc call completes. Then according to its result, it may turn to
//      completed if success, enqueue to the completed queue. Or turn to retrying,
//      enqueue to the completed queue.
//   4. The event loop pools task from the completed queue. If its state is Pending,
//      enqueue it to pending queue. If its state if Complete, alter its state to Idle.
// We leverage CAS to make sure above steps are all atomic.
const ( // Task's state
	_TaskIdle      uint32 = 0
	_TaskPending          = 1
	_TaskRunning          = 2
	_TaskRetrying         = 3
	_TaskCompleted        = 4
)
const (
	_MsgPending    = "pending"
	_MsgCommand    = "command"
	_MsgCompletion = "completion"
)

type appendTask struct {
	leader *leader

	peer      *labrpc.ClientEnd
	peerIndex int

	state             uint32
	lastNetworkFailed bool
	failureNumber     int

	args       *AppendArgs
	nextIndex  int
	matchIndex int

	heartbeatTimeout *time.Timer
}

func (rf *Raft) makeLeader() (l *leader) {
	l = &leader{
		Raft:      rf,
		eventLoop: makeEventLoop(len(rf.peers) * 2),

		tasks:      make([]*appendTask, len(rf.peers)),
		readyTasks: make(chan *appendTask, len(rf.peers)-1),
		counter:    makeReplicaCounter(calcMajority(len(rf.peers))),
	}

	l.registerHandler(_MsgCommand, func(value interface{}) {
		l.handleCommand(value.(*commandCall))
	})

	l.registerHandler(_MsgPending, func(value interface{}) {
		l.handlePending(value.(*appendTask))
	})

	l.registerHandler(_MsgCompletion, func(value interface{}) {
		l.handleCompletion(value.(*appendTask))
	})

	l.registerHandler(_MsgAppend, func(value interface{}) {
		l.handleAppend(value.(*appendCall))
	})

	l.registerHandler(_MsgRequest, func(value interface{}) {
		l.handleRequest(value.(*requestCall))
	})

	l.registerHandler(_MsgQuery, func(value interface{}) {
		value.(queryCall) <- rf.currentTerm
	})

	l.registerHandler(_MsgGreaterTerm, func(value interface{}) {
		l.stop()
		rf.currentTerm = value.(int)
		rf.votedFor = -1
		rf.persist()
		// switch to follower
		follower := rf.makeFollower()

		l.transferEventTo(follower.eventLoop, func(msg *eventMsg) bool {
			if msg.msgType == _MsgCommand {
				msg.payload.(*commandCall).resultCh <- -1
			}
			return eventFilter(msg)
		})

		rf.setCharacter(follower)
		follower.loop()
	})

	lastLogIndex := -1
	if logNum := len(rf.logs); logNum > 0 {
		lastLogIndex = rf.logs[logNum-1].Index
	}

	for peerIndex, peer := range rf.peers {
		if peerIndex != rf.me {
			task := &appendTask{
				leader:    l,
				peer:      peer,
				peerIndex: peerIndex,

				state:             _TaskIdle,
				failureNumber:     0,
				lastNetworkFailed: false,

				nextIndex:  lastLogIndex + 1,
				matchIndex: -1,
			}
			task.heartbeatTimeout = time.AfterFunc((1<<20)*time.Hour, func() { task.pend() })

			l.tasks[peerIndex] = task
			l.runSenderWorker()
		}
	}

	l.fireEvent(_MsgCommand, &commandCall{_StartWork, false, make(chan int, 1),})
	l.pendAll()

	return
}

func (leader *leader) loop() {
	//leader.DPrintf("- leader -")
	leader.start()
}

func (leader *leader) handleAppend(call *appendCall) {
	if args := call.args; args.Term > leader.currentTerm {
		leader.fireEvent(_MsgGreaterTerm, args.Term)
		leader.fireEvent(_MsgAppend, call) // re-enqueue this append args to let it handled follower
	} else {
		call.replyCh <- &AppendReply{
			Term:    leader.currentTerm,
			Success: false,
		}
	}
}

func (leader *leader) handleRequest(call *requestCall) {
	if args := call.args; args.Term > leader.currentTerm {
		leader.fireEvent(_MsgGreaterTerm, args.Term)
		leader.fireEvent(_MsgRequest, call)
	} else {
		call.replyCh <- &RequestReply{
			Term:        leader.currentTerm,
			VoteGranted: false,
		}
	}
}

func (leader *leader) handleCommand(cmd *commandCall) {
	if leader.isOffline() {
		if cmd.command != _StartWork && cmd.retrying { // only retry once except _StartWork.
			cmd.resultCh <- -1 // reject
		} else {
			cmd.retrying = true
			leader.pendAll()                   // launch a round of send,
			leader.fireEvent(_MsgCommand, cmd) // and then retry the command
		}
		return
	}

	log := Log{
		Term:    leader.currentTerm,
		Index:   len(leader.logs),
		Command: cmd.command,
	}

	// Fake index is the index returned to the tester.
	// We adopt an optimization that leader issue a special
	// StartWork log when it start working. Due to the fucking idiot
	// test case, we have to adopt another index sequence to ensure
	// index continuity. Then it's the FakeIndex.
	// FakeIndex set to 0 for StartWork logs, or increase one by one
	// for normal logs.
	if c, ok := cmd.command.(int); ok && c == _StartWork {
		log.FakeIndex = 0
	} else {
		log.FakeIndex = leader.nextFakeIndex()
	}

	leader.logs = append(leader.logs, log)

	leader.counter.count(log.Index, leader.me)

	leader.persist() // persist before replying

	cmd.resultCh <- log.FakeIndex // respond

	//leader.DPrintf("Command [%v, %v] | Accepted [%v]", log.Index, log.FakeIndex, cmd.command)
	leader.pendAll()
}

func (leader *leader) handlePending(task *appendTask) {
	// take a task from pending_queue, alter state, prepare args and dispatch it to sender thread pool
	atomic.StoreUint32(&task.state, _TaskRunning)

	if task.lastNetworkFailed {
		task.failureNumber++
	} else {
		task.failureNumber = 0
	}

	task.args = task.prepareArgs()
	leader.readyTasks <- task
}

func (leader *leader) handleCompletion(task *appendTask) {
	task.failureNumber = 0

	if task.tryCommit() {
		leader.pendAll()
	}

	if atomic.CompareAndSwapUint32(&task.state, _TaskCompleted, _TaskIdle) {
		// state == completed indicates no more jobs need to do, relax for a while
		task.resetTimer()
	} else {
		// more jobs come, continue working
		leader.fireEvent(_MsgPending, task)
	}
}

// reused worker in thread pool
func (leader *leader) runSenderWorker() {
	go func() {
		for {
			select {
			case <-leader.stopCh:
				return
			case task := <-leader.readyTasks:
				select {
				case <-leader.stopCh: // make sure the stop signal has the highest priority
				default:
					task.send(task.args)
				}
			}
		}
	}()
}

func (leader *leader) pendAll() {
	for peerIndex, task := range leader.tasks {
		if peerIndex != leader.me {
			task.pend()
		}
	}
}

func (leader *leader) isOffline() bool {
	disconnectedNumber := 0

	for peerIndex, task := range leader.tasks {
		if peerIndex != leader.me && task.isDisconnected() {
			disconnectedNumber++
		}
	}

	return disconnectedNumber >= leader.majority
}

func (task *appendTask) isDisconnected() bool {
	return task.failureNumber >= _DisconnectedThreshold
}

func (task *appendTask) pend() {
	// we have such state diagram:
	//   idle -> pending <-> running -> completed
	//    /|\      /|\                      |
	//     |--------|-----------------------|
	// and such promise:
	//   transition from task_pending, task_running & task_completed to others is always issued by a single goroutine
	// then we leverage CAS when transit from task_idle
	// to satisfy such semantic:
	//   each task appears in pending_queue at most once

	task.heartbeatTimeout.Stop()

	// logically we have two queue: queue of pending tasks and queue of completed tasks
	// running tasks (it means the task is in progress of sending rpc) belong to no queue
	if oldState := atomic.SwapUint32(&task.state, _TaskPending);
		oldState == _TaskIdle || oldState == _TaskRetrying {
		task.leader.fireEvent(_MsgPending, task)
	}
}

func (task *appendTask) prepareArgs() (args *AppendArgs) {
	// nextIndex is initialized as last log's index + 1 (call it frontier for short; it is 0 when leader starts work with no log)
	//
	// if AppendEntries succeed, then nextIndex is set as frontier again.
	// if AppendEntries failed due to mismatch of previous log, then nextIndex decrease.
	//
	// Since no log precedes the very first one, AppendEntries never conflicts when nextIndex == 0
	// So the following invariance maintains:
	//    nextIndex >= 0

	rf := task.leader

	// if this AppendEntries will carry no logs, startOffset can be negative
	startOffset := rf.getLogOffsetByIndex(task.nextIndex)
	// endOffset is the last log's offset, -1 if there is no logs

	// try to find previous log
	prevIndex, prevTerm := -1, -1
	if startOffset > 0 { // if previous one exists
		prevLog := rf.logs[startOffset-1]
		prevIndex, prevTerm = prevLog.Index, prevLog.Term
	}

	var logs []Log = nil
	if startOffset >= 0 { // this call will carry logs
		logs = rf.logs[startOffset:len(rf.logs)] // invariance: len(logs) >= 1
	}

	args = &AppendArgs{
		Term: rf.currentTerm,

		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,

		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,

		Entries: logs,
	}
	return
}

func (task *appendTask) send(args *AppendArgs) {
	re := &AppendReply{}

	//logStart, logEnd := -1, -1
	//if entriesNum := len(args.Entries); entriesNum > 0 {
	//	logStart = args.Entries[0].Index
	//	logEnd = args.Entries[entriesNum-1].Index
	//}
	//task.leader.DPrintf("Send Append [%v] | Log: %v ~ %v", task.peerIndex, logStart, logEnd)

	if tryCall(task.peer, "Raft.AppendEntries", args, re) {
		task.lastNetworkFailed = false
		if re.Success {
			if entries := args.Entries; entries != nil {
				// update cursors
				task.matchIndex = entries[len(entries)-1].Index
				task.nextIndex = task.matchIndex + 1
			}
			// Try to CAS task's state Running to Completed.
			// If it succeed, we have alter state to Completed.
			// Or else, another thread has alter it to Pending.
			// In both situation, handleCompletion() will handle it properly.
			atomic.CompareAndSwapUint32(&task.state, _TaskRunning, _TaskCompleted)
			task.leader.fireEvent(_MsgCompletion, task)
		} else if re.Term > args.Term { // oops, we have been stale leader, fallback.
			task.leader.fireEvent(_MsgGreaterTerm, re.Term)
		} else { // conflict of previous log, then rollback `nextIndex`
			leader := task.leader
			// Here is fast rollback at leader side.
			// 1. Check if the proposed baseline also exists in our own logs
			if leader.getLog(re.BaselineIndex, re.BaselineTerm) != nil {
				// 1.1 if true, set `nextIndex` according to the proposed index
				task.nextIndex = re.BaselineIndex + 1
			} else // 1.2 if false, seek baseline according to proposed term
			if leaderBaselineLog := leader.seekBaseline(re.BaselineTerm); leaderBaselineLog != nil {
				// 1.2.1 found
				task.nextIndex = leaderBaselineLog.Index + 1
			} else { // 1.2.2 not found, set to 0 (this means leader has no earlier log)
				task.nextIndex = 0
			}

			atomic.StoreUint32(&task.state, _TaskRetrying)
			task.pend() // retry
		}
	} else { // failed due to network error
		task.lastNetworkFailed = true
		atomic.StoreUint32(&task.state, _TaskRetrying)
		task.pend() // retry
	}
}

func (task *appendTask) tryCommit() bool {
	leader := task.leader

	//if task.args != nil && task.args.Entries != nil {
	//	leader.DPrintf("Command [%v] | Replicated [%v]", task.matchIndex, task.peerIndex)
	//}

	if canCommit := leader.counter.count(task.matchIndex, task.peerIndex); canCommit >= 0 {
		//leader.DPrintf("Leader Commit [%v]", canCommit)
		leader.commitIndex = canCommit

		if leader.getLogByIndex(canCommit).Term == leader.currentTerm {
			leader.commit()
			return true
		}
	}
	return false
}

func (task *appendTask) resetTimer() {
	task.heartbeatTimeout.Reset(_HeartbeatTimeout)
}
