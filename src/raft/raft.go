package raft

import (
	"bytes"
	"labgob"
	"labrpc"
)

// import "bytes"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
	Quit      = 3
	Total     = 4
)

type Query struct {
	ch chan int
}

type Raft struct {
	applyCh chan ApplyMsg

	stateChan [Total]chan bool

	appendCallQueue  chan *ACall
	requestCallQueue chan *RCall

	commandQueue chan *commandTask
	query        chan *Query

	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	logBase int

	// persistent props
	currentTerm int
	votedFor    int
	logs        []Log

	// volatile props
	commitIndex int
	lastAppied  int

	// volatile leader props
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	q := &Query{make(chan int, 2)}
	rf.query <- q
	return int(<-q.ch), (<-q.ch == Leader)
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logs []Log

	if d.Decode(&currentTerm) == nil &&
		d.Decode(&voteFor) == nil &&
		d.Decode(&logs) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
	}
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// rf.DPrintf("start query")
	select {
	case <-rf.stateChan[Quit]:
		return -1, -1, false
	default:
	}
	term, isLeader = rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	newTask := &commandTask{
		command: command,
		index:   make(chan int),
	}
	rf.commandQueue <- newTask
	index = <-newTask.index + 1

	// rf.DPrintf("start %v %v %v %v", index, term, isLeader, rf.logs)

	return
}

func (rf *Raft) Kill() {
	close(rf.stateChan[Quit])
	//	rf.DPrintf("killed, term: %v, votedFor: %v, logs: %v", rf.currentTerm, rf.votedFor, rf.logs)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) (rf *Raft) {

	rf = &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		logBase: 0,

		currentTerm: 0,
		votedFor:    -1,
		logs:        nil,

		commitIndex: -1,
		lastAppied:  -1,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		requestCallQueue: make(chan *RCall, 10),
		appendCallQueue:  make(chan *ACall, 10),

		commandQueue: make(chan *commandTask, 100),

		applyCh: applyCh,

		query: make(chan *Query),
	}

	for i := 0; i < Total; i++ {
		rf.stateChan[i] = make(chan bool)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runFollower()
	go rf.runCandidate()
	go rf.runLeader()

	//	rf.DPrintf("started, term: %v, votedFor: %v, logs: %v", rf.currentTerm, rf.votedFor, rf.logs)

	rf.stateChan[Follower] <- true

	return
}

func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	call := &ACall{
		args:  args,
		reply: make(chan *AppendReply, 1),
	}
	rf.appendCallQueue <- call
	*reply = *(<-call.reply)
}

func (rf *Raft) RequestVote(args *RequestArgs, reply *RequestReply) {
	// rf.DPrintf("receive request: %+v", args)
	call := &RCall{
		args:  args,
		reply: make(chan *RequestReply, 1),
	}
	rf.requestCallQueue <- call
	*reply = *(<-call.reply)
	// rf.DPrintf("reply request: %+v", args)
}
