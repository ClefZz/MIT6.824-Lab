package raft

import (
	"labrpc"
	"bytes"
	"labgob"
	"unsafe"
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

	FakeIndex int
}

type Raft struct {
	applyCh chan ApplyMsg

	peers     []*labrpc.ClientEnd
	majority  int // cached majority number
	persister *Persister
	me        int

	baseLogIndex int

	// persistent props
	currentTerm int
	votedFor    int
	logs        []Log
	fakeIndex   int // fake index to report to tester. see commit in raft_leader.go for details.

	// volatile props
	commitIndex int
	lastApplied int

	currentCharacter unsafe.Pointer
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) (rf *Raft) {

	rf = &Raft{
		peers:     peers,
		majority:  calcMajority(len(peers)),
		persister: persister,
		me:        me,

		baseLogIndex: 0,

		fakeIndex:   0,
		currentTerm: 0,
		votedFor:    -1,

		commitIndex: -1,
		lastApplied: -1,

		applyCh: applyCh,
	}
	rf.setCharacter(rf.makeFollower())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.getCharacter().loop()

	return
}

func (rf *Raft) Kill() {
	rf.DPrintf("- stop -")
	var mark character = nil
	for character := rf.getCharacter(); character != mark; character = rf.getCharacter() {
		character.stop()
		mark = character
	}
}

func (rf *Raft) GetState() (int, bool) {
	for character := rf.getCharacter(); ; character = rf.getCharacter() {
		if term := character.issueQuery(); term != -1 {
			_, isLeader := character.(*leader)
			return term, isLeader
		}
	}
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	for character := rf.getCharacter(); ; character = rf.getCharacter() {
		if term = character.issueQuery(); term != -1 {
			var l *leader
			if l, isLeader = character.(*leader); isLeader {
				if index = l.issueCommand(command); index == -1 {
					isLeader = false
				}
			}
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	for {
		if re := rf.getCharacter().issueAppend(args); re != nil {
			*reply = *re
			return
		}
	}
}

func (rf *Raft) RequestVote(args *RequestArgs, reply *RequestReply) {
	for {
		if re := rf.getCharacter().issueRequest(args); re != nil {
			*reply = *re
			return
		}
	}
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.fakeIndex)
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

	var fakeIndex, currentTerm, votedFor int
	var logs []Log
	if d.Decode(&fakeIndex) == nil &&
		d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&logs) == nil {
		rf.fakeIndex = fakeIndex
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}
