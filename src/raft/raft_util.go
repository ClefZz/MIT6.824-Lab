package raft

import (
	"log"
	"container/list"
	"bytes"
	"fmt"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func calcMajority(total int) int {
	return (total >> 1) + 1
}

func min(i1 int, i2 int) int {
	if i1 > i2 {
		return i2
	} else {
		return i1
	}
}

type set struct {
	m map[int]bool
}

func makeSet(value int) (s *set) {
	s = &set{make(map[int]bool)}
	s.add(value)
	return
}

func (s *set) add(i int) bool {
	if !s.m[i] {
		s.m[i] = true
		return true
	}
	return false
}

func (s *set) len() int {
	return len(s.m)
}

// count each log entries replicatedPeers. replica means at which peers a log entries has been replicated.
//
// due to the continuity of log's index, count a greater log index causes a cascade counting of lesser ones.
// when some log indices have gained a majority replicatedPeers, they are removed from the counter
// and the greatest one among them is returned.
type replicaCounter struct {
	*list.List
	majority    int
	minLogIndex int
}

// replica record of a single entry
type replica struct {
	logIndex        int
	replicatedPeers *set
}

func makeReplicaCounter(majority int) (*replicaCounter) {
	return &replicaCounter{
		List:        list.New(),
		majority:    majority,
		minLogIndex: -1,
	}
}

// print counter's status, for debug
func (counter *replicaCounter) show(tag string, rf *Raft) {
	var buf bytes.Buffer

	for e := counter.Front(); e != nil; e = e.Next() {
		buf.WriteString(fmt.Sprintf("{%v %v} ", e.Value.(*replica).logIndex, e.Value.(*replica).replicatedPeers.len()))
	}
	rf.DPrintf("[%v] majority counter: minLogIndex: %v, %v", tag, counter.minLogIndex, buf.String())
}

// count a new replica. refer to `replicaCounter`'s comment for more information
func (counter *replicaCounter) count(logIndex, replicaAt int) int {
	if logIndex <= counter.minLogIndex {
		return -1
	}

	var e *list.Element

	canCommit, neverCounted := -1, true
	// update each one's `replicatedPeers` until encountering a greater index
	// so counter's list always ascends
	for e = counter.Front(); e != nil; e = e.Next() {
		if entry := e.Value.(*replica); entry.logIndex <= logIndex {
			entry.replicatedPeers.add(replicaAt)
			if entry.replicatedPeers.len() >= counter.majority {
				canCommit = entry.logIndex
			}
			if entry.logIndex == logIndex {
				neverCounted = false
			}
		} else {
			break
		}
	}

	// handle trailing node
	if neverCounted {
		if e == nil {
			counter.PushBack(&replica{logIndex, makeSet(replicaAt)})
		} else {
			counter.InsertBefore(&replica{logIndex, makeSet(replicaAt)}, e)
		}
	}

	if canCommit != -1 {
		// some log entries has gained majority replicas, remove them
		counter.truncate(canCommit)
	}

	return canCommit
}

func (counter *replicaCounter) truncate(threshold int) {
	// remove records with index less than threshold
	counter.minLogIndex = threshold

	for e := counter.Front(); e != nil && e.Value.(*replica).logIndex <= threshold; {
		next := e.Next()
		counter.Remove(e)
		e = next
	}
}
