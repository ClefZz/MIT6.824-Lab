package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type Reg struct {
	workers chan string

	ready  chan int
	failed chan int
	done   chan int

	tasks struct {
		sync.Mutex
		m map[int]int
	}

	waitGroup sync.WaitGroup
}

const (
	READY = iota
	RUNNING
	FAILED
	DONE
)

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	reg := Reg{
		workers: make(chan string),
		ready:   make(chan int, 10),
		failed:  make(chan int, 10),
		done:    make(chan int, 10),
		tasks: struct {
			sync.Mutex
			m map[int]int
		}{m: make(map[int]int)},
	}
	reg.waitGroup.Add(ntasks)

	ends := []chan bool{make(chan bool), make(chan bool), make(chan bool)}

	// worker emitter
	go func(end chan bool) {
		for {
			select {
			case newWorker := <-registerChan:
				reg.workers <- newWorker
			case <-end:
				break
			}
		}
	}(ends[0])

	// initial task emitter, it exits automatically
	go func() {
		for i := 0; i < ntasks; i++ {
			reg.tasks.Lock()
			reg.tasks.m[i] = READY
			reg.tasks.Unlock()
			reg.ready <- i
		}
	}()

	// failed task recycler, exit on signal
	go func(end chan bool) {
		for {
			select {
			case failedTask := <-reg.failed:
				reg.tasks.Lock()
				if v, ok := reg.tasks.m[failedTask]; ok && v == FAILED {
					reg.tasks.m[failedTask] = READY
					go func(t int) { reg.ready <- t }(failedTask)
					reg.tasks.Unlock()
				} else {
					reg.tasks.Unlock()
					continue
				}
			case <-end:
				break
			}
		}
	}(ends[1])

	go func(end chan bool) {
		for {
			select {
			case readyTask := <-reg.ready:
				reg.tasks.Lock()
				state, ok := reg.tasks.m[readyTask]
				if ok && state == READY {
					reg.tasks.m[readyTask] = RUNNING
					reg.tasks.Unlock()
				} else {
					reg.tasks.Unlock()
					continue
				}
				if phase == mapPhase {
					go doCall(<-reg.workers, jobName, phase, mapFiles[readyTask], readyTask, n_other, &reg)
				} else {
					go doCall(<-reg.workers, jobName, phase, "", readyTask, n_other, &reg)
				}
			case <-end:
				break
			}
		}
	}(ends[2])

	reg.waitGroup.Wait()
	for _, end := range ends {
		go func(c chan bool) { c <- true }(end)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

func doCall(srv string, jobName string, jobPhase jobPhase,
	file string, taskNum int, n_other int, reg *Reg) {

	if call(srv, "Worker.DoTask", DoTaskArgs{jobName, file, jobPhase, taskNum, n_other}, nil) {
		select {
		case reg.workers <- srv:
		case <-time.After(time.Second):
		}

		reg.tasks.Lock()
		if state, ok := reg.tasks.m[taskNum]; ok && state == RUNNING {
			reg.tasks.m[taskNum] = DONE
			reg.waitGroup.Done()
		} 
		reg.tasks.Unlock()
	} else {
		retry := false
		reg.tasks.Lock()
		if state, ok := reg.tasks.m[taskNum]; ok && state == RUNNING {
			retry = true
			reg.tasks.m[taskNum] = FAILED
		} 
		reg.tasks.Unlock()

		if retry {
			reg.failed <- taskNum
		}
	}
}
