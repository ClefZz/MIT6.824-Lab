package raft

import (
	"log"
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

func SetMax(i1 *int, i2 int) {
	if *i1 < i2 {
		*i1 = i2
	}
}
