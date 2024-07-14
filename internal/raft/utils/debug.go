package utils

import (
	"log"
	"sync/atomic"
)

var Debug atomic.Int32

func init() {
	Debug.Store(1)
}

// only used for RPC communication as of now
func Dprintf(text string, args ...interface{}) {
	if int(Debug.Load()) == 1 {
		log.Printf(text, args...)
	}
}
