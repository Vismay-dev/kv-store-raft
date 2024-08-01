package utils

import (
	"log"
	"runtime"
	"sync/atomic"
	"time"
)

var Debug atomic.Int32

func init() {
	Debug.Store(0)
}

// only used for RPC communication as of now
func Dprintf(text string, args ...interface{}) {
	if int(Debug.Load()) == 1 {
		log.Printf(text, args...)
	}
}

// Function to periodically log stack traces
func LogStackTraces(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			buf := make([]byte, 1<<20) // 1 MB buffer
			stackLen := runtime.Stack(buf, true)
			log.Printf("=== Goroutine Stack Traces ===\n%s\n", buf[:stackLen])
		}
	}
}
