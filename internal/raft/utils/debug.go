package utils

import "log"

var Debug bool = false

// only used for RPC communication as of now
func Dprintf(text string, args ...interface{}) {
	if Debug {
		log.Printf(text, args...)
	}
}
