package utils

import (
	"log"
)

var Debug int32

// only used for RPC communication as of now
func Dprintf(text string, args ...interface{}) {
	if Debug == 1 {
		log.Printf(text, args...)
	}
}
