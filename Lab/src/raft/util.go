package raft

//import "fmt"
import "log"

// Debugging
const DebugForLifeCycle = 0
const DebugForRequestVote = 0
const DebugForAppendEntry = 0
const DebugForInstallSnapshot = 0
const DebugErr = 0


func DLCPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugForLifeCycle > 0 {
		log.Printf(format, a...)
	}
	return
}

func DRVPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugForRequestVote > 0 {
		log.Printf(format, a...)
	}
	return
}

func DAEPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugForAppendEntry > 0 {
		log.Printf(format, a...)
	}
	return
}

func DISPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugForInstallSnapshot > 0 {
		log.Printf(format, a...)
	}
	return
}

func DErrPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugErr > 0 {
		log.Printf(format, a...)
	}
	return
}


