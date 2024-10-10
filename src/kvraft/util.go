package kvraft

import (
	"fmt"
	"log"
	"time"
)

// Debugging

type logTopic string

const (
	dClient logTopic = "CLNT"
	dServer logTopic = "Serv"
	dDrop   logTopic = "DROP"
	dError  logTopic = "ERRO"
	dInfo   logTopic = "INFO"
	dTest   logTopic = "TEST"
	dTimer  logTopic = "TIMR"
	dTrace  logTopic = "TRCE"
	dWarn   logTopic = "WARN"
	ddbexe  logTopic = "DBexecute"
	dSnap   logTopic = "SNAP"
)

var debugStart time.Time

const debugVerbosity = 1

func init() {

	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
