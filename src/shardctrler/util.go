package shardctrler

import (
	"fmt"
	"log"
	"time"

	"6.5840/raft"
)

// Debugging

type logTopic string

const (
	dClient logTopic = "CLNT"
	dServer logTopic = "Serv"
	dLeader logTopic = "LEAD"
	dError  logTopic = "ERRO"
	dInfo   logTopic = "INFO"
	dTest   logTopic = "TEST"
	dTimer  logTopic = "TIMR"
	dTrace  logTopic = "TRCE"
	dWarn   logTopic = "WARN"
	dSnap   logTopic = "SNAP"
	ddbexe  logTopic = "DBexecute"
	dJoin   logTopic = "JOIN"
	dLeave  logTopic = "LEAVE"
	dMove   logTopic = "MOVE"
	dQuery  logTopic = "QUERY"
)

var debugStart time.Time

const debugVerbosity = raft.DebugVerbosity

func init() {

	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d shardctrler-%v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
