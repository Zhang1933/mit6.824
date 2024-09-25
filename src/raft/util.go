package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
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

func GetElectRandomTimeout() int {
	// 选举超时间隔 TODO:测试时间间隔
	return rand.Intn(50) + ElectTimeout
}

func (rf *Raft) FindTermFirstIdx(Xterm int, right int) int {
	// 从right开始,找到日志中第一个Xterm对应的idx,使用前需锁
	i := right
	for i >= 0 && rf.log[i].Term >= Xterm {
		i--
	}
	return i + 1
}
