package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// Debugging
const Debug = false

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

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

func init() {
	debugVerbosity = getVerbosity()
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

// func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		pc := make([]uintptr, 15)
// 		n := runtime.Callers(2, pc)
// 		frames := runtime.CallersFrames(pc[:n])
// 		frame, _ := frames.Next()
// 		fmt.Printf("Line %d, [%d (%d)]: ", frame.Line, rf.me, rf.currentTerm)
// 		fmt.Printf(format+"\n", a...)
// 	}
// 	return
// }

type Task struct {
	taskType int
	receiver int
	logIndex int
}

type TaskQueue struct {
	mutex sync.Mutex
	cond  *sync.Cond

	tasks []Task
}

func (tq *TaskQueue) InitializeTaskQueue() {
	tq.mutex.Lock()
	tq.tasks = make([]Task, 0)
	tq.cond = sync.NewCond(&tq.mutex)
	tq.mutex.Unlock()
}

func (tq *TaskQueue) push(task Task) {
	tq.mutex.Lock()
	tq.tasks = append(tq.tasks, task)
	tq.cond.Signal()
	tq.mutex.Unlock()
}

func (tq *TaskQueue) pop() Task {
	tq.mutex.Lock()
	for len(tq.tasks) == 0 {
		tq.cond.Wait()
	}
	task := tq.tasks[0]
	tq.tasks = tq.tasks[1:]
	tq.mutex.Unlock()
	return task
}
