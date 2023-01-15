package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

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
	dClient    logTopic = "CLNT"
	dApply     logTopic = "APLY"
	dDrop      logTopic = "DROP"
	dError     logTopic = "ERRO"
	dInfo      logTopic = "INFO"
	dRegister  logTopic = "RGST"
	dPutAppend logTopic = "PTAP"
	dGet       logTopic = "GET1"
	dPersist   logTopic = "PERS"
	dSnap      logTopic = "SNAP"
	dTerm      logTopic = "TERM"
	dTest      logTopic = "TEST"
	dTimer     logTopic = "TIMR"
	dTrace     logTopic = "TRCE"
	dVote      logTopic = "VOTE"
	dWarn      logTopic = "WARN"
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

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	SessionExpired = "SessionExpired"
)

type Err string

type RegisterClientArgs struct {
	ClientID    int64
	SequenceNum int
}

type RegisterClientReply struct {
	Status Err
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID    int64
	SequenceNum int
}

type PutAppendReply struct {
	Success    bool
	LeaderHint int
	Status     Err
}

type GetArgs struct {
	Key string

	ClientID    int64
	SequenceNum int
}

type GetReply struct {
	Success    bool
	Value      string
	Status     Err
	LeaderHint int
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func CreateMemoryKV() MemoryKV {
	return MemoryKV{make(map[string]string)}
}

func (mkv MemoryKV) Get(key string) (string, Err) {
	value, ok := mkv.KV[key]
	if !ok {
		return value, ErrNoKey
	} else {
		return value, OK
	}
}

func (mkv MemoryKV) Put(key string, value string) Err {
	mkv.KV[key] = value
	return OK
}

func (mkv MemoryKV) Append(key string, value string) Err {
	mkv.KV[key] += value
	return OK
}
