package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"

	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"math/rand"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

const (
	Follower  = 901
	Candidate = 902
	Leader    = 903
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int   // latest term server has seen (initialized to 0 on first boot)
	votedFor    int   // candidateID that received vote in current term (-1 if none)
	logs        []Log // log entries; each entry contains a command and a term number

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated
	tasks      TaskQueue

	// Extra fields

	// For election timeout:
	lastCommunicationTime  time.Time     // time deadline for receiving next communication
	currentElectionTimeout time.Duration // randomized time period

	muTime sync.Mutex

	// For state identification:
	currentState   int
	appearingState int

	// For state transition:
	condFollower  *sync.Cond
	condCandidate *sync.Cond
	condLeader    *sync.Cond

	muState sync.Mutex

	// For callbacks:
	muCallback sync.Mutex

	// For commit status:
	replicationCount  []int // for each log entry on this server, the number of replicas
	alignmentStatus   [][]bool
	replicationStatus [][]bool

	muCommmit sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.muState.Lock()
	term := rf.currentTerm
	isLeader := rf.appearingState == Leader
	rf.muState.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	// e.Encode(rf.nextIndex)
	// e.Encode(rf.matchIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	// var commitIndex int
	// var lastApplied int
	// var nextIndex []int
	// var matchIndex []int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		// d.Decode(&commitIndex) != nil ||
		// d.Decode(&lastApplied) != nil ||
		// d.Decode(&nextIndex) != nil ||
		// d.Decode(&matchIndex) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		// rf.commitIndex = commitIndex
		// rf.lastApplied = lastApplied
		// rf.nextIndex = nextIndex
		// rf.matchIndex = matchIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

const (
	Heartbeat       = 501
	Alignment       = 502
	Replication     = 503
	CommitBroadcast = 504
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []Log
	LeaderCommit int
	TaskType     int
	LogIndex     int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.muState.Lock()
	rf.muCommmit.Lock()

	// for leader to update itself
	reply.Term = rf.currentTerm
	// DPrintf("NEW TERM DISCOVERED: %d", reply.Term)

	// refuse the call if incoming term is smaller
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		return
	}

	rf.ResetElectionTimeout()
	// DPrintf("timeout reset")

	switch args.TaskType {
	// heartbeat
	case Heartbeat:
		// DPrintf("received heartbeat")
		if rf.currentState == Candidate {
			rf.condCandidate.Signal()
		} else if rf.currentState == Leader {
			rf.condLeader.Signal()
		}
		rf.currentState = Follower
		rf.appearingState = Follower

	// alignment task
	case Alignment:
		if len(rf.logs) < args.PrevLogIndex {
			reply.MatchIndex = len(rf.logs)
			reply.Success = false
		} else if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.MatchIndex = args.PrevLogIndex - 1
			if reply.MatchIndex > 0 {
				for ; reply.MatchIndex > 0 && rf.logs[reply.MatchIndex-1].Term != args.PrevLogTerm; reply.MatchIndex-- {

				}
			}
			reply.Success = false
		} else {
			reply.Success = true
		}

	// replication task
	case Replication:
		if len(rf.logs) < args.PrevLogIndex || // receiver's log is shorter than the index
			(args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			reply.Success = false
		} else {
			identical := false
			if len(rf.logs) > args.PrevLogIndex+len(args.LogEntries) {
				identical = true
				for i := range args.LogEntries {
					if rf.logs[i+args.PrevLogIndex] != args.LogEntries[i] {
						identical = false
						break
					}
				}
			}
			if !identical {
				// overwrite the log entries after the index
				rf.logs = append(rf.logs[:args.PrevLogIndex], args.LogEntries...)
				rf.persist()
			}
			// rf.replicationCount = make([]int, len(rf.logs))
			reply.Success = true
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// update commitIndex
		update := true
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < len(rf.logs) {
			if rf.logs[args.LeaderCommit-1].Term < args.Term {
				update = false
			} else {
				rf.commitIndex = args.LeaderCommit
				update = true
			}
		} else {
			if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term < args.Term {
				update = false
			} else {
				rf.commitIndex = len(rf.logs)
				update = true
			}
		}
		// send ApplyMsg for each newly committed log entry
		if update {
			for i := oldCommitIndex; i < rf.commitIndex; i++ {
				// DPrintf("%d: %d", i+1, rf.logs[i].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i + 1,
				}
				rf.lastApplied = i + 1
			}
			// DPrintf(dCommit, "[%d (%d)]: committed till index %d", rf.me, rf.currentTerm, rf.commitIndex)
		}
	}

	// update the receiver's term
	if args.Term > rf.currentTerm {
		// DPrintf("updated term in recieving RPC %d", args.TaskType)
		if rf.currentState == Candidate {
			rf.condCandidate.Signal()
		} else if rf.currentState == Leader {
			rf.condLeader.Signal()
		}
		rf.currentState = Follower
		rf.appearingState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.muCommmit.Unlock()
	rf.muState.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, ch chan bool, status []int,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	rf.muCommmit.Lock()
	nextIndexLocal := rf.nextIndex[server]
	totalIndexLocal := rf.nextIndex[server] + len(args.LogEntries)
	rf.muCommmit.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.muState.Lock()
	rf.muCommmit.Lock()

	// check if this reply is outdated
	if rf.currentTerm > args.Term {
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		return ok
	}

	// check if new term is discovered
	if reply.Term > rf.currentTerm {
		// DPrintf("NEW TERM DISCOVERED")
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.appearingState = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// step down if necessary
	if rf.currentState != Leader {
		rf.muState.Unlock()
		// DPrintf("current state: %d\n", rf.currentState)
		rf.muCommmit.Unlock()
		rf.condLeader.Signal()
		return ok
	}

	// check if the reply is outdated
	if args.PrevLogIndex < rf.nextIndex[server]-1 {
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		return ok
	}

	switch args.TaskType {
	case Alignment:
		if !ok {
			rf.muCommmit.Unlock()
			rf.muState.Unlock()
			// nextCheckingTime := time.Now().Add(10 * time.Millisecond)
			// delay := time.Until(nextCheckingTime)
			// time.Sleep(delay)
			// rf.tasks.push(Task{Alignment, server, args.LogIndex})
			// DPrintf("ALIGNMENT fail because of connection")
			return ok
		} else if !reply.Success {
			rf.nextIndex[server] = reply.MatchIndex + 1
			// rf.nextIndex[server] = nextIndexLocal - 1
			// rf.nextIndex[server]--
			// rf.tasks.push(Task{Alignment, server, args.LogIndex})
			// DPrintf("ALIGNMENT fail")
		} else {
			rf.alignmentStatus[args.LogIndex-1][server] = true
			// rf.tasks.push(Task{Replication, server, args.LogIndex})
			// DPrintf("ALIGNMENT success")
		}

	case Replication:
		if !ok {
			rf.muCommmit.Unlock()
			rf.muState.Unlock()
			// nextCheckingTime := time.Now().Add(10 * time.Millisecond)
			// delay := time.Until(nextCheckingTime)
			// time.Sleep(delay)
			rf.tasks.push(Task{Replication, server, args.LogIndex})
			// DPrintf("REPLICATION failed because of connection")
			return ok
		} else if !reply.Success {
			// rf.nextIndex[server]--
			rf.nextIndex[server] = nextIndexLocal - 1
			// rf.tasks.push(Task{Replication, server, args.LogIndex})
			// DPrintf("REPLICATION failed")
		} else {
			rf.nextIndex[server] = totalIndexLocal
			rf.matchIndex[server] = totalIndexLocal - 1
			rf.replicationStatus[args.LogIndex-1][server] = true
			// rf.nextIndex[server] = rf.nextIndex[server] + len(args.LogEntries)
			// rf.replicationCount[args.LogIndex-1]++
			// DPrintf("REPLICATION %t for peer %d", reply.Success, server)
		}
	}

	rf.muCommmit.Unlock()
	rf.muState.Unlock()

	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.muState.Lock()
	// DPrintf("vote reached!")

	// for candidate to update itself
	reply.Term = rf.currentTerm

	// refuse the request if incoming term is smaller
	if args.Term < rf.currentTerm {
		// DPrintf("vote blocked!")
		reply.VoteGranted = false
		rf.muState.Unlock()
		return
	}

	// update the receiver's term
	if args.Term > rf.currentTerm {
		if rf.currentState == Candidate {
			rf.condCandidate.Signal()
		} else if rf.currentState == Leader {
			rf.condLeader.Signal()
		}
		rf.currentState = Follower
		rf.appearingState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(len(rf.logs) == 0 ||
			rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex)) {

		// DPrintf(dVote, "[%d (%d)]: vote granted to peer %d", rf.me, rf.currentTerm, args.CandidateID)
		if rf.currentState == Candidate {
			rf.condCandidate.Signal()
		} else if rf.currentState == Leader {
			rf.condLeader.Signal()
		}
		rf.ResetElectionTimeout()
		rf.currentState = Follower
		rf.appearingState = Follower
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.persist()
	} else {
		if !(len(rf.logs) == 0 ||
			rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex)) {
			// DPrintf(dVote, "[%d (%d)]: vote denied to peer %d, not up-to-date", rf.me, rf.currentTerm, args.CandidateID)
		} else {
			// DPrintf(dVote, "[%d (%d)]: vote denied to peer %d, already voted", rf.me, rf.currentTerm, args.CandidateID)
		}
		reply.VoteGranted = false
	}

	rf.muState.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, voteCounter chan bool,
	args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.muState.Lock()

	// check if this reply is outdated
	if rf.currentTerm > args.Term {
		// DPrintf("OUTDATED!")
		// rf.muCommmit.Unlock()
		rf.muState.Unlock()
		return ok
	}

	// discover new term
	if reply.Term > rf.currentTerm {
		// DPrintf("update to new term when requesting vote from others")
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.appearingState = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// quit Candidate if necessary
	if rf.currentState != Candidate {
		// DPrintf("quit Candidate in handling")
		rf.muState.Unlock()
		rf.condCandidate.Signal()
		return ok
	}

	// pass voting result to vote counter
	voteCounter <- reply.VoteGranted

	rf.muState.Unlock()
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.muState.Lock()
	rf.muCommmit.Lock()

	if !rf.killed() {
		index = len(rf.logs) + 1
		term = rf.currentTerm
		isLeader = rf.appearingState == Leader
	}

	if isLeader {
		// DPrintf(dLeader, "[%d (%d)]: Took command: %d", rf.me, rf.currentTerm, command)
		log := Log{
			Command: command,
			Term:    rf.currentTerm,
			Index:   len(rf.logs) + 1,
		}
		rf.logs = append(rf.logs, log)
		rf.alignmentStatus = append(rf.alignmentStatus, make([]bool, len(rf.peers)))
		rf.replicationStatus = append(rf.replicationStatus, make([]bool, len(rf.peers)))
		rf.persist()
		// for i := len(rf.replicationCount); i < log.Index; i++ {
		// 	rf.replicationCount = append(rf.replicationCount, 1)
		// }
		// for i := range rf.peers {
		// 	if i != rf.me {
		// 		rf.tasks.push(Task{Alignment, i, log.Index})
		// 	}
		// }

		go rf.replicateLog(log.Index)
	}

	rf.muCommmit.Unlock()
	rf.muState.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.condFollower = sync.NewCond(&rf.muState)
	rf.condCandidate = sync.NewCond(&rf.muState)
	rf.condLeader = sync.NewCond(&rf.muState)
	rf.currentState = Follower
	rf.appearingState = Follower
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Run()

	return rf
}

func (rf *Raft) Run() {
	for !rf.killed() {
		rf.muState.Lock()

		switch rf.currentState {
		case Follower:
			rf.Follower()

		case Candidate:
			rf.Candidate()

		case Leader:
			rf.Leader()
		}

		rf.muState.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(quitTicker chan bool) {
	rf.ResetElectionTimeout()
	for rf.killed() == false {
		select {
		case <-quitTicker:
			return

		default:
			// sleep until next checkpoint
			rf.muTime.Lock()
			lastCommunicationTime := rf.lastCommunicationTime
			nextCheckingTime := rf.lastCommunicationTime.Add(time.Duration(rf.currentElectionTimeout))
			rf.muTime.Unlock()
			delay := time.Until(nextCheckingTime)
			// DPrintf("delay: %d", delay.Milliseconds())
			time.Sleep(delay)

			// begin leader selection if no new communication has been made
			rf.muTime.Lock()
			receivedNewCommunication := rf.lastCommunicationTime.After(lastCommunicationTime)
			// DPrintf("Remain: %d", rf.lastCommunicationTime.Sub(lastCommunicationTime).Milliseconds())
			rf.muTime.Unlock()
			if !receivedNewCommunication {
				rf.muState.Lock()
				if rf.currentState == Leader {
					rf.muState.Unlock()
					continue
				}
				originalState := rf.currentState
				rf.currentState = Candidate
				rf.appearingState = Candidate
				rf.muState.Unlock()

				// DPrintf(dTimer, "[%d (%d)]: election timeout", rf.me, rf.currentTerm)
				if originalState == Candidate {
					rf.condCandidate.Signal()
				} else if originalState == Follower {
					rf.condFollower.Signal()
				}

				rf.ResetElectionTimeout()
			}
		}
	}
}

func (rf *Raft) ResetElectionTimeout() {
	// election timeout range: [500, 1000]
	rf.muTime.Lock()
	rf.lastCommunicationTime = time.Now()
	electionTimeout := rand.Int63n(500) + 500 // in milliseconds
	rf.currentElectionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.muTime.Unlock()
}

func (rf *Raft) Follower() {
	// DPrintf(dDrop, "[%d (%d)]: become follower", rf.me, rf.currentTerm)

	quitTicker := make(chan bool, 1)
	go rf.ticker(quitTicker)

	rf.condFollower.Wait()

	quitTicker <- true
}

func (rf *Raft) Candidate() {
	// DPrintf(dTimer, "[%d (%d)]: become candidate", rf.me, rf.currentTerm)

	quitTicker := make(chan bool, 1)
	go rf.ticker(quitTicker)

	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
	// DPrintf("term %d -> %d", rf.currentTerm-1, rf.currentTerm)

	voteCounter := make(chan bool, len(rf.peers))

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.logs),
		// LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	if len(rf.logs) != 0 {
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	replies := make([]RequestVoteReply, len(rf.peers))

	// request votes from peers
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, voteCounter, &args, &replies[i])
		}
	}

	// count votes
	quitVoteCounter := make(chan bool, 1)
	go rf.voteCounter(voteCounter, quitVoteCounter)

	// go to sleep
	rf.condCandidate.Wait()

	// quit ticker and voteCounter
	quitTicker <- true
	quitVoteCounter <- true
	// DPrintf("quit ticker")
}

func (rf *Raft) voteCounter(voteCounter chan bool, quitVoteCounter chan bool) {
	voteCount := 1
	for !rf.killed() {
		select {
		case <-quitVoteCounter:
			return

		default:
			voteGranted := <-voteCounter
			if voteGranted {
				voteCount++
				// DPrintf(dVote, "[%d (%d)]: vote++ -> %d", rf.me, rf.currentTerm, voteCount)
			}
			if voteCount >= (len(rf.peers)+1)/2 {
				// DPrintf("vote enough")
				rf.muState.Lock()
				rf.currentState = Leader
				rf.muState.Unlock()
				rf.condCandidate.Signal()
				return
			}
		}
	}
}

func (rf *Raft) sendHeartbeat(quitHeartbeat chan bool) {
	for rf.killed() == false {
		select {
		case <-quitHeartbeat:
			return

		default:
			// DPrintf("send heartbeat")
			rf.muState.Lock()
			rf.muCommmit.Lock()

			lastHeartbeatTime := time.Now()

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				LeaderCommit: rf.commitIndex,
				TaskType:     Heartbeat,
			}
			reply := make([]AppendEntriesReply, len(rf.peers))

			// for i := range rf.peers {
			// 	if i != rf.me {
			// rf.tasks.push(Task{Heartbeat, -1, -1})
			// 	}
			// }
			for i := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntries(i, nil, nil, &args, &reply[i])
				}
			}

			rf.muCommmit.Unlock()
			rf.muState.Unlock()

			nextHeartbeatTime := lastHeartbeatTime.Add(150 * time.Millisecond)
			delay := time.Until(nextHeartbeatTime)
			time.Sleep(delay)
		}
	}
}

func (rf *Raft) Leader() {
	// DPrintf(dLeader, "[%d (%d)]: become leader", rf.me, rf.currentTerm)

	rf.muCommmit.Lock()

	quitHeartbeat := make(chan bool, 1)
	go rf.sendHeartbeat(quitHeartbeat)

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) + 1
	}
	rf.alignmentStatus = make([][]bool, len(rf.logs))
	rf.replicationStatus = make([][]bool, len(rf.logs))
	for i := range rf.alignmentStatus {
		rf.alignmentStatus[i] = make([]bool, len(rf.peers))
		rf.replicationStatus[i] = make([]bool, len(rf.peers))
	}

	rf.appearingState = Leader

	rf.muCommmit.Unlock()

	rf.tasks.InitializeTaskQueue()

	// quitCollector := make(chan bool, 1)
	// go rf.taskCollector(quitCollector)

	rf.condLeader.Wait()

	quitHeartbeat <- true
	// quitCollector <- true

	// DPrintf(dDrop, "[%d (%d)]: quit leader", rf.me, rf.currentTerm)
}

func (rf *Raft) taskCollector(quitCollector chan bool) {
	for !rf.killed() {
		select {
		case <-quitCollector:
			// DPrintf("quit collector!!!!!!!!")
			return

		default:
			// rf.muState.Lock()

			// collect task
			task := rf.tasks.pop() // might block
			rf.ProcessTask(task)

			// rf.muState.Unlock()
		}
	}
}

func (rf *Raft) ProcessTask(task Task) {
	switch task.taskType {
	case Heartbeat:
		// DPrintf("send heartbeat")
		rf.muState.Lock()
		rf.muCommmit.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			LeaderCommit: rf.commitIndex,
			TaskType:     Heartbeat,
		}
		reply := make([]AppendEntriesReply, len(rf.peers))
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		// DPrintf("send Heartbeat")
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppendEntries(i, nil, nil, &args, &reply[i])
			}
		}

	case Alignment:
		rf.muCommmit.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[task.receiver] - 1,
			LeaderCommit: rf.commitIndex,
			TaskType:     Alignment,
			LogIndex:     task.logIndex,
		}
		if rf.nextIndex[task.receiver] == 1 {
			args.PrevLogTerm = -1
		} else {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
		}
		reply := AppendEntriesReply{}
		// DPrintf(dLog, "[%d (%d)]: sending alignment", rf.me, rf.currentTerm)
		go rf.sendAppendEntries(task.receiver, nil, nil, &args, &reply)
		rf.muCommmit.Unlock()

	case Replication:
		rf.muCommmit.Lock()
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[task.receiver] - 1,
			LeaderCommit: rf.commitIndex,
			// LogEntries:   rf.logs[rf.nextIndex[task.receiver]-1 : task.logIndex],
			TaskType: Replication,
			LogIndex: task.logIndex,
		}
		if rf.nextIndex[task.receiver] == 1 {
			args.PrevLogTerm = -1
		} else {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
		}
		if rf.nextIndex[task.receiver]-1 <= task.logIndex {
			args.LogEntries = rf.logs[rf.nextIndex[task.receiver]-1 : task.logIndex]
		}
		reply := AppendEntriesReply{}
		// DPrintf("send REPLICATION %v", args)
		go rf.sendAppendEntries(task.receiver, nil, nil, &args, &reply)
		rf.muCommmit.Unlock()
	}
}

func (rf *Raft) replicateLog(logIndex int) {
	// DPrintf(dLeader, "[%d (%d)]: begin to replicate log at index %d", rf.me, rf.currentTerm, logIndex)

	committed := false
	for !rf.killed() {
		rf.muState.Lock()
		rf.muCommmit.Lock()

		if rf.currentState != Leader {
			rf.muCommmit.Unlock()
			rf.muState.Unlock()
			return
		}

		replicationCount := 0

		for i := range rf.alignmentStatus[logIndex-1] {
			if i != rf.me && !rf.alignmentStatus[logIndex-1][i] {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
					TaskType:     Alignment,
					LogIndex:     logIndex,
				}
				if rf.nextIndex[i] <= 1 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
				}
				reply := AppendEntriesReply{}
				// DPrintf(dLog, "[%d (%d)]: sending alignment to [%d]", rf.me, rf.currentTerm, i)
				go rf.sendAppendEntries(i, nil, nil, &args, &reply)
			} else if i != rf.me && !rf.replicationStatus[logIndex-1][i] {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
					TaskType:     Replication,
					LogIndex:     logIndex,
				}
				if rf.nextIndex[i] <= 1 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
				}
				if rf.nextIndex[i]-1 <= logIndex {
					args.LogEntries = rf.logs[rf.nextIndex[i]-1 : logIndex]
				}
				reply := AppendEntriesReply{}
				// DPrintf(dLog2, "[%d (%d)]: sending replication to [%d]", rf.me, rf.currentTerm, i)
				go rf.sendAppendEntries(i, nil, nil, &args, &reply)
			} else {
				replicationCount++
			}
		}

		if !committed && replicationCount >= (len(rf.peers)+1)/2 {
			// DPrintf("Quorum")
			committed = true

			oldCommitIndex := rf.commitIndex
			if rf.commitIndex < logIndex {
				rf.commitIndex = logIndex
			}

			// send ApplyMsg for each newly committed log entry
			for i := oldCommitIndex; i < rf.commitIndex; i++ {
				// DPrintf("committing %d: %d", i+1, rf.logs[i].Command)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Command,
					CommandIndex: i + 1,
				}
				rf.lastApplied = i + 1
			}

			// DPrintf(dCommit, "[%d (%d)]: committed till index %d", rf.me, rf.currentTerm, rf.commitIndex)
		}

		rf.muCommmit.Unlock()
		rf.muState.Unlock()

		if replicationCount == len(rf.peers) {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
