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
	currentTerm       int   // latest term server has seen (initialized to 0 on first boot)
	votedFor          int   // candidateID that received vote in current term (-1 if none)
	logs              []Log // log entries; each entry contains a command and a term number
	lastIncludedIndex int
	lastIncludedTerm  int

	snapshot []byte

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

	// For commit status:
	alignmentStatus   [][]bool
	replicationStatus [][]bool
	applyMsgQueue     TaskQueue

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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}

	rf.snapshot = snapshot
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
	rf.muCommmit.Lock()
	DPrintf(dDrop, "[%d (%d)]: inside snapshot", rf.me, rf.currentTerm)
	rf.snapshot = snapshot
	rf.logs = rf.logs[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.persist()
	DPrintf(dDrop, "[%d (%d)]: lastincludedindex->%d", rf.me, rf.currentTerm, rf.lastIncludedIndex)
	DPrintf(dDrop, "[%d (%d)]: len(logs)->%d", rf.me, rf.currentTerm, len(rf.logs))
	rf.muCommmit.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	LogIndex          int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.muState.Lock()
	rf.muCommmit.Lock()

	// for leader to update itself
	reply.Term = rf.currentTerm
	// DPrintf("NEW TERM DISCOVERED: %d", reply.Term)

	// refuse the call if incoming term is smaller
	if args.Term < rf.currentTerm {
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		return
	}

	// if rf.lastIncludedIndex == args.LastIncludedIndex && rf.lastIncludedTerm == args.LastIncludedTerm {
	// 	rf.muCommmit.Unlock()
	// 	rf.muState.Unlock()
	// 	return
	// }

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	DPrintf(dLog2, "[%d (%d)]: installing snapshot 1", rf.me, rf.currentTerm)

	if len(rf.logs) > 0 {
		for i := len(rf.logs) - 1; i >= 0; i-- {
			if rf.logs[i].Term == rf.lastIncludedTerm && rf.logs[i].Index == rf.lastIncludedIndex {
				rf.logs = rf.logs[i+1:]
				rf.persist()
				DPrintf(dLog2, "[%d (%d)]: installing snapshot 2", rf.me, rf.currentTerm)
				rf.muCommmit.Unlock()
				rf.muState.Unlock()
				return
			}
		}
	}

	rf.logs = make([]Log, 0)
	rf.persist()

	rf.applyMsgQueue.push(ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	})
	DPrintf(dLog2, "[%d (%d)]: installing snapshot 3", rf.me, rf.currentTerm)

	rf.muCommmit.Unlock()
	rf.muState.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

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
		// DPrintf("current state: %d\n", rf.currentState)
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
		rf.condLeader.Signal()
		return ok
	}
	DPrintf(dLog2, "[%d (%d)]: installing snapshot, replyterm = %d, curstate = %d", rf.me, rf.currentTerm, reply.Term, rf.currentState)

	// rf.alignmentStatus[args.LogIndex-1][server] = true
	rf.nextIndex[server] = rf.lastIncludedIndex + 1

	rf.muCommmit.Unlock()
	rf.muState.Unlock()
	return ok
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
	MatchIndex int
	Success    bool
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
		if len(rf.logs)+rf.lastIncludedIndex < args.PrevLogIndex {
			reply.MatchIndex = len(rf.logs) + rf.lastIncludedIndex
			reply.Success = false
			DPrintf(dLog2, "[%d (%d)]: len %d | lastincludedindex %d | prevlogindex %d", rf.me, rf.currentTerm, len(rf.logs), rf.lastIncludedIndex, args.PrevLogIndex)
		} else if args.PrevLogIndex-rf.lastIncludedIndex > 0 && rf.logs[args.PrevLogIndex-1-rf.lastIncludedIndex].Term != args.PrevLogTerm {
			reply.MatchIndex = args.PrevLogIndex - 1
			if reply.MatchIndex > rf.lastIncludedIndex {
				for ; reply.MatchIndex > rf.lastIncludedIndex &&
					rf.logs[reply.MatchIndex-1-rf.lastIncludedIndex].Term != args.PrevLogTerm; reply.MatchIndex-- {

				}
			}
			reply.Success = false

			// DPrintf(dLog2, "[%d (%d)]: 2", rf.me, rf.currentTerm)
		} else {
			reply.MatchIndex = args.PrevLogIndex
			reply.Success = true
			// no snapshot needed, update commit index if needed
			if reply.MatchIndex > rf.lastIncludedIndex {
				if rf.commitIndex < rf.lastIncludedIndex {
					rf.commitIndex = rf.lastIncludedIndex
					DPrintf(dInfo, "[%d (%d)]: commit index updated from start", rf.me, rf.currentTerm)
				}
			}
			DPrintf(dLog2, "[%d (%d)]: true", rf.me, rf.currentTerm)
		}

	// replication task
	case Replication:
		if rf.lastIncludedIndex > args.PrevLogIndex {
			reply.Success = true
			rf.muCommmit.Unlock()
			rf.muState.Unlock()
			return
		}
		if len(rf.logs)+rf.lastIncludedIndex < args.PrevLogIndex || // receiver's log is shorter than the index
			(args.PrevLogIndex-rf.lastIncludedIndex > 0 && rf.logs[args.PrevLogIndex-1-rf.lastIncludedIndex].Term != args.PrevLogTerm) {
			reply.Success = false
			DPrintf(dError, "SHOULDNT REACH HERE!!")
			// DPrintf(dLog2, "[%d (%d)]: prevlogindex %d, lastincludedindex %d", rf.me, rf.currentTerm, args.PrevLogIndex, rf.lastIncludedIndex)
			// DPrintf(dLog2, "[%d (%d)]: this term %d, incoming term %d", rf.me, rf.currentTerm, rf.logs[args.PrevLogIndex-1-rf.lastIncludedIndex].Term, args.PrevLogTerm)
		} else {
			DPrintf(dInfo, "[%d (%d)]: current status: log last index %d", rf.me, rf.currentTerm, len(rf.logs)+rf.lastIncludedIndex)
			identical := false
			if len(rf.logs)+rf.lastIncludedIndex > args.PrevLogIndex+len(args.LogEntries) {
				identical = true
				DPrintf(dLog2, "[%d (%d)]: prevlogindex %d, lastincludedindex %d", rf.me, rf.currentTerm, args.PrevLogIndex, rf.lastIncludedIndex)
				for i := range args.LogEntries {
					if i+args.PrevLogIndex < rf.lastIncludedIndex {
						continue
					}
					if rf.logs[i+args.PrevLogIndex-rf.lastIncludedIndex] != args.LogEntries[i] {
						identical = false
						break
					}
				}
			}
			if !identical {
				// overwrite the log entries after the index
				rf.logs = append(rf.logs[:args.PrevLogIndex-rf.lastIncludedIndex], args.LogEntries...)
				rf.persist()
				DPrintf(dLog2, "[%d (%d)]: appended from %d to %d", rf.me, rf.currentTerm, args.PrevLogIndex, args.LogIndex)
			}
			// rf.replicationCount = make([]int, len(rf.logs))
			reply.Success = true
		}
	}

	if args.LeaderCommit > rf.commitIndex && rf.commitIndex >= rf.lastIncludedIndex {
		// update commitIndex
		update := true
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit < len(rf.logs)+rf.lastIncludedIndex {
			if rf.logs[args.LeaderCommit-1-rf.lastIncludedIndex].Term < args.Term {
				update = false
			} else {
				rf.commitIndex = args.LeaderCommit
				update = true
			}
		} else {
			if len(rf.logs) > 0 && rf.logs[len(rf.logs)-1].Term < args.Term {
				update = false
			} else {
				rf.commitIndex = len(rf.logs) + rf.lastIncludedIndex
				update = true
			}
		}
		// send ApplyMsg for each newly committed log entry
		if update {
			for i := oldCommitIndex; i < rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-rf.lastIncludedIndex].Command,
					CommandIndex: i + 1,
				}
				rf.applyMsgQueue.push(applyMsg)
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

	// rf.muCommmit.Lock()
	// // nextIndexLocal := rf.nextIndex[server]
	// totalIndexLocal := rf.nextIndex[server] + len(args.LogEntries)
	// DPrintf(dInfo, "localtotal = %d + %d = %d", rf.nextIndex[server], len(args.LogEntries), totalIndexLocal)
	// DPrintf(dInfo, "logindex BUT = %d", args.LogIndex)
	// rf.muCommmit.Unlock()

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
		// DPrintf("current state: %d\n", rf.currentState)
		rf.muCommmit.Unlock()
		rf.muState.Unlock()
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
			if rf.nextIndex[server] > reply.MatchIndex+1 {
				rf.nextIndex[server] = reply.MatchIndex + 1
			}
			DPrintf(dLog2, "[%d (%d)]: nextindex->%d", rf.me, rf.currentTerm, rf.nextIndex[server])
			// rf.nextIndex[server] = nextIndexLocal - 1
			// rf.nextIndex[server]--
			// rf.tasks.push(Task{Alignment, server, args.LogIndex})
			// DPrintf("ALIGNMENT fail")
		} else if reply.MatchIndex >= rf.lastIncludedIndex {
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
			// rf.tasks.push(Task{Replication, server, args.LogIndex})
			// DPrintf("REPLICATION failed because of connection")
			return ok
		} else if !reply.Success {
			// rf.nextIndex[server]--
			// rf.nextIndex[server] = nextIndexLocal - 1
			// rf.tasks.push(Task{Replication, server, args.LogIndex})
			// DPrintf("REPLICATION failed")
			rf.alignmentStatus[args.LogIndex-1][server] = false
		} else {
			rf.nextIndex[server] = args.LogIndex + 1
			rf.matchIndex[server] = args.LogIndex
			rf.replicationStatus[args.LogIndex-1][server] = true
			// DPrintf(dLog2, "[%d (%d)]: totalIndexLocal->%d", rf.me, rf.currentTerm, totalIndexLocal)
			// DPrintf(dLog2, "[%d (%d)]: settotrue->%d", rf.me, rf.currentTerm, args.LogIndex)
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
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)+rf.lastIncludedIndex <= args.LastLogIndex)) {

		DPrintf(dVote, "[%d (%d)]: vote granted to peer %d", rf.me, rf.currentTerm, args.CandidateID)
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
			DPrintf(dVote, "[%d (%d)]: vote denied to peer %d, not up-to-date", rf.me, rf.currentTerm, args.CandidateID)
		} else {
			DPrintf(dVote, "[%d (%d)]: vote denied to peer %d, already voted", rf.me, rf.currentTerm, args.CandidateID)
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
		index = len(rf.logs) + rf.lastIncludedIndex + 1
		term = rf.currentTerm
		isLeader = rf.appearingState == Leader
	}

	if isLeader {
		DPrintf(dLeader, "[%d (%d)]: Took command: %d", rf.me, rf.currentTerm, command)
		log := Log{
			Command: command,
			Term:    rf.currentTerm,
			Index:   len(rf.logs) + rf.lastIncludedIndex + 1,
		}
		rf.logs = append(rf.logs, log)
		// DPrintf(dDrop, "[%d (%d)]: len(logs)->%d", rf.me, rf.currentTerm, len(rf.logs))
		rf.alignmentStatus = append(rf.alignmentStatus, make([]bool, len(rf.peers)))
		rf.replicationStatus = append(rf.replicationStatus, make([]bool, len(rf.peers)))
		rf.persist()

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
	DPrintf(dInfo, "[%d (%d)]: KILLED!!!!!!!!!!!!", rf.me, rf.currentTerm)
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
	rf.lastIncludedIndex = 0
	rf.applyMsgQueue.InitializeTaskQueue()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.Run()

	return rf
}

func (rf *Raft) Run() {
	go rf.DeliverApplyMsg()

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
				DPrintf(dTimer, "[%d (%d)]: election timeout", rf.me, rf.currentTerm)
				rf.muState.Unlock()

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
	DPrintf(dDrop, "[%d (%d)]: become follower", rf.me, rf.currentTerm)

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
		LastLogIndex: len(rf.logs) + rf.lastIncludedIndex,
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
			// DPrintf(dLeader, "[%d (%d)]: reached", rf.me, rf.currentTerm)
			rf.muCommmit.Lock()

			lastHeartbeatTime := time.Now()

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				LeaderCommit: rf.commitIndex,
				TaskType:     Heartbeat,
			}
			reply := make([]AppendEntriesReply, len(rf.peers))

			for i := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntries(i, nil, nil, &args, &reply[i])
				}
			}

			rf.muCommmit.Unlock()
			rf.muState.Unlock()
			// DPrintf(dLeader, "[%d (%d)]: send heartbeat", rf.me, rf.currentTerm)

			nextHeartbeatTime := lastHeartbeatTime.Add(150 * time.Millisecond)
			delay := time.Until(nextHeartbeatTime)
			time.Sleep(delay)
		}
	}
}

func (rf *Raft) Leader() {
	DPrintf(dLeader, "[%d (%d)]: become leader", rf.me, rf.currentTerm)

	rf.muCommmit.Lock()

	quitHeartbeat := make(chan bool, 1)
	go rf.sendHeartbeat(quitHeartbeat)

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs) + 1 + rf.lastIncludedIndex
	}
	rf.alignmentStatus = make([][]bool, len(rf.logs)+rf.lastIncludedIndex)
	rf.replicationStatus = make([][]bool, len(rf.logs)+rf.lastIncludedIndex)
	for i := range rf.alignmentStatus {
		rf.alignmentStatus[i] = make([]bool, len(rf.peers))
		rf.replicationStatus[i] = make([]bool, len(rf.peers))
	}

	rf.appearingState = Leader
	DPrintf(dInfo, "[%d (%d)]: commitindex = %d, lii = %d", rf.me, rf.currentTerm, rf.commitIndex, rf.lastIncludedIndex)
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
		if rf.snapshot != nil {
			DPrintf(dInfo, "[%d (%d)]: read snapshot", rf.me, rf.currentTerm)
			rf.applyMsgQueue.push(ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			})
		}
	}

	rf.muCommmit.Unlock()

	rf.tasks.InitializeTaskQueue()

	rf.condLeader.Wait()

	quitHeartbeat <- true

	// DPrintf(dDrop, "[%d (%d)]: quit leader", rf.me, rf.currentTerm)
}

func (rf *Raft) replicateLog(logIndex int) {
	DPrintf(dLog, "[%d (%d)]: begin to replicate log at index %d", rf.me, rf.currentTerm, logIndex)

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
				if rf.nextIndex[i] == rf.lastIncludedIndex+1 {
					args.PrevLogTerm = rf.lastIncludedTerm
				} else if rf.nextIndex[i] < rf.lastIncludedIndex+1 {
					snapshotArgs := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						LogIndex:          args.LogIndex,
						Data:              rf.snapshot,
					}
					snapshotReply := InstallSnapshotReply{}
					go rf.sendInstallSnapshot(i, &snapshotArgs, &snapshotReply)
					DPrintf(dLog2, "[%d (%d)]: sending snapshot to %d", rf.me, rf.currentTerm, i)
					continue
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1-rf.lastIncludedIndex].Term
				}
				reply := AppendEntriesReply{}
				// DPrintf(dLog, "[%d (%d)]: sending alignment to [%d], pvi=%d", rf.me, rf.currentTerm, i, args.PrevLogIndex)
				go rf.sendAppendEntries(i, nil, nil, &args, &reply)
			} else if i != rf.me && !rf.replicationStatus[logIndex-1][i] {
				// DPrintf(dLog, "[%d (%d)]: try to replicate %d to [%d], pvi=%d", rf.me, rf.currentTerm, logIndex, i, rf.nextIndex[i]-1)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					LeaderCommit: rf.commitIndex,
					TaskType:     Replication,
					LogIndex:     logIndex,
				}
				// DPrintf(dDrop, "[%d (%d)]: len(logs)->%d", rf.me, rf.currentTerm, len(rf.logs))
				if rf.nextIndex[i] <= rf.lastIncludedIndex+1 {
					args.PrevLogTerm = -1
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-1-rf.lastIncludedIndex].Term
				}
				if rf.nextIndex[i]-1 < logIndex && rf.nextIndex[i] > rf.lastIncludedIndex { // TODO:
					// DPrintf(dLog2, "[%d (%d)]: nextindex=%d, lastIncludedIndex=%d", rf.me, rf.currentTerm, rf.nextIndex[i], rf.lastIncludedIndex)
					args.LogEntries = rf.logs[rf.nextIndex[i]-1-rf.lastIncludedIndex : logIndex-rf.lastIncludedIndex]
				} else {
					continue
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
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-rf.lastIncludedIndex].Command,
					CommandIndex: i + 1,
				}
				rf.applyMsgQueue.push(applyMsg)
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

func (rf *Raft) DeliverApplyMsg() {
	DPrintf(dCommit, "[%d (%d)]: apply channel on", rf.me, rf.currentTerm)

	for !rf.killed() {
		applyMsg := rf.applyMsgQueue.pop() // might block
		rf.applyCh <- applyMsg
		rf.lastApplied = applyMsg.CommandIndex
		DPrintf(dCommit, "[%d (%d)]: applied %d - %d", rf.me, rf.currentTerm, applyMsg.CommandIndex, applyMsg.Command)
	}
}

// ===== The End ===== //
