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
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	"fmt"
	"math/rand"
	"runtime"
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

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// coTicker  *sync.Cond
	// mutex     sync.Mutex
	muTime    sync.Mutex
	muState   sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int // latest term server has seen (initialized to 0 on first boot)
	votedFor    int // candidateID that received vote in current term (-1 if none)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated

	// Extra state:
	lastCommunicationTime  time.Time // time deadline for receiving next communication
	currentElectionTimeout time.Duration
	currentState           int

	quitCandidate chan bool
	quitLeader    chan bool
	quitTicker    chan bool

	// tickerOn bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.muState.Lock()
	term := rf.currentTerm
	isLeader := rf.currentState == Leader
	rf.muState.Unlock()
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.muState.Lock()

	// for leader to update itself
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		rf.ResetElectionTimeout()
		if rf.currentState == Candidate {
			// rf.muState.Unlock()
			rf.quitCandidate <- true
			// rf.LOG("candidate->follower")
			// rf.muState.Lock()
		} else if rf.currentState == Leader {
			// rf.muState.Unlock()
			rf.quitLeader <- true
			// rf.LOG("leader->follower")
			// rf.muState.Lock()
		}

		rf.currentState = Follower
		reply.Success = true

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			// rf.LOG(fmt.Sprintf("accepted heartbeat from %d", args.LeaderID))
		}
	}
	rf.muState.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	// rule no.2
	rf.muState.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.votedFor = -1
		rf.quitLeader <- true
	}
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
	// Your code here (2A, 2B).
	rf.muState.Lock()

	// for candidate to update itself
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
			reply.VoteGranted = false
		} else {
			rf.ResetElectionTimeout()
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		}
	} else {
		if rf.currentState == Candidate {
			// rf.muState.Unlock()
			// rf.LOG("candidate->follower")
			rf.quitCandidate <- true
			// rf.muState.Lock()
		} else if rf.currentState == Leader {
			// rf.muState.Unlock()
			rf.quitLeader <- true
			// rf.LOG("leader->follower")
			// rf.muState.Lock()
		}
		rf.ResetElectionTimeout()
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}
	// if !reply.VoteGranted {
	// 	rf.LOG("vote denied")
	// } else {
	// 	rf.LOG("vote granted")
	// }
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
func (rf *Raft) sendRequestVote(server int, ch chan bool,
	args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// rule no.2
	rf.muState.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.votedFor = -1
		rf.quitCandidate <- true
	}
	rf.muState.Unlock()

	// if ok {
	// 	rf.LOG(fmt.Sprintf("RPC to %d send succeed", server))
	// }
	ch <- reply.VoteGranted
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
	isLeader := true

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.muState.Lock()

	// check if another ticker is running
	// if rf.tickerOn {
	// 	// rf.muState.Unlock()
	// 	// return
	// 	rf.quitTicker <- false
	// 	rf.LOG("send update")
	// }
	// rf.tickerOn = true

	// initialize quit channels
	// rf.mutex.Lock()
	rf.quitCandidate = make(chan bool, len(rf.peers))
	rf.quitTicker = make(chan bool, 1)
	quitTicker := rf.quitTicker

	rf.muState.Unlock()

	rf.ResetElectionTimeout()
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-quitTicker:
			// rf.LOG("ticker quit")
			// rf.muState.Lock()
			// rf.tickerOn = false
			// if !flag {
			// 	rf.LOG("update")
			// }
			// rf.muState.Unlock()
			return
			// rf.coTicker.Wait()
		default:
			// sleep until next checkpoint
			rf.muTime.Lock()
			lastCommunicationTime := rf.lastCommunicationTime
			nextCheckingTime := rf.lastCommunicationTime.Add(time.Duration(rf.currentElectionTimeout))
			rf.muTime.Unlock()
			delay := time.Until(nextCheckingTime)
			time.Sleep(delay)

			// begin leader selection if no new communication has been made
			rf.muTime.Lock()
			receivedNewCommunication := rf.lastCommunicationTime.After(lastCommunicationTime)
			rf.muTime.Unlock()
			if !receivedNewCommunication {
				rf.muState.Lock()

				// terminate last leader selection, if any
				rf.quitCandidate <- true
				// rf.LOG("because of time out")

				// restore server states
				rf.votedFor = -1
				rf.quitCandidate = make(chan bool, len(rf.peers))

				rf.muState.Unlock()

				// reset timer: starting an election
				rf.ResetElectionTimeout()
				go rf.BecomeCandidate()
			}
		}
	}
	// rf.mutex.Unlock()
}

func (rf *Raft) BecomeCandidate() {
	rf.muState.Lock()

	// update server to candidate
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	quitCandidate := rf.quitCandidate
	// rf.LOG("inc")

	// argument initialization
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		0,
		0,
	}
	replies := make([]RequestVoteReply, len(rf.peers))

	ch := make(chan bool)

	rf.muState.Unlock()

	// rf.LOG("Start election")

	// request votes from peers
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, ch, &args, &replies[i])
		}
	}

	// count the vote; break if quorum is achieved
	voteCount := 1
	for voteCount < (len(rf.peers)+1)/2 {
		select {
		case <-quitCandidate:
			// rf.LOG(fmt.Sprintf("quit election"))
			return
		case voteGranted := <-ch:
			if voteGranted {
				voteCount++
			}
		}
	}

	go rf.BecomeLeader()
}

func (rf *Raft) BecomeLeader() {
	// rf.LOG("Leader")
	rf.ResetElectionTimeout()

	// update server to leader
	rf.muState.Lock()
	// if rf.quitLeader != nil && rf.currentState == Leader {
	// 	rf.quitLeader <- true
	// 	rf.LOG("quit already exist leader")
	// }
	rf.currentState = Leader
	rf.quitLeader = make(chan bool, len(rf.peers))
	quitLeader := rf.quitLeader

	// exit the ticker() goroutine
	rf.quitTicker <- true

	// argument initialization
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}

	rf.muState.Unlock()

	// send heartbeats repeatedly
	for rf.killed() == false {
		select {
		case <-quitLeader:
			// rf.LOG("quit leader")
			go rf.ticker()
			// rf.ResetElectionTimeout()
			// rf.coTicker.Broadcast()
			return
		default:
			lastCommunicationTime := time.Now()
			replies := make([]AppendEntriesReply, len(rf.peers))
			// rf.LOG(fmt.Sprintf("heartbeat sent from %d", rf.currentState))
			for i := range rf.peers {
				if i != rf.me {
					go rf.sendAppendEntries(i, &args, &replies[i])
				}
			}
			// rf.LOG("!")
			// rf.ResetElectionTimeout()
			// rf.muTime.Lock()
			nextCheckingTime := lastCommunicationTime.Add(150 * time.Millisecond)
			// rf.muTime.Unlock()
			delay := time.Until(nextCheckingTime)
			// rf.LOG(delay.String())
			time.Sleep(delay)
		}
	}
}

func (rf *Raft) ResetElectionTimeout() {
	// election timeout range: [250, 750]
	rf.muTime.Lock()
	rf.lastCommunicationTime = time.Now()
	electionTimeout := rand.Int63n(500) + 500 // in milliseconds
	rf.currentElectionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.muTime.Unlock()
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
	rf.votedFor = -1
	// rf.coTicker = sync.NewCond(&rf.mutex)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rs *Raft) LOG(msg string) {
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	fmt.Printf("Line %d, Peer[%d(%d)]: %s\n", frame.Line, rs.me, rs.currentTerm, msg)
}
