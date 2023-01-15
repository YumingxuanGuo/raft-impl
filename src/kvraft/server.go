package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Session struct {
	// Cond              *sync.Cond
	LatestSequenceNum int
	SavedValue        interface{}
	SavedErr          Err
}

type Op struct {
	Key         string
	Value       string
	Op          string
	ClientID    int64
	SequenceNum int
}

const (
	Discarded = 301
	Saved     = 302
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	restored bool

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister // Object to hold this peer's persisted state

	snapshot []byte

	stateMachine KVStateMachine
	sessions     map[int64]Session
	conds        map[int64]*sync.Cond

	condReg *sync.Cond
}

func (kv *KVServer) createSnapshot() []byte {
	// kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.sessions)
	e.Encode(kv.stateMachine)
	data := w.Bytes()
	// kv.mu.Unlock()

	DPrintf(dSnap, "kv %d: Snapshot created!", kv.me)

	return data
}

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var sessions map[int64]Session
	var stateMachine MemoryKV

	if d.Decode(&sessions) != nil {
		DPrintf(dSnap, "kv %d: Snapshot session FAILED!\n", kv.me)
	} else if d.Decode(&stateMachine) != nil {
		DPrintf(dSnap, "kv %d: Snapshot statemachine FAILED!\n", kv.me)
	} else {
		kv.sessions = sessions
		kv.stateMachine = stateMachine
		kv.restored = true
		DPrintf(dSnap, "kv %d: Snapshot decoded!", kv.me)

		for id := range kv.sessions {
			if _, existed := kv.conds[id]; !existed {
				kv.conds[id] = sync.NewCond(&kv.mu)
				DPrintf(dSnap, "kv %d: conds[%d] newed", kv.me, id)
			}
		}
	}
}

func (kv *KVServer) RegisterClient(args *RegisterClientArgs, reply *RegisterClientReply) {
	DPrintf(dRegister, "kv %d: Registering...\n", kv.me)

	resend := make(chan bool, 1)
	finished := make(chan RegisterClientReply, 1)
	quitTimer := make(chan bool, 1)
	go kv.Timer(resend, quitTimer)

	for {
		select {
		case <-resend:
			kv.mu.Lock()
			if !kv.restored {
				DPrintf(dRegister, "kv %d: not yet restored (Register)\n", kv.me)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			DPrintf(dRegister, "kv %d: (re)trying Register...\n", kv.me)
			tmpReply := &RegisterClientReply{}
			go kv.tryRegisterClient(finished, args, tmpReply)

		case tmpReply := <-finished:
			quitTimer <- true
			*reply = tmpReply
			DPrintf(dRegister, "kv %d: Register finished!\n", kv.me)
			return
		}
	}
}

func (kv *KVServer) tryRegisterClient(ch chan RegisterClientReply, args *RegisterClientArgs, reply *RegisterClientReply) {
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		ch <- *reply
	}()

	// return if this server is not leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf(dRegister, "kv %d: Not leader (Register)!", kv.me)
		reply.Status = ErrWrongLeader
		return
	}

	// append command to log, replicate and commit it
	op := Op{"", "", "Register", args.ClientID, args.SequenceNum}
	kv.rf.Start(op)

	for _, existed := kv.sessions[args.ClientID]; !existed; _, existed = kv.sessions[args.ClientID] {
		DPrintf(dRegister, "kv %d: registration.cond.wait", kv.me)
		kv.condReg.Wait()
	}

	DPrintf(dRegister, "kv %d: woke up (Register), finishing...", kv.me)

	reply.Status = OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf(dGet, "kv %d: Getting...\n", kv.me)

	resend := make(chan bool, 1)
	finished := make(chan GetReply, 1)
	quitTimer := make(chan bool, 1)
	go kv.Timer(resend, quitTimer)

	for {
		select {
		case <-resend:
			kv.mu.Lock()
			if !kv.restored {
				DPrintf(dGet, "kv %d: not yet restored (Get)\n", kv.me)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			DPrintf(dGet, "kv %d: (re)trying Get...\n", kv.me)
			tmpReply := &GetReply{}
			go kv.tryGet(finished, args, tmpReply)

		case tmpReply := <-finished:
			quitTimer <- true
			*reply = tmpReply
			DPrintf(dGet, "kv %d: Get finished!\n", kv.me)
			return
		}
	}
}

func (kv *KVServer) tryGet(ch chan GetReply, args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		ch <- *reply
	}()

	// return if this server is not leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf(dGet, "kv %d: Not leader (Get)!", kv.me)
		reply.Status = ErrWrongLeader
		return
	}

	// DPrintf("sequence num = %d", args.SequenceNum)

	// return if no record of ClientID or the response was discarded
	session, ok := kv.sessions[args.ClientID]
	if !ok {
		DPrintf(dGet, "kv %d: client %d not registered!", kv.me, args.ClientID)
		reply.Status = SessionExpired
		return
	}
	if args.SequenceNum < session.LatestSequenceNum {
		DPrintf(dGet, "kv %d: client.seq = %d < session.seq = %d!", kv.me, args.SequenceNum, session.LatestSequenceNum)
		reply.Status = SessionExpired
		return
	}

	if args.SequenceNum == session.LatestSequenceNum {
		reply.Value = kv.sessions[args.ClientID].SavedValue.(string)
		reply.Status = kv.sessions[args.ClientID].SavedErr
		return
	}

	// append command to log, replicate and commit it
	DPrintf(dGet, "kv %d: Start()ing on raft...", kv.me)
	op := Op{args.Key, "", "Get", args.ClientID, args.SequenceNum}
	kv.rf.Start(op)

	for args.SequenceNum > kv.sessions[args.ClientID].LatestSequenceNum {
		DPrintf(dGet, "kv %d: wating (Get): client.seq = %d, session.seq = %d\n", kv.me, args.SequenceNum, kv.sessions[args.ClientID].LatestSequenceNum)
		// kv.sessions[args.ClientID].Cond.Wait()
		kv.conds[args.ClientID].Wait()
	}

	// apply command in log order
	reply.Value = kv.sessions[args.ClientID].SavedValue.(string)
	reply.Status = kv.sessions[args.ClientID].SavedErr

	DPrintf(dGet, "kv %d: woke up (Get), finishing...", kv.me)

	// fmt.Printf("kv %d client %d (Get): client.seqnum = %d, session.seqnum = %d\n", kv.me, args.ClientID, args.SequenceNum, kv.sessions[args.ClientID].latestSequenceNum)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf(dPutAppend, "kv %d: PutAppending %v(%v, %v)...\n", kv.me, args.Op, args.Key, args.Value)

	resend := make(chan bool, 1)
	finished := make(chan PutAppendReply, 1)
	quitTimer := make(chan bool, 1)
	go kv.Timer(resend, quitTimer)

	started := false

	for {
		select {
		case <-resend:
			kv.mu.Lock()
			if !kv.restored {
				DPrintf(dPutAppend, "kv %d: not yet restored (PutAppend)\n", kv.me)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()

			DPrintf(dPutAppend, "kv %d: (re)trying PutAppend...\n", kv.me)
			tmpReply := &PutAppendReply{}
			go kv.tryPutAppend(finished, args, tmpReply, &started)

		case tmpReply := <-finished:
			quitTimer <- true
			*reply = tmpReply
			DPrintf(dPutAppend, "kv %d: PutAppend finished!\n", kv.me)
			return
		}
	}
}

func (kv *KVServer) tryPutAppend(ch chan PutAppendReply, args *PutAppendArgs, reply *PutAppendReply, started *bool) {
	kv.mu.Lock()
	defer func() {
		kv.mu.Unlock()
		ch <- *reply
	}()

	// return if this server is not leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf(dPutAppend, "kv %d: Not leader (PutAppend)!", kv.me)
		reply.Status = ErrWrongLeader
		return
	}

	// return if no record of ClientID or the response was discarded
	session, ok := kv.sessions[args.ClientID]
	if !ok {
		DPrintf(dPutAppend, "kv %d: client %d not registered!", kv.me, args.ClientID)
		reply.Status = SessionExpired
		return
	}
	if args.SequenceNum < session.LatestSequenceNum {
		DPrintf(dPutAppend, "kv %d: client.seq = %d < session.seq = %d!", kv.me, args.SequenceNum, session.LatestSequenceNum)
		reply.Status = SessionExpired
		return
	}

	if args.SequenceNum == session.LatestSequenceNum {
		reply.Status = OK
		return
	}

	// append command to log, replicate and commit it
	DPrintf(dPutAppend, "kv %d: Start()ing on raft...", kv.me)
	op := Op{args.Key, args.Value, args.Op, args.ClientID, args.SequenceNum}
	kv.rf.Start(op)
	*started = true

	for args.SequenceNum > kv.sessions[args.ClientID].LatestSequenceNum {
		DPrintf(dPutAppend, "kv %d: wating (PutAppend): client.seq = %d, session.seq = %d\n", kv.me, args.SequenceNum, kv.sessions[args.ClientID].LatestSequenceNum)
		// kv.sessions[args.ClientID].Cond.Wait()
		kv.conds[args.ClientID].Wait()
	}

	// fmt.Printf("kv %d client %d (PutAppend): client.seqnum = %d, session.seqnum = %d\n", kv.me, args.ClientID, args.SequenceNum, kv.sessions[args.ClientID].latestSequenceNum)
	reply.Status = OK

	DPrintf(dPutAppend, "kv %d: woke up (PutAppend), finishing...", kv.me)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.condReg = sync.NewCond(&kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sessions = make(map[int64]Session)
	kv.conds = make(map[int64]*sync.Cond)
	kv.stateMachine = CreateMemoryKV()
	kv.persister = persister
	kv.decodeSnapshot(persister.ReadSnapshot())

	kv.restored = true
	DPrintf(dInfo, "kv %d: starting kvserver...", kv.me)

	// You may need initialization code here.
	go kv.ProcessApplyMsgs()

	// go kv.Restore()

	return kv
}

func (kv *KVServer) Restore() {
	DPrintf(dInfo, "kv %d: restoring...", kv.me)

	for !kv.killed() {
		kv.mu.Lock()

		if kv.restored {
			DPrintf(dInfo, "kv %d: restore finished!", kv.me)
			kv.mu.Unlock()
			return
		}

		kv.rf.Start(Op{Op: "RestorationBroadcast"})

		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) ProcessApplyMsgs() {
	for !kv.killed() {
		msg := <-kv.applyCh // block

		kv.mu.Lock()

		// commit message
		if msg.CommandValid {
			// DPrintf(dInfo, "kv %d: handling #%d", kv.me, msg.CommandIndex)

			op := msg.Command.(Op)
			if op.Op == "Register" {
				if _, ok := kv.sessions[op.ClientID]; !ok {
					DPrintf(dApply, "kv %d: apply registering id = %d, session.sqn = %d", kv.me, op.ClientID, op.SequenceNum-1)
					DPrintf(dApply, "kv %d: broadcasting condReg...", kv.me)
					// kv.sessions[op.ClientID] = Session{sync.NewCond(&kv.mu), -1, nil, OK}
					// kv.sessions[op.ClientID] = Session{sync.NewCond(&kv.mu), op.SequenceNum - 1, nil, OK}
					kv.sessions[op.ClientID] = Session{op.SequenceNum - 1, nil, OK}
					kv.conds[op.ClientID] = sync.NewCond(&kv.mu)
				}
				kv.condReg.Broadcast()
				// kv.mu.Unlock()
				// continue
			} else if op.Op == "RestorationBroadcast" {
				kv.restored = true
				DPrintf(dApply, "kv %d: apply RestorationBroadcast", kv.me)
				// kv.mu.Unlock()
				// continue
			} else {
				session, ok := kv.sessions[op.ClientID]
				cond, ok := kv.conds[op.ClientID]
				if ok {
					// apply the operation if it's new
					if op.SequenceNum > session.LatestSequenceNum {
						DPrintf(dApply, "kv %d: applying operation... (op.sqn = %d, session.sqn = %d)", kv.me, op.SequenceNum, session.LatestSequenceNum)
						var value string
						var err Err
						if op.Op == "Put" {
							err = kv.stateMachine.Put(op.Key, op.Value)
							DPrintf(dInfo, "kv %d: Put(%v, %v) applied!", kv.me, op.Key, op.Value)
						} else if op.Op == "Append" {
							err = kv.stateMachine.Append(op.Key, op.Value)
							DPrintf(dInfo, "kv %d: Append(%v, %v) from id = %d applied!", kv.me, op.Key, op.Value, op.ClientID)
						} else if op.Op == "Get" {
							value, err = kv.stateMachine.Get(op.Key)
							DPrintf(dInfo, "kv %d: Get(%v) applied!", kv.me, op.Key)
						}
						// save output of this sequence number for this client, discard prior responce
						// kv.sessions[op.ClientID] = Session{session.Cond, session.LatestSequenceNum + 1, value, err}
						kv.sessions[op.ClientID] = Session{session.LatestSequenceNum + 1, value, err}
					}
					// kv.sessions[op.ClientID].Cond.Broadcast()
					DPrintf(dClient, "kv %d: broadcasting conds[%d]...", kv.me, op.ClientID)
					cond.Broadcast()
				} else {
					DPrintf(dInfo, "kv %d: operation duplicated, op.sqn = %d, session.sqn = %d", kv.me, op.SequenceNum, session.LatestSequenceNum)
				}
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= 2*kv.maxraftstate {
				DPrintf(dSnap, "kv %d: creating snapshot...", kv.me)
				snapshot := kv.createSnapshot()
				DPrintf(dSnap, "kv %d: telling raft to drop logs...", kv.me)
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
				DPrintf(dSnap, "kv %d: raft dropping finished!", kv.me)
			}

		} else if msg.SnapshotValid {
			DPrintf(dSnap, "kv %d: applying incoming snapshot...", kv.me)
			kv.restored = false
			kv.decodeSnapshot(msg.Snapshot)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Timer(ch chan bool, quit chan bool) {
	for {
		select {
		case <-quit:
			return

		default:
			ch <- true
			time.Sleep(500 * time.Millisecond)
		}
	}
}
