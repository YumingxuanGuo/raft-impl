package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	me      int64

	lastKnownLeader int
	sequenceNum     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// DPrintf(dGet, "CK %d: Calling Get...", ck.me)
	reply := GetReply{}
	for ok := false; true; {
		args := GetArgs{
			Key:         key,
			ClientID:    ck.me,
			SequenceNum: ck.sequenceNum,
		}
		reply = GetReply{}
		ok = ck.servers[ck.lastKnownLeader].Call("KVServer.Get", &args, &reply)

		DPrintf(dGet, "CK %d: Sequence num = %d", ck.me, args.SequenceNum)

		// call RPC again if kvserver cannot be reached or is not leader
		if !ok || reply.Status == ErrWrongLeader {
			ck.lastKnownLeader = (ck.lastKnownLeader + 1) % len(ck.servers)
			DPrintf(dGet, "CK %d: No leader in Get?", ck.me)
			continue
		}

		if reply.Status == SessionExpired {
			ck.RegisterClient()
			continue
		}

		// return result string if available
		if reply.Status == OK || reply.Status == ErrNoKey {
			break
		}
	}

	ck.sequenceNum++
	// DPrintf(dGet, "CK %d: client get finished, sequence num -> %d\n", ck.me, ck.sequenceNum)

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// DPrintf(dPutAppend, "CK %d: Calling %v(%v, %v)", ck.me, op, key, value)

	for ok := false; true; {
		args := PutAppendArgs{
			Key:         key,
			Value:       value,
			Op:          op,
			ClientID:    ck.me,
			SequenceNum: ck.sequenceNum,
		}
		reply := PutAppendReply{}
		ok = ck.servers[ck.lastKnownLeader].Call("KVServer.PutAppend", &args, &reply)

		// call RPC again if kvserver cannot be reached or is not leader
		if !ok || reply.Status == ErrWrongLeader {
			ck.lastKnownLeader = (ck.lastKnownLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Status == SessionExpired {
			ck.RegisterClient()
			continue
		}

		// return result string if available
		if reply.Status == OK {
			break
		}
	}

	ck.sequenceNum++
	// DPrintf(dPutAppend, "CK %d: client putappend finished, sequence num -> %d\n", ck.me, ck.sequenceNum)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) RegisterClient() {
	// DPrintf(dRegister, "CK %d: Registering...", ck.me)

	reply := RegisterClientReply{}
	for ok := false; true; {

		args := RegisterClientArgs{ck.me, ck.sequenceNum}
		reply = RegisterClientReply{}

		ok = ck.servers[ck.lastKnownLeader].Call("KVServer.RegisterClient", &args, &reply)

		// call RPC again if kvserver cannot be reached or is not leader
		if !ok || reply.Status == ErrWrongLeader {
			if !ok {
				// DPrintf(dInfo, "NOT CONNECTED")
			}
			ck.lastKnownLeader = (ck.lastKnownLeader + 1) % len(ck.servers)
			continue
		}

		// return result string if available
		if reply.Status == OK {
			break
		}
	}
}
