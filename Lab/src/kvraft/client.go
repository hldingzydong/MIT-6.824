package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	clerkId		int64
	lastLeaderId int
	uuidCount int64           // int64 maybe enough to pass the test
	mu        sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func DPrintfForPutAppend(key string, value string, op string, serverId int) {
	if op == "Put" {
		DPrintf("Clerk put <%s,%s> into Server[%d]", key, value, serverId)
	}else{
		DPrintf("Clerk append <%s,%s> into Server[%d]", key, value, serverId)
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeaderId = -1
	ck.uuidCount = 0
	ck.clerkId = nrand()
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	var getArgs GetArgs
	var getReply GetReply
	var lastLeaderId int

	getArgs.Key = key
	getArgs.ClerkId = ck.clerkId
	ck.mu.Lock()
	getArgs.Uuid = ck.uuidCount
	ck.uuidCount++
	lastLeaderId = ck.lastLeaderId
	ck.mu.Unlock()
	
	if lastLeaderId != -1 && lastLeaderId < len(ck.servers) {
		ok := ck.servers[lastLeaderId].Call("KVServer.Get", &getArgs, &getReply)
		DPrintf("Cleck get <%s> from Server[%d]", key, lastLeaderId)
		if ok == nil {
			if reply.Err == "OK" {
				return reply.Value
			} else if reply.Err = "ErrNoKey" {
				return ""
			}
		}
	}

	for {
		// choose a random server
		randomServer := nrand()%len(ck.servers)
		ok := ck.servers[randomServer].Call("KVServer.Get", &getArgs, &getReply)
		DPrintf("Cleck get <%s> from Server[%d]", key, randomServer)
		if ok == nil {
			if reply.Err == "OK" {
				return reply.Value
			} else if reply.Err = "ErrNoKey" {
				return ""
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var putAppendArgs PutAppendArgs
	var putAppendReply PutAppendReply
	var lastLeaderId int

	putAppendArgs.Key = key
	putAppendArgs.Value = value
	putAppendArgs.Op = op
	putAppendArgs.ClerkId = ck.clerkId

	ck.mu.Lock()
	putAppendArgs.Uuid = ck.uuidCount
	ck.uuidCount++
	lastLeaderId = ck.lastLeaderId
	ck.mu.Unlock()

	if lastLeaderId != -1 && lastLeaderId < len(ck.servers) {
		ok := ck.servers[lastLeaderId].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		DPrintfForPutAppend(key, value, op, lastLeaderId)
		if ok == nil {
			if putAppendReply.Err == "OK" {
				return
			}
		}
	}

	// execute to this step means last step is not success, so need to try other servers
	for {
		// choose a random server
		randomServer := nrand()%len(ck.servers)
		ok := ck.servers[lastLeaderId].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		DPrintfForPutAppend(key, value, op, randomServer)
		if ok == nil && putAppendReply.Err == "OK" {
			ck.mu.Lock()
			ck.lastLeaderId = randomServer
			ck.mu.Unlock()
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
