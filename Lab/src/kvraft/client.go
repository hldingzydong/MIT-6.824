package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeaderId int
	uuidCount int64           // int64 maybe enough to pass the test
	clerkId		int64
	mu        sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

const CDebug = 0

func CDPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfForPutAppend(key string, value string, op string, serverId int, uuid int64, clerkId int64) {
	if op == "Put" {
		CDPrintf("client.go: Clerk(%d) try to put <%s,%s>(uuid=%d) into Server[%d]", clerkId, key, value, uuid, serverId)
	}else{
		CDPrintf("client.go: Clerk(%d) try to append <%s,%s>(uuid=%d) into Server[%d]", clerkId, key, value, uuid, serverId)
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeaderId = 0
	ck.uuidCount = 1
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
		CDPrintf("client.go: Clerk(%d) try to get <%s>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getArgs.Uuid, lastLeaderId)
		if ok {
			if getReply.Err == OK {
				CDPrintf("client.go: Clerk(%d) receive GetRPC reply(OK) <%v,%v>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getReply.Value, getArgs.Uuid, lastLeaderId)
				return getReply.Value
			} else if getReply.Err == ErrNoKey {
				CDPrintf("client.go: Clerk(%d) receive GetRPC reply(ErrNoKey) <%v>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getArgs.Uuid, lastLeaderId)
				return ""
			}
		}
	}

	for {
		var getReply GetReply
		// choose a random server
		lastLeaderId = (lastLeaderId+1)%len(ck.servers)
		ok := ck.servers[lastLeaderId].Call("KVServer.Get", &getArgs, &getReply)
		CDPrintf("client.go: Clerk(%d) try to get <%s>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getArgs.Uuid, lastLeaderId)
		if ok {
			if getReply.Err == OK {
				CDPrintf("client.go: Clerk(%d) receive GetRPC reply(OK) <%v,%v>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getReply.Value, getArgs.Uuid, lastLeaderId)
				ck.mu.Lock()
				ck.lastLeaderId = lastLeaderId
				ck.mu.Unlock()
				return getReply.Value
			} else if getReply.Err == ErrNoKey {
				CDPrintf("client.go: Clerk(%d) receive GetRPC reply(ErrNoKey) <%v>(uuid=%d) from Server[%d]", getArgs.ClerkId, key, getArgs.Uuid, lastLeaderId)
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
		//DPrintfForPutAppend(key, value, op, lastLeaderId, putAppendArgs.Uuid, putAppendArgs.ClerkId)
		if ok && putAppendReply.Err == OK {
			CDPrintf("client.go: Clerk(%d) receive PutAppendRPC reply(OK) <%v,%v>(uuid=%d) from Server[%d]", putAppendArgs.ClerkId, key, value, putAppendArgs.Uuid, lastLeaderId)
			return
		}
	}

	// execute to this step means last step is not success, so need to try other servers
	for {
		var putAppendReply PutAppendReply
		// choose a random server
		lastLeaderId = (lastLeaderId+1)%len(ck.servers)
		ok := ck.servers[lastLeaderId].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		//DPrintfForPutAppend(key, value, op, lastLeaderId, putAppendArgs.Uuid, putAppendArgs.ClerkId)
		if ok && putAppendReply.Err == OK {
			CDPrintf("client.go: Clerk(%d) receive PutAppendRPC reply(OK) <%v,%v>(uuid=%d) from Server[%d]", putAppendArgs.ClerkId, key, value, putAppendArgs.Uuid, lastLeaderId)
			ck.mu.Lock()
			ck.lastLeaderId = lastLeaderId
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
