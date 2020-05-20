package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"strconv"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType 		string
	OpClerkId   int64
	OpUuid   	int64 
	Key			string
	Value		string
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	dead    		int32 // set by Kill()

	maxraftstate 	int // snapshot if log grows this big

	lastApplyIdMap	map[int64]int64   // store <clerkId,lastApplyRequestUuid>
	clerkChannelMap map[string]chan Op // store <clerkId, clerkNotifyChannel>
	serverMap		map[string]string // store <key,value>   
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.OpUuid = args.Uuid
	op.OpClerkId = args.ClerkId
	op.OpType = "Get"
	op.Key = args.Key

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}else{
		DPrintf("server.go: Leader[%d] receive Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
		// 由clerkId + uuid 可以确认全剧唯一的请求，依此对该请求生成一个channel
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(1000) * time.Millisecond)
		defer delete(kv.clerkChannelMap, requestUuid)
		select {
			case <-timeoutTimer.C:
				reply.Err = ErrWrongLeader
				DPrintf("server.go: Leader[%d] timeout aftre receiving Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
				return
			case newOp := <- tmpChan:
				if newOp == op {
					kv.mu.Lock()
					defer kv.mu.Unlock()

					DPrintf("server.go: Leader[%d] respond to Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
					lastValue, ok := kv.serverMap[op.Key]
					if ok {
						reply.Err = OK
						reply.Value = lastValue
					}else{
						reply.Err = ErrNoKey
					}
					kv.lastApplyIdMap[op.OpClerkId] = op.OpUuid
					return
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.OpUuid = args.Uuid
	op.OpClerkId = args.ClerkId
	op.OpType = args.Op
	op.Key = args.Key 
	op.Value = args.Value 

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}else{
		DPrintf("server.go: Leader[%d] receive PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s)", kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)
		// 该Server是leader
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()
		
		// 定义一个倒计时器,当倒计时到达时若还未从applyCh拿到response则reply ”ErrWrongLeader"
		timeoutTimer := time.NewTimer(time.Duration(5000) * time.Millisecond)
		defer delete(kv.clerkChannelMap, requestUuid)
		select {
			case <-timeoutTimer.C:
				reply.Err = ErrWrongLeader
				DPrintf("server.go: Leader[%d] timeout aftre receiving PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s))", 
					kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)
				return
			case newOp := <- tmpChan:
				if newOp == op {
					kv.mu.Lock()
					defer kv.mu.Unlock()

					DPrintf("server.go: Leader[%d] respond to PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s)", 
							kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)
					if op.OpType == "Put" {
						kv.serverMap[op.Key] = op.Value
						kv.lastApplyIdMap[op.OpClerkId] = op.OpUuid
					}else{
						lastValue, ok := kv.serverMap[op.Key]
						if ok {
							kv.serverMap[op.Key] = lastValue + op.Value
						}else{
							kv.serverMap[op.Key] = op.Value
						}
						kv.lastApplyIdMap[op.OpClerkId] = op.OpUuid
					}

					reply.Err = OK
					return
			}
		}
	}
}


func (kv *KVServer) DaemonThread() {
	for !kv.killed() {
		applyMsg := <- kv.applyCh
		DPrintf("Server[%d] read an applyMsg[%v] from its applyCh", kv.me, applyMsg)
		if !applyMsg.CommandValid {
			continue
		}else{
			getOp := applyMsg.Command.(Op)
			// check if duplicate
			clerkId := getOp.OpClerkId
			uuid := getOp.OpUuid
			// 如果该次applyMsg所对应的request已经apply 过一次,则不会再apply了
			kv.mu.Lock()
			lastUuid, ok := kv.lastApplyIdMap[clerkId]
			if ok && lastUuid >= uuid {
				kv.mu.Unlock()
				continue
			}else{
				DPrintf("server.go: Server[%d] get applyMsg from Clerk[%d](Uuid=%d)", kv.me, clerkId, uuid)
				kv.lastApplyIdMap[clerkId] = uuid
				// find this request 对应的channel
				requestUuid := strconv.FormatInt(clerkId,10) + strconv.FormatInt(uuid,10)
				clerkChan, ok := kv.clerkChannelMap[requestUuid]
				if ok {
					clerkChan <- getOp
				} 
			}
			kv.mu.Unlock()
		}
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplyIdMap = make(map[int64]int64)
	kv.serverMap = make(map[string]string)
	kv.clerkChannelMap = make(map[string]chan Op)

	go kv.DaemonThread()

	return kv
}
