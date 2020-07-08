package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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

	lastApplyIdMap	map[int64]int64    // store <clerkId,lastApplyRequestUuid>
	clerkChannelMap map[string]chan Op // store <clerkId+uuid, clerkNotifyChannel>
	serverMap		map[string]string  // store <key,value>

	lastLogIndex    int                // global view
}


//
// Get RPC handler
//
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.OpUuid = args.Uuid
	op.OpClerkId = args.ClerkId
	op.OpType = "Get"
	op.Key = args.Key

	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}else{
		//DPrintf("server.go: Leader[%d] receive Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
		// 由clerkId + uuid 可以确认全剧唯一的请求，依此对该请求生成一个channel
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)
		select {
			case <-timeoutTimer.C:
				kv.mu.Lock()
				DPrintf("server.go: Leader[%d] timeout aftre receiving Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
				if kv.isDuplicate(args.ClerkId, args.Uuid) {
					reply.Err = OK
					reply.Value = kv.serverMap[op.Key]
				}else{
					reply.Err = ErrWrongLeader
				}
				
				delete(kv.clerkChannelMap, requestUuid)
				kv.mu.Unlock()
				return

			case <-tmpChan:
				kv.mu.Lock()
				defer kv.mu.Unlock()

				//DPrintf("server.go: Leader[%d] respond to Get RPC from Clerk[%d](args.Uuid=%d, args.Key=%s)", kv.me, args.ClerkId, args.Uuid, args.Key)
				lastValue, ok := kv.serverMap[op.Key]
				if ok {
					reply.Err = OK
					reply.Value = lastValue
				}else{
					reply.Err = ErrNoKey
				}
				delete(kv.clerkChannelMap, requestUuid)
				return
		}
	}
}

//
// PutAppend RPC handler
//
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	op.OpUuid = args.Uuid
	op.OpClerkId = args.ClerkId
	op.OpType = args.Op
	op.Key = args.Key 
	op.Value = args.Value 

	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}else{
		DPrintf("server.go: Leader[%d] receive PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s)", kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)
		// 该Server是leader
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()
		
		// 定义一个倒计时器,当倒计时到达时若还未从applyCh拿到response则reply ”ErrWrongLeader"
		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)
		select {
			case <-timeoutTimer.C:

				kv.mu.Lock()
				DPrintf("server.go: Leader[%d] timeout aftre receiving PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s))", 
					kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)

				if kv.isDuplicate(op.OpClerkId, op.OpUuid) {
					reply.Err = OK
				}else{
					reply.Err = ErrWrongLeader
				}
				
				delete(kv.clerkChannelMap, requestUuid)
				kv.mu.Unlock()
				return

			case <-tmpChan:
				kv.mu.Lock()
				defer kv.mu.Unlock()

				DPrintf("server.go: Leader[%d] respond to PutAppend RPC from Clerk[%d](args.Uuid=%d, args.Key=%s, args.Value=%s)", 
						kv.me, args.ClerkId, args.Uuid, args.Key, args.Value)
				
				reply.Err = OK
				delete(kv.clerkChannelMap, requestUuid)
				return
		}
	}
}


//
// if the request is ever applied
//
func (kv *KVServer) isDuplicate(clerkId int64, uuid int64) bool {
	lastRequestUuid, ok := kv.lastApplyIdMap[clerkId]
	if !ok || lastRequestUuid < uuid {
		return false
	}
	return true
}


//
// backup thread for a kv server
// read apply message from applyCh and update its <k,v>
// and notify RPC handler reply to clerks
//
func (kv *KVServer) DaemonThread() {
	for !kv.killed() {
		applyMsg := <- kv.applyCh
		DPrintf("server.go: Server[%d] read an applyMsg[%v] from its applyCh", kv.me, applyMsg)
		if !applyMsg.CommandValid {
			// this is a snapshot come from Leader
			DPrintf("server.go: Server[%d] get snapshot from its Raft", kv.me)
			snapshotInBytes := applyMsg.CommandSnapshot
			kv.mu.Lock()
			r := bytes.NewBuffer(snapshotInBytes)
    		d := labgob.NewDecoder(r)
    		if d.Decode(&kv.serverMap) != nil || d.Decode(&kv.lastApplyIdMap) != nil {
        		return
    		}
			kv.mu.Unlock()
		}else{
			getOp := applyMsg.Command.(Op)
			getIndex := applyMsg.CommandIndex

			clerkId := getOp.OpClerkId
			uuid := getOp.OpUuid
			requestUuid := strconv.FormatInt(clerkId,10) + strconv.FormatInt(uuid,10)

			// 如果该次applyMsg所对应的request已经apply 过一次,则不会再apply了
			kv.mu.Lock()
			if getIndex > kv.lastLogIndex {
				kv.lastLogIndex = getIndex
			}

			if !kv.isDuplicate(clerkId, uuid) {
				DPrintf("server.go: Server[%d] get applyMsg from Clerk[%d](Uuid=%d)", kv.me, clerkId, uuid)
				kv.lastApplyIdMap[clerkId] = uuid
					 
				if getOp.OpType == "Put" {
					kv.serverMap[getOp.Key] = getOp.Value
				}else if getOp.OpType == "Append" {
					lastValue, ok := kv.serverMap[getOp.Key]
					if ok {
						kv.serverMap[getOp.Key] = lastValue + getOp.Value
					}else{
						kv.serverMap[getOp.Key] = getOp.Value
					}
				}

				// check if need to install snapshot
				if kv.isNeedToSnapshot() {
					kv.callRaftStartSnapshot()
				}
			}else{
				// check if need to install snapshot
				if kv.isNeedToSnapshot() {
					kv.callRaftStartSnapshot()
				}
				kv.mu.Unlock()
				continue
			}
				
			clerkChan, ok := kv.clerkChannelMap[requestUuid]
			if ok {
				clerkChan <- getOp
			}
			kv.mu.Unlock()
		}
	}
}


//
// when kv.maxraftstate > -1, it means that the kv server needs snapshot
// then it needs to a backup thread to periodically check if need to 
// take a snapshot now and tell raft
//
func (kv *KVServer) isNeedToSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	maxRaftPersistSize := kv.maxraftstate * 9 / 10
	raftSize := kv.rf.ReadPersistSnapshotSize()
	if raftSize > maxRaftPersistSize {
		return true
	}
	return false
}


func (kv *KVServer) callRaftStartSnapshot() {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.serverMap)
    e.Encode(kv.lastApplyIdMap)
    e.Encode(kv.lastLogIndex)
    snapshotInBytes := w.Bytes()

	kv.rf.StartSnapshot(snapshotInBytes)
	DPrintf("server.go: Wow!!! Server(%d) take a snapshot(snapshot.LastLogIndex=%d) and notify its raft", kv.me, kv.lastLogIndex)
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
	kv.clerkChannelMap = make(map[string]chan Op)

	// read persister‘s current snapshot
	snapshotInBytes := kv.rf.ReadPersistSnapshot()
	if len(snapshotInBytes) > 0 {
		DPrintf("server.go: Server(%d) read snapshot from its persister", kv.me)
		r := bytes.NewBuffer(snapshotInBytes)
    	d := labgob.NewDecoder(r)
    	var snapshot raft.Snapshot
    	if d.Decode(&snapshot.ServerMap) != nil || d.Decode(&snapshot.LastApplyIdMap) != nil {
        	DPrintf("read snapshot error")
        	return nil
    	}
    	if d.Decode(&snapshot.LastLogIndex) != nil {
    		DPrintf("read snapshot error")
        	return nil
    	}

    	kv.lastApplyIdMap = snapshot.LastApplyIdMap
    	kv.serverMap = snapshot.ServerMap
    	kv.lastLogIndex = snapshot.LastLogIndex
	}else{
		kv.lastApplyIdMap = make(map[int64]int64)
		kv.serverMap = make(map[string]string)
		kv.lastLogIndex = 0
	}

	go kv.DaemonThread()

	return kv
}