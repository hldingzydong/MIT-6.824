package shardkv

import (
	"../shardmaster"
	"bytes"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"



type Op struct {
	OpType 		string
	OpClerkId   int64
	OpUuid   	int64
	Key			string
	Value		string
	Shard       int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int   					// snapshot if log grows this big
	// added by developer
	serverId     int64
	dead    	 int32 					// set by Kill()

	sm       	 *shardmaster.Clerk
	config   	 shardmaster.Config

	lastApplyIdMap	map[int64]int64    // store <clerkId,lastApplyRequestUuid>
	clerkChannelMap map[string]chan Op // store <clerkId+uuid, clerkNotifyChannel>
	serverMap		map[string]string  // store <key,value>

	lastLogIndex    int                // global view

	pullDataStatus  []bool

	uuidCount		int64
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// RPC handler
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// judge if return ErrWrongGroup
	shard := key2shard(args.Key)
	for kv.pullDataStatus[shard] {
	}
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()
	if kv.gid != gid {
		reply.Err = ErrWrongGroup
		return
	}

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
	}else {
		// 由clerkId + uuid 可以确认全剧唯一的请求，依此对该请求生成一个channel
		requestUuid := strconv.FormatInt(args.ClerkId, 10) + strconv.FormatInt(args.Uuid, 10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)
		select {
		case <-timeoutTimer.C:
			kv.mu.Lock()
			if kv.isDuplicate(args.ClerkId, args.Uuid) {
				reply.Err = OK
				reply.Value = kv.serverMap[op.Key]
			} else {
				reply.Err = ErrWrongLeader
			}

			delete(kv.clerkChannelMap, requestUuid)
			kv.mu.Unlock()
			return

		case <-tmpChan:
			kv.mu.Lock()
			defer kv.mu.Unlock()

			lastValue, ok := kv.serverMap[op.Key]
			if ok {
				reply.Err = OK
				reply.Value = lastValue
			} else {
				reply.Err = ErrNoKey
			}
			delete(kv.clerkChannelMap, requestUuid)
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// judge if return ErrWrongGroup
	shard := key2shard(args.Key)
	for kv.pullDataStatus[shard] {
	}
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()
	if kv.gid != gid {
		reply.Err = ErrWrongGroup
		return
	}

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
	}else {
		// 该Server是leader
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)
		select {
		case <-timeoutTimer.C:
			kv.mu.Lock()
			if kv.isDuplicate(op.OpClerkId, op.OpUuid) {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			delete(kv.clerkChannelMap, requestUuid)
			kv.mu.Unlock()
			return

		case <-tmpChan:
			kv.mu.Lock()
			defer kv.mu.Unlock()

			reply.Err = OK
			delete(kv.clerkChannelMap, requestUuid)
			return
		}
	}
}

func (kv *ShardKV) PullShardKV(args *PullKVArgs, reply *PullKVReply) {
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNewConfig
		return
	}
	var op Op
	op.OpType = "PullData"
	op.OpClerkId = args.ServerId
	op.OpUuid = args.Uuid
	op.Shard = args.Shard

	kv.mu.Lock()
	_, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}else{
		requestUuid := strconv.FormatInt(args.ServerId,10) + strconv.FormatInt(args.Uuid,10)

		kv.mu.Lock()
		kv.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := kv.clerkChannelMap[requestUuid]
		kv.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)

		select {
			case <-timeoutTimer.C:
				kv.mu.Lock()
				if kv.isDuplicate(op.OpClerkId, op.OpUuid) {
					reply.Err = OK
				} else {
					reply.Err = ErrWrongLeader
				}
				delete(kv.clerkChannelMap, requestUuid)
				kv.mu.Unlock()
				return

			case <-tmpChan:
				kv.mu.Lock()
				defer kv.mu.Unlock()

				reply.KV = kv.GenerateShardKV(op.Shard)
				reply.Err = OK
				delete(kv.clerkChannelMap, requestUuid)
				return
		}
	}
}

//
// if the request is ever applied
//
func (kv *ShardKV) isDuplicate(clerkId int64, uuid int64) bool {
	lastRequestUuid, ok := kv.lastApplyIdMap[clerkId]
	if !ok || lastRequestUuid < uuid {
		return false
	}
	return true
}

//
// periodically pull configuration from shardmaster
// at least rough every 100 ms
// call masters[i].Query(#currentConfig+1) to get
// the new configuration if it exists
//
func (kv *ShardKV) PullConfigurationFromShardMaster()  {
	for !kv.killed() {
		time.Sleep(70 * time.Millisecond)

		newestConfig := kv.sm.Query(-1)
		kv.mu.Lock()
		if newestConfig.Num != kv.config.Num {
			nextConfig := kv.sm.Query(kv.config.Num + 1)
			for {
				pullTargetServers := kv.GenerateJoinShardsAndServers(nextConfig)
				if len(pullTargetServers) > 0 {
					for shard, targetServers := range pullTargetServers {
						kv.pullDataStatus[shard] = true
						success := false
						for !success {
							for _, targetServer := range targetServers {
								args := PullKVArgs{
									Shard:     shard,
									ConfigNum: kv.config.Num,
									ServerId: kv.serverId,
									Uuid: kv.uuidCount,
								}
								kv.uuidCount++

								reply := PullKVReply{}
								ok := kv.make_end(targetServer).Call("ShardKV.PullShardKV", &args, &reply)
								if ok {
									if reply.Err == OK {
										kv.handlePullData(&reply)
										success = true
										break
									} else if reply.Err == ErrWrongLeader || reply.Err == ErrNewConfig {
										continue
									}
								}
							}
						}
						kv.pullDataStatus[shard] = false
					}
				}

				// update kv.config
				kv.config = nextConfig
				if newestConfig.Num == kv.config.Num {
					break
				}
				nextConfig = kv.sm.Query(kv.config.Num+1)
			}
		}
		kv.mu.Unlock()
	}
}

//
// @return shard -> server names
//
func (kv *ShardKV) GenerateJoinShardsAndServers(latestConfig shardmaster.Config) map[int][]string {
	pullTargetServers := make(map[int][]string)
	for shard, latestGid := range latestConfig.Shards {
		if latestGid == kv.gid && kv.config.Shards[shard] != kv.gid {
			pullTargetGid:= kv.config.Shards[shard]
			pullTargetServer, ok := kv.config.Groups[pullTargetGid]
			if ok {
				pullTargetServers[shard] = pullTargetServer
			}
		}
	}
	return pullTargetServers
}

//
// seem to be silly for not one pass
//
func (kv *ShardKV) GenerateShardKV(shard int) map[string]string {
	leaveKVs := make(map[string]string)
	for key, value := range kv.serverMap {
		currShard := key2shard(key)
		if currShard == shard {
			// simplify server implementation
			leaveKVs[key] = value
		}
	}
	return leaveKVs
}

func (kv *ShardKV) handlePullData(reply *PullKVReply)  {
	if len(reply.KV) > 0 {
		for nKey, nValue := range reply.KV {
			kv.serverMap[nKey] = nValue
		}
	}

	if len(reply.LastApplyIdMap) > 0 {
		for clerkId, lastRequestId := range reply.LastApplyIdMap {
			kv.lastApplyIdMap[clerkId] = lastRequestId
		}
	}
}

//
// detect snapshot
//
func (kv *ShardKV) isNeedToSnapshot() bool {
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

//
// do snapshot
//
func (kv *ShardKV) callRaftStartSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.serverMap)
	e.Encode(kv.lastApplyIdMap)
	e.Encode(kv.lastLogIndex)
	snapshotInBytes := w.Bytes()

	kv.rf.StartSnapshot(snapshotInBytes)
}

//
// backup thread for a kv server
// read apply message from applyCh and update its <k,v>
// and notify RPC handler reply to clerks
//
func (kv *ShardKV) DaemonThread() {
	for !kv.killed() {
		applyMsg := <- kv.applyCh
		if !applyMsg.CommandValid {
			// this is a snapshot come from Leader
			snapshotInBytes := applyMsg.CommandSnapshot
			kv.mu.Lock()
			r := bytes.NewBuffer(snapshotInBytes)
			d := labgob.NewDecoder(r)
			if d.Decode(&kv.serverMap) != nil || d.Decode(&kv.lastApplyIdMap) != nil {
				// decode meet error, so end up
				kv.mu.Unlock()
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
			} else{
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clerkChannelMap = make(map[string]chan Op)

	kv.serverId = nrand()
	kv.uuidCount = 0

	kv.config = kv.sm.Query(-1)
	kv.pullDataStatus = make([]bool, len(kv.config.Shards))
	for i:=0;i<len(kv.pullDataStatus);i++ {
		kv.pullDataStatus[i] = false
	}

	// support snapshot
	snapshotInBytes := kv.rf.ReadPersistSnapshot()
	if len(snapshotInBytes) > 0 {
		r := bytes.NewBuffer(snapshotInBytes)
		d := labgob.NewDecoder(r)
		var snapshot raft.Snapshot
		if d.Decode(&snapshot.ServerMap) != nil || d.Decode(&snapshot.LastApplyIdMap) != nil {
			return nil
		}
		if d.Decode(&snapshot.LastLogIndex) != nil {
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
	go kv.PullConfigurationFromShardMaster()

	return kv
}
