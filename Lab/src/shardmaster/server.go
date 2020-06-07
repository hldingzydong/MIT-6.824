package shardmaster

import (
	"../raft"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"
import "strconv"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	lastApplyIdMap	map[int64]int64   // store <clerkId,lastApplyRequestUuid>
	clerkChannelMap map[string]chan Op // store <clerkId+uuid, clerkNotifyChannel>
	dead    		int32 // set by Kill()
}

type Op struct {
	ClerkId 	int64
	Uuid   		int64
	OpType      Type
	Servers 	map[int][]string  // when OpType == Join
	GIDs 		[]int			  // when OpType == Leave
	Shard 		int				  // when OpType == Move
	GID   		int   			  // when OpType == Move
	Num   		int 			  // when OpType == Query
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	var op Op
	op.ClerkId = args.ClerkId
	op.Uuid = args.Uuid
	op.OpType = Join
	op.Servers = args.Servers

	sm.mu.Lock()
	_, _, isLeader := sm.Raft().Start(op)
	sm.mu.Unlock()

	if !isLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
	}else{
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		sm.mu.Lock()
		sm.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := sm.clerkChannelMap[requestUuid]
		sm.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)

		select {
			case <-timeoutTimer.C:
			sm.mu.Lock()
			if sm.isDuplicate(args.ClerkId, args.Uuid) {
				reply.Err = OK
				reply.WrongLeader = false
			}else{
				reply.Err = WrongLeader
				reply.WrongLeader = true
			}

			delete(sm.clerkChannelMap, requestUuid)
			sm.mu.Unlock()
			return

			case <-tmpChan:
			sm.mu.Lock()
			defer sm.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK
			delete(sm.clerkChannelMap, requestUuid)
			return
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	var op Op
	op.ClerkId = args.ClerkId
	op.Uuid = args.Uuid
	op.OpType = Leave
	op.GIDs = args.GIDs

	sm.mu.Lock()
	_, _, isLeader := sm.Raft().Start(op)
	sm.mu.Unlock()

	if !isLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
	}else{
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		sm.mu.Lock()
		sm.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := sm.clerkChannelMap[requestUuid]
		sm.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)

		select {
			case <-timeoutTimer.C:
			sm.mu.Lock()
			if sm.isDuplicate(args.ClerkId, args.Uuid) {
				reply.Err = OK
				reply.WrongLeader = false
			}else{
				reply.Err = WrongLeader
				reply.WrongLeader = true
			}

			delete(sm.clerkChannelMap, requestUuid)
			sm.mu.Unlock()
			return

			case <-tmpChan:
			sm.mu.Lock()
			defer sm.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK
			delete(sm.clerkChannelMap, requestUuid)
			return
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	var op Op
	op.ClerkId = args.ClerkId
	op.Uuid = args.Uuid
	op.OpType = Move
	op.Shard = args.Shard
	op.GID = args.GID

	sm.mu.Lock()
	_, _, isLeader := sm.Raft().Start(op)
	sm.mu.Unlock()

	if !isLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
	}else{
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		sm.mu.Lock()
		sm.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := sm.clerkChannelMap[requestUuid]
		sm.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)

		select {
		case <-timeoutTimer.C:
			sm.mu.Lock()
			if sm.isDuplicate(args.ClerkId, args.Uuid) {
				reply.Err = OK
				reply.WrongLeader = false
			}else{
				reply.Err = WrongLeader
				reply.WrongLeader = true
			}

			delete(sm.clerkChannelMap, requestUuid)
			sm.mu.Unlock()
			return

		case <-tmpChan:
			sm.mu.Lock()
			defer sm.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK
			delete(sm.clerkChannelMap, requestUuid)
			return
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	var op Op
	op.ClerkId = args.ClerkId
	op.Uuid = args.Uuid
	op.OpType = Query
	op.Num = args.Num

	sm.mu.Lock()
	_, _, isLeader := sm.Raft().Start(op)
	sm.mu.Unlock()

	if !isLeader {
		reply.Err = WrongLeader
		reply.WrongLeader = true
	}else{
		requestUuid := strconv.FormatInt(args.ClerkId,10) + strconv.FormatInt(args.Uuid,10)

		sm.mu.Lock()
		sm.clerkChannelMap[requestUuid] = make(chan Op, 1)
		tmpChan := sm.clerkChannelMap[requestUuid]
		sm.mu.Unlock()

		timeoutTimer := time.NewTimer(time.Duration(500) * time.Millisecond)

		select {
		case <-timeoutTimer.C:
			sm.mu.Lock()
			if sm.isDuplicate(args.ClerkId, args.Uuid) {
				reply.Err = OK
				reply.WrongLeader = false
			}else{
				reply.Err = WrongLeader
				reply.WrongLeader = true
			}

			delete(sm.clerkChannelMap, requestUuid)
			sm.mu.Unlock()
			return

		case <-tmpChan:
			sm.mu.Lock()
			defer sm.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK

			if args.Num < 0 {
				reply.Config = sm.configs[len(sm.configs)-1]
			}else{
				reply.Config = sm.configs[args.Num]
			}

			delete(sm.clerkChannelMap, requestUuid)
			return
		}
	}
}

//
// back thread to receive applyMsg from shardmaster's raft
//
func (sm *ShardMaster) DaemonThread() {
	for !sm.killed() {
		applyMsg := <- sm.applyCh
		if !applyMsg.CommandValid {
			continue
		}else {
			getOp := applyMsg.Command.(Op)

			clerkId := getOp.ClerkId
			uuid := getOp.Uuid
			requestUuid := strconv.FormatInt(clerkId,10) + strconv.FormatInt(uuid,10)

			sm.mu.Lock()
			if !sm.isDuplicate(clerkId, uuid) {
				// according to OpType, reconfig
				if getOp.OpType != Query {
					var config Config
					config.Num = len(sm.configs)

					switch getOp.OpType {
						case Join:

						case Leave:

						case Move:

					}
					sm.configs = append(sm.configs, config)
				}
			}else {
				sm.mu.Unlock()
				continue
			}

			clerkChan, ok := sm.clerkChannelMap[requestUuid]
			if ok {
				clerkChan <- getOp
			}
			sm.mu.Unlock()
		}
	}
}


//
// helper function
// check is request is duplicate
//
func (sm *ShardMaster) isDuplicate(clerkId int64, uuid int64) bool {
	lastRequestUuid, ok := sm.lastApplyIdMap[clerkId]
	if !ok || lastRequestUuid < uuid {
		return false
	}
	return true
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.lastApplyIdMap = make(map[int64]int64)
	sm.clerkChannelMap = make(map[string]chan Op)

	go sm.DaemonThread()

	return sm
}
