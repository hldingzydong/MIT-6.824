package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNewConfig   = "ErrNewConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Uuid  int64
	ClerkId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Uuid  int64
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}

//
// added by develoepr
//
type PullKVArgs struct {
	Shard  			int
	ConfigNum 		int // 允许从newer config的server上pull, 不允许从older config的server上pull，因为如果从older的server上pull
						// 可能pull到的data是 stale data， older的server仍在接收client的request
	ServerId      int64
	Uuid 		  int64
}

type PullKVReply struct {
	Err 	   		Err
	KV  	   		map[string]string
	LastApplyIdMap  map[int64]int64  // 防止 duplicate request(like Put/Append)
}
