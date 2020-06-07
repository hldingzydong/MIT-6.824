package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.uuidCount = 1
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}

	ck.mu.Lock()
	args.ClerkId = ck.clerkId
	args.Uuid = ck.uuidCount
	ck.uuidCount++
	args.Num = num
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}

	ck.mu.Lock()
	args.Servers = servers
	args.ClerkId = ck.clerkId
	args.Uuid = ck.uuidCount
	ck.uuidCount++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}

	ck.mu.Lock()
	args.GIDs = gids
	args.ClerkId = ck.clerkId
	args.Uuid = ck.uuidCount
	ck.uuidCount++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}

	ck.mu.Lock()
	args.Shard = shard
	args.GID = gid
	args.ClerkId = ck.clerkId
	args.Uuid = ck.uuidCount
	ck.uuidCount++
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
