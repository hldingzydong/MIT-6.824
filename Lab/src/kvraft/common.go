package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"        // Put()/Append() won't produce this error message
	ErrWrongLeader = "ErrWrongLeader"  // when timeout also return this message
)

//
// error message explain
// For Get(), may "ErrNoKey"(and return a empty string) "ErrWrongLeader"
// For Put(), may "ErrWrongLeader", if no key then put, else replace its value
// For Append(), may "ErrWrongLeader", if no key then act like Put()
//

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
	Uuid int64
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}
