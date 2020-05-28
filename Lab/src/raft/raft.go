package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"
import "bytes"
import "../labgob"
// import "fmt"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

/*
 *  Raft node's state
 */
type State string

const(
    Leader State = "Leader"
    Candidate = "Candidate"
    Follower = "Follower"
)

/***************************************************************************
                        Data Structure Definition
***************************************************************************/
//
// snapshot definition for communication between kv server 
// and its Raft
//
type Snapshot struct {
    ServerMap       map[string]string // store <key,value>
    LastApplyIdMap  map[int64]int64   // store <clerkId,lastApplyRequestUuid>
    LastLogIndex        int               // the last applied log's index, view from kv server 
}

/*
 * ApplyMessage sent to applyCh
 */
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
    CommandSnapshot   Snapshot          // when a follower receive a snapshot RPC, it should fill the CommandSnapshot and send it to its kvserver
}

/*
 * Log Entry
 */
type Entry struct {
    Command      interface{}
    Term         int              // term when create this entry
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()

    // state
    state       State

    // persistent field
    currentTerm int               // currentTerm
    votedFor    int               // candidateId that received vote in current term (or null if none)
    leaderId    int               // leader's Id
    log         []Entry           // log entries

    // volatile filed
    commitIndex int               // global view
    lastApplied int               // global view

    // election time
    electionTime int              // election time

    // leader's field
    nextIndex   []int             // global view
    matchIndex  []int             // global view

    // timer
    electionTimer  *time.Timer

    // apply channel 
    applyCh         chan ApplyMsg

    // for snapshot
    snapshottedIndex       int    // global view
}


//
// init raft node with log capacity
//
func (rf *Raft) initRaftNodeToFollower(logCapacity int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.state = "Follower"

    rf.currentTerm = 0
    rf.votedFor = -1
    rf.log = make([]Entry, 1, logCapacity)
    rf.log[0].Term = 0

    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.electionTime = generateElectionTime()
    rf.electionTimer = time.NewTimer(time.Duration(rf.electionTime) * time.Millisecond)

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i:=0; i<len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
        rf.matchIndex[i] = 0
    }

    rf.snapshottedIndex = 0
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
    var term int
    var isleader bool
    
    rf.mu.Lock()
    term = rf.currentTerm
    isleader = (rf.state == "Leader")
    rf.mu.Unlock()

    return term, isleader
}



/***************************************************************************
                            Persiste(including snapshot)
***************************************************************************/
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    e.Encode(rf.snapshottedIndex)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor    int
    var log         []Entry
    var snapshottedIndex int

    if d.Decode(&currentTerm) != nil {
        DErrPrintf("read currentTerm error")
        return
    }
    if d.Decode(&votedFor) != nil {
        DErrPrintf("read votedFor error")
        return
    }
    if d.Decode(&log) != nil {
        DErrPrintf("read log entries error")
        return
    }

    if d.Decode(&snapshottedIndex) != nil {
        DErrPrintf("read snapshottedIndex error")
        return
    }

    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.log = log
    rf.snapshottedIndex = snapshottedIndex
    rf.commitIndex = snapshottedIndex
    rf.lastApplied = snapshottedIndex
    DLCPrintf("Read Server(%d) state(currentTerm=%d, votedFor=%d, logLength=%d) from persister done", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}


func (rf *Raft) persistWithSnapshot(snapshot Snapshot) {
    w1 := new(bytes.Buffer)
    e1 := labgob.NewEncoder(w1)
    e1.Encode(rf.currentTerm)
    e1.Encode(rf.votedFor)
    e1.Encode(rf.log)
    e1.Encode(rf.snapshottedIndex)
    stateInBytes := w1.Bytes()

    w2 := new(bytes.Buffer)
    e2 := labgob.NewEncoder(w2)
    e2.Encode(snapshot)
    snapshotInBytes := w2.Bytes()

    rf.persister.SaveStateAndSnapshot(stateInBytes, snapshotInBytes)
}


//
// start take a snapshot and persist its state
//
func (rf *Raft) StartSnapshot(snapshot Snapshot) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    lastLogIndexInLogView := rf.convertToRaftLogViewIndex(snapshot.LastLogIndex)
    if lastLogIndexInLogView > 0 && lastLogIndexInLogView < len(rf.log) {
        lastIncludedTerm := rf.log[lastLogIndexInLogView].Term
        rf.cutoffLogBeforeIndex(lastLogIndexInLogView, lastIncludedTerm)
        rf.snapshottedIndex = snapshot.LastLogIndex

        rf.persistWithSnapshot(snapshot)

        if rf.state == "Leader" {
            go rf.sendInstallSnapshotToMultipleFollowers(snapshot.LastLogIndex, lastIncludedTerm)
        }
    }
    return
}


/***************************************************************************
                            Request Vote RPC
***************************************************************************/
type RequestVoteArgs struct {
    CandidateTerm           int   
    CandidateId             int
    LastLogIndex            int
    LastLogTerm             int
}

type RequestVoteReply struct {
    FollowerTerm            int
    VotedGranted            bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()

    DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] is Requested Vote from Server (%d)[CandidateTerm=%d]",
                rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.CandidateTerm)
        
    // 遇到一个比当前term小的Request Vote RPC -> refuse
    // term相同但是我在该term期间已经投过票了 -> refuse 
    if rf.currentTerm > args.CandidateTerm {
        reply.VotedGranted = false
        reply.FollowerTerm = rf.currentTerm
        rf.mu.Unlock()
        return
    } else if rf.currentTerm == args.CandidateTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
        reply.VotedGranted = false
        reply.FollowerTerm = rf.currentTerm
        rf.mu.Unlock()
        return
    }

    // 遇到一个没有自己log up-to-date的 Request Vote RPC -> refuse
    // 否则, 就grant vote
    // 但是不结束, 因为有可能遇到比自己term更高的RPC, 需要更新自己当前状态
    reply.FollowerTerm = args.CandidateTerm

    lastLogIndex := rf.convertToGlobalViewIndex(len(rf.log) - 1)
    lastLogTerm := rf.log[len(rf.log)-1].Term
    if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
        reply.VotedGranted = false
    }else{
        reply.VotedGranted = true
        rf.votedFor = args.CandidateId
    }
    
    // 当遇到一个higher的term时,不管现在状态是什么,都要先setCurrentTerm,再convert to follower
    // 否则,即term相同,根据是否grant vote决定是否reset election time
    if rf.currentTerm < args.CandidateTerm {
        rf.currentTerm = args.CandidateTerm
        if reply.VotedGranted || rf.state != "Follower"{
            rf.persist()
            rf.mu.Unlock()
            rf.convertToFollower()
            return
        }
    }else if reply.VotedGranted {
        rf.persist()
        rf.mu.Unlock()
        rf.convertToFollower()
        return
    }
    rf.persist()
    rf.mu.Unlock()
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft) requestForVotes() {
    count := 0
    finished := 1
    meetHigherTerm := -1

    var tmpMutex sync.Mutex
    cond := sync.NewCond(&tmpMutex)

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }else{
            go func(follower int) {
                var args RequestVoteArgs
                var reply RequestVoteReply

                rf.mu.Lock()
                args.CandidateTerm = rf.currentTerm
                args.CandidateId = rf.me
                args.LastLogIndex = rf.convertToGlobalViewIndex(len(rf.log)-1)
                args.LastLogTerm = rf.log[len(rf.log)-1].Term
                rf.mu.Unlock()

                vote := rf.sendRequestVote(follower, &args, &reply)
                
                tmpMutex.Lock()
                defer tmpMutex.Unlock()

                rf.mu.Lock()
                if vote && rf.currentTerm == args.CandidateTerm {
                    if reply.VotedGranted == true {
                        DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] receive Vote from Server (%d)[term=%d]", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower, reply.FollowerTerm)
                        count++;
                    } else if reply.FollowerTerm > args.CandidateTerm {
                        DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] meet higher term from Server (%d)[term=%d]", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower, reply.FollowerTerm)
                        meetHigherTerm = reply.FollowerTerm
                    } else{
                        DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] is refused by Server (%d)[term=%d]", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower, reply.FollowerTerm)
                    }
                }
                rf.mu.Unlock()

                finished++
                // 告知主线程 Server[i]已reply
                cond.Broadcast()
            }(i)
        }
    }

    tmpMutex.Lock()
    for count < len(rf.peers)/2 && finished != len(rf.peers) && meetHigherTerm == -1 {
        cond.Wait()
    }
    
    rf.mu.Lock()
    if rf.state == "Candidate" {
        if meetHigherTerm != -1 {
            rf.currentTerm = meetHigherTerm
        }
    }
    
    if rf.state == "Candidate" {
        if count >= len(rf.peers)/2 {
            DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote and become leader", rf.me, rf.state, rf.currentTerm, rf.votedFor)
            rf.persist()
            rf.mu.Unlock()
            tmpMutex.Unlock()
            rf.convertToLeader()
            return
        }else{ //如果遇到 higher term 或者 大多数的reply是false
            DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote and back to follower", rf.me, rf.state, rf.currentTerm, rf.votedFor)
            rf.persist()
            rf.mu.Unlock()
            tmpMutex.Unlock()
            rf.convertToFollower()
            return
        }
    }
    rf.persist()
    rf.mu.Unlock()
    tmpMutex.Unlock()
}

/***************************************************************************
                            Append Entries RPC
***************************************************************************/
type AppendEntriesArgs struct {
    LeaderTerm              int
    LeaderId                int
    PrevLogIndex            int
    PrevLogTerm             int
    LeaderCommitIndex       int
    Entries                 []Entry
}

type AppendEntriesReply struct {
    FollowerTerm            int
    ConflictTerm            int
    ConflictIndex           int
    IsSuccess               bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()

    if len(args.Entries) != 0 {
        DAEPrintf("Server (%d)[term=%d] received Append Entries request[LeaderTerm=%d, PrevLogIndex=%d] from Server (%d)", rf.me, rf.currentTerm, args.LeaderTerm, args.PrevLogIndex, args.LeaderId)
    }else{
        DAEPrintf("Server (%d) received Heart Beat request from Server (%d)", rf.me, args.LeaderId)
    }

    if rf.currentTerm > args.LeaderTerm {
        reply.IsSuccess = false
        reply.FollowerTerm = rf.currentTerm
        rf.mu.Unlock()
        return
    }

    argsPrevLogIndexInLogView := rf.convertToRaftLogViewIndex(args.PrevLogIndex)

    if argsPrevLogIndexInLogView < 0 {
        reply.IsSuccess = true

        // sync log if needed
        if args.PrevLogIndex + len(args.Entries) > rf.snapshottedIndex {
            // if snapshottedIndex == prevLogIndex, all log entries should be added.
            startIdx := rf.snapshottedIndex - args.PrevLogIndex
            // only keep the last snapshotted one
            rf.log = rf.log[:1]
            rf.log = append(rf.log, args.Entries[startIdx:]...)
        }
        rf.mu.Unlock()
        return
    }

    // 如果PrevLogIndex在log中不存在
    if len(rf.log) <= argsPrevLogIndexInLogView{
        reply.IsSuccess = false
        reply.FollowerTerm = args.LeaderTerm
        reply.ConflictTerm = -1
        reply.ConflictIndex = rf.convertToGlobalViewIndex(len(rf.log))
    }else { 
        // 如果存在, 则比较PrevLogTerm
        lastTerm := rf.log[argsPrevLogIndexInLogView].Term
        if lastTerm != args.PrevLogTerm {
            reply.IsSuccess = false
            reply.FollowerTerm = args.LeaderTerm
            reply.ConflictTerm = lastTerm

            // 找到该Term中的第一个index
            i := argsPrevLogIndexInLogView
            for i>=1 && rf.log[i].Term == lastTerm{
                i-- 
            }
            reply.ConflictIndex = rf.convertToGlobalViewIndex(i+1)
        }else{
            // 可以进行Append Entries
            reply.IsSuccess = true
            reply.FollowerTerm = args.LeaderTerm
            rf.leaderId = args.LeaderId

            if len(args.Entries) > 0 {
                // 先找到 conflict entry 的index
                unmatch_idx := -1
                for idx := range args.Entries {
                    if len(rf.log) < (argsPrevLogIndexInLogView+2+idx) || rf.log[(argsPrevLogIndexInLogView+1+idx)].Term != args.Entries[idx].Term {
                        // unmatch log found
                        unmatch_idx = idx
                        break
                    }
                }

                if unmatch_idx != -1 {
                    // there are unmatch entries
                    // truncate unmatch Follower entries, and apply Leader entries
                    rf.log = rf.log[:(argsPrevLogIndexInLogView + 1 + unmatch_idx)]
                    rf.log = append(rf.log, args.Entries[unmatch_idx:]...)
                    DAEPrintf("Server (%d) append entries and now log is from %v to %v(index=%d)", rf.me, rf.log[1], rf.log[len(rf.log)-1], len(rf.log)-1)
                }
            }

            // 此时应当还check lastApplied和args.CommitIndex
            rfCommitIndexInLogView := rf.convertToRaftLogViewIndex(rf.commitIndex)
            argsLeaderCommitIndexInLogView := rf.convertToRaftLogViewIndex(args.LeaderCommitIndex)
            if argsLeaderCommitIndexInLogView > rfCommitIndexInLogView {

                i := rfCommitIndexInLogView+1

                var newCommitIndex int
                if argsLeaderCommitIndexInLogView < len(rf.log)-1 {
                    newCommitIndex = argsLeaderCommitIndexInLogView
                }else{
                    newCommitIndex = len(rf.log)-1
                }
        
                if rf.convertToRaftLogViewIndex(rf.lastApplied) < newCommitIndex {
                    applyMsgArray := make([]ApplyMsg, newCommitIndex-i+1)

                    for ; i <= newCommitIndex; i++ {
                        applyMsg := ApplyMsg{}
                        applyMsg.CommandValid = true
                        applyMsg.Command = rf.log[i].Command
                        applyMsg.CommandIndex = rf.convertToGlobalViewIndex(i)

                        applyMsgArray[i-rfCommitIndexInLogView-1] = applyMsg
                    }

                    DAEPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] apply message from index=%d to index=%d to applyCh", 
                        rf.me, rf.state, rf.currentTerm, rf.votedFor, rfCommitIndexInLogView, newCommitIndex)
                    rf.commitIndex = rf.convertToGlobalViewIndex(newCommitIndex)
                    rf.lastApplied = rf.commitIndex
            
                    go func(){
                        for _, applyMsg := range applyMsgArray {
                            rf.applyCh <- applyMsg
                        }
                    }()
                }
            }
        }
    } 

    rf.currentTerm = args.LeaderTerm
    rf.persist()
    rf.mu.Unlock()
    rf.convertToFollower()
    return
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) sendAppendEntriesToOneFollower(followerId int) {
    var args AppendEntriesArgs
    var reply AppendEntriesReply

    rf.mu.Lock()
    args.LeaderId = rf.me
    args.LeaderTerm = rf.currentTerm
    args.PrevLogIndex = rf.nextIndex[followerId]-1
    if rf.convertToRaftLogViewIndex(args.PrevLogIndex) >= 0 && rf.convertToRaftLogViewIndex(args.PrevLogIndex) < len(rf.log) {
        args.PrevLogTerm = rf.log[rf.convertToRaftLogViewIndex(args.PrevLogIndex)].Term
    }
    args.LeaderCommitIndex = rf.commitIndex
    if rf.convertToRaftLogViewIndex(rf.nextIndex[followerId]) > 0 && rf.convertToRaftLogViewIndex(rf.nextIndex[followerId]) < len(rf.log) {
        args.Entries = rf.log[rf.convertToRaftLogViewIndex(rf.nextIndex[followerId]):]
    }
    rf.mu.Unlock()  

    ok := rf.sendAppendEntries(followerId, &args, &reply)

    rf.mu.Lock()
    if ok && rf.currentTerm == args.LeaderTerm {
        if !reply.IsSuccess {
            // 如果Leader发现了更高的term,就convert to follower
            if reply.FollowerTerm > args.LeaderTerm && rf.state == "Leader" {
                DLCPrintf("Server (%d) is meet higher term and stop sending Heart Beat", rf.me)
                rf.state = "Follower"
                rf.currentTerm = reply.FollowerTerm
                rf.persist()
                rf.mu.Unlock()
                rf.convertToFollower()
                return
            }else if rf.nextIndex[followerId] == args.PrevLogIndex + 1 {
                // find conflict term
                conflictIndex := 1
                for conflictIndex < len(rf.log) && rf.log[conflictIndex].Term < reply.ConflictTerm {
                    conflictIndex++
                }
                if conflictIndex != len(rf.log) && rf.log[conflictIndex].Term == reply.ConflictTerm {
                    DAEPrintf("624: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], rf.convertToGlobalViewIndex(conflictIndex + 1))
                    rf.nextIndex[followerId] = rf.convertToGlobalViewIndex(conflictIndex + 1)
                }else{
                    DAEPrintf("628: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], reply.ConflictIndex)
                    rf.nextIndex[followerId] = reply.ConflictIndex
                }
            }
        }else if rf.nextIndex[followerId] == args.PrevLogIndex + 1 {
            DAEPrintf("632: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], args.PrevLogIndex + len(args.Entries) + 1)
            rf.nextIndex[followerId] = args.PrevLogIndex + len(args.Entries) + 1
            rf.matchIndex[followerId] = args.PrevLogIndex + len(args.Entries)

            //rf.commitEntries()
        }
    }
    rf.mu.Unlock()
}

//
// As a leader, need to send Append Entries RPC periodically to followers
// when Entry is empty, it is a heartbeat
//
func (rf *Raft) sendAppendEntriesToMultipleFollowers() {
    for !rf.killed() && rf.state == "Leader" {
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                continue
            }else{
                if rf.nextIndex[i] <= rf.snapshottedIndex {
                    go rf.sendInstallSnapshotToOneFollower(i, rf.log[0].Term)
                }else{
                    go rf.sendAppendEntriesToOneFollower(i)
                }
            }
        }

        rf.mu.Lock()
        if rf.state != "Leader" {
            DLCPrintf("Server (%d) is no longer Leader and stop sending Heart Beat", rf.me)
            rf.persist()
            rf.mu.Unlock()
            return
        }
        rf.commitEntries()
        rf.mu.Unlock()

        time.Sleep(100 * time.Millisecond)
    }
}


func (rf *Raft) commitEntries() {
    var count int
    var applyMsgArray []ApplyMsg
    ableCommit := false

    commitIndexInRaftView := rf.convertToRaftLogViewIndex(rf.commitIndex)

    i := commitIndexInRaftView + 1
    for ; i < len(rf.log); i++ {
        if rf.log[i].Term < rf.currentTerm {
            continue
        }else if rf.log[i].Term > rf.currentTerm {
            break
        }else{
            count = 0
            for j := 0; j < len(rf.peers); j++ {
                if j == rf.me {
                    continue
                }else{
                    if rf.convertToRaftLogViewIndex(rf.matchIndex[j]) >= i {
                        count++
                    }
                }
            }
            if count < len(rf.peers)/2 {
                break
            }else{
                ableCommit = true
            }
        }
    }

    if ableCommit {
        // commit index=rf.commitIndex + 1 -> i
        applyMsgArray = make([]ApplyMsg, i-commitIndexInRaftView-1)
        for k := commitIndexInRaftView+1; k < i; k++ {
            applyMsg := ApplyMsg{}
            applyMsg.CommandValid = true
            applyMsg.Command = rf.log[k].Command
            applyMsg.CommandIndex = rf.convertToGlobalViewIndex(k)

            applyMsgArray[k-commitIndexInRaftView-1] = applyMsg
        }
        DAEPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] apply message from index=%d to index=%d to applyCh", 
            rf.me, rf.state, rf.currentTerm, rf.votedFor, commitIndexInRaftView, i-1)
        
        rf.commitIndex = rf.convertToGlobalViewIndex(i-1)
        rf.lastApplied = rf.commitIndex
    }
    

    if applyMsgArray != nil && len(applyMsgArray) > 0 {
        go func(){
            for _, applyMsg := range applyMsgArray {
                //fmt.Printf("Server[%d] apply message to applyCh\n", rf.me)
                rf.applyCh <- applyMsg
            }
        }()
    }
} 

func (rf *Raft) startAppendEntries() {
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }else{
            if rf.nextIndex[i] <= rf.snapshottedIndex {
                go rf.sendInstallSnapshotToOneFollower(i, rf.log[0].Term)
            }else{
                go rf.sendAppendEntriesToOneFollower(i)
            }
        }
    }
}

/***************************************************************************
                            Install Snapshot RPC
***************************************************************************/
type InstallSnapshotArgs struct {
    LeaderTerm          int
    LeaderId            int
    LastIncludedIndex   int
    LastIncludedTerm    int
    Data                []byte   // snapshot
}

type InstallSnapshotReply struct {  
    FollowerTerm        int
}

//
// Install Snapshot RPC handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    DISPrintf("Server(%d) receive InstallSnapshot RPC from Leader(%d)[LeaderTerm=%d, LastIncludedIndex=%d, LastIncludedTerm=%d]", rf.me, args.LeaderId,
                args.LeaderTerm, args.LastIncludedIndex, args.LastIncludedTerm)
    if args.LeaderTerm < rf.currentTerm {
        reply.FollowerTerm = rf.currentTerm
        return
    }

    reply.FollowerTerm = rf.currentTerm

    raftViewIndex := rf.convertToRaftLogViewIndex(args.LastIncludedIndex)
    if raftViewIndex <= 0 {
        return
    }

    if raftViewIndex >= len(rf.log) || (raftViewIndex < len(rf.log) && args.LastIncludedTerm == rf.log[raftViewIndex].Term) {
        rf.cutoffLogBeforeIndex(raftViewIndex, args.LastIncludedTerm)
        rf.snapshottedIndex = args.LastIncludedIndex
        if rf.commitIndex < args.LastIncludedIndex {
            rf.commitIndex = args.LastIncludedIndex
            rf.lastApplied = rf.commitIndex
        }
        // generate an applyMsg and pass it to applCh
        applyMsg := ApplyMsg{}
        // indicate that it is not a command, it is a snapshot
        applyMsg.CommandValid = false
        // use labgob decode data from InstallSnapshot RPC
        r := bytes.NewBuffer(args.Data)
        d := labgob.NewDecoder(r)
        var commandSnapshot Snapshot
        if d.Decode(&commandSnapshot) != nil {
            DErrPrintf("read snapshot data error")
            return
        }

        rf.persistWithSnapshot(commandSnapshot)

        applyMsg.CommandSnapshot = commandSnapshot
        DPrintfSnapshot(commandSnapshot)

        go func() {
            DISPrintf("Follower(%d) send a snapshot to its applyCh", rf.me)
            rf.applyCh <- applyMsg
        }()
    }
}


func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

func (rf *Raft) sendInstallSnapshotToOneFollower(followerId int, lastIncludedTerm int) {
    var installSnapshotArgs InstallSnapshotArgs
    var installSnapshotReply InstallSnapshotReply

    rf.mu.Lock()
    DISPrintf("Leader(%d) start to send InstallSnapshot RPC(LeaderTerm=%d, LastIncludedIndex=%d, LastIncludedTerm=%d) to follower(%d)", rf.me, rf.currentTerm, rf.snapshottedIndex, 
        lastIncludedTerm, followerId)
    installSnapshotArgs.LeaderTerm = rf.currentTerm
    installSnapshotArgs.LeaderId = rf.me
    installSnapshotArgs.LastIncludedIndex = rf.snapshottedIndex
    installSnapshotArgs.LastIncludedTerm = lastIncludedTerm
    installSnapshotArgs.Data = rf.persister.ReadSnapshot()
    rf.mu.Unlock()

    ok := rf.sendInstallSnapshot(followerId, &installSnapshotArgs, &installSnapshotReply)
    if ok {
        if installSnapshotReply.FollowerTerm > rf.currentTerm {
            rf.currentTerm = installSnapshotReply.FollowerTerm
            rf.convertToFollower()
        }else{
            rf.mu.Lock()
            if rf.matchIndex[followerId] < installSnapshotArgs.LastIncludedIndex {
                rf.matchIndex[followerId] = installSnapshotArgs.LastIncludedIndex
            }
            DISPrintf("Leader(%d) update rf.nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], rf.matchIndex[followerId] + 1)
            rf.nextIndex[followerId] = rf.matchIndex[followerId] + 1
            rf.mu.Unlock()
        }
    }
}

func (rf *Raft) sendInstallSnapshotToMultipleFollowers(lastIncludedIndex int, lastIncludedTerm int) {
    for i:=0; i<len(rf.peers);i++ {
        if i == rf.me {
            continue
        }else{
            if rf.nextIndex[i] <= lastIncludedIndex {
                go rf.sendInstallSnapshotToOneFollower(i, lastIncludedTerm)
            }
        }
    }
}

/***************************************************************************
                            Life Cycle
***************************************************************************/
//
// raft node convert to follower and begin 倒计时
//
func (rf *Raft) convertToFollower() {
    rf.mu.Lock()
    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] convert to Follower", rf.me, rf.state, rf.currentTerm, rf.votedFor)  
    rf.state = "Follower"
    rf.electionTime = generateElectionTime()
    rf.electionTimer.Reset(time.Duration(rf.electionTime) * time.Millisecond)
    rf.mu.Unlock()
}

//
// raft node convert to candidate and begin request votes
//
func (rf *Raft) convertToCandidate() {
    rf.mu.Lock()
    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] convert to Candidate", rf.me, rf.state, rf.currentTerm, rf.votedFor) 
    rf.state = "Candidate"
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.electionTime = generateElectionTime()
    rf.electionTimer.Reset(time.Duration(rf.electionTime) * time.Millisecond)
    rf.persist()
    rf.mu.Unlock()

    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d， electionTime=%d] start request votes", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)    
    // 启动一个线程, requestVote
    go rf.requestForVotes()

}

//
// raft node convert to candidate and begin send heart beat
//
func (rf *Raft) convertToLeader() {
    rf.mu.Lock()
    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] convert to Leader", rf.me, rf.state, rf.currentTerm, rf.votedFor)
    rf.electionTimer.Stop()    
    rf.state = "Leader"
    for i:=0; i<len(rf.peers); i++ {
        rf.nextIndex[i] = rf.convertToGlobalViewIndex(len(rf.log))
        rf.matchIndex[i] = rf.convertToGlobalViewIndex(0)
    }
    rf.mu.Unlock()
    // 启动一个线程,定时给各个Follower发送HeartBeat Request 
    time.Sleep(50 * time.Millisecond)
    go rf.sendAppendEntriesToMultipleFollowers()
}


/***************************************************************************
                        Helper Function
***************************************************************************/
//
// timer 
// 
func (rf *Raft) electionTimeoutListener() {
    for !rf.killed(){
         <-rf.electionTimer.C
            rf.convertToCandidate()
    }
}


//
// cut off log ahead of given index
//
func (rf *Raft) cutoffLogBeforeIndex(lastLogIndex int, lastLogTerm int) {
    DISPrintf("Start to cut off server(%d) log. lastLogIndex=%d, lastLogfTerm=%d, before cut off its len(log)=%d", rf.me, lastLogIndex, lastLogTerm, len(rf.log))
    if lastLogIndex >= len(rf.log) {
        rf.log = make([]Entry, 1, 100)
        rf.log[0].Term = lastLogTerm
    }else if rf.convertToGlobalViewIndex(lastLogIndex) <= rf.commitIndex {
        rf.log = rf.log[lastLogIndex:]
        DISPrintf("Cut off server(%d) log success and now its len(log)=%d", rf.me, len(rf.log))
    }
}


//
// printf snapshot
// type Snapshot struct {
//    ServerMap       map[string]string // store <key,value>
//    LastApplyIdMap  map[int64]int64   // store <clerkId,lastApplyRequestUuid>
//    LastLogIndex        int               // the last applied log's index
// }
//
func DPrintfSnapshot(snapshot Snapshot) {
    DISPrintf("------ Snapshot Head ------")
    DISPrintf("Snapshot.ServerMap:")
    for k,v := range snapshot.ServerMap {
        DISPrintf("<%s,%s>",k,v)
    }
    DISPrintf("Snapshot.LastApplyIdMap:")
    for k,v := range snapshot.LastApplyIdMap {
        DISPrintf("<%d,%d>",k,v)
    }
    DISPrintf("Snapshot.LastLogIndex: %d", snapshot.LastLogIndex)
    DISPrintf("------ Snapshot End ------")
}



//
// Index converter
//
func (rf *Raft) convertToGlobalViewIndex(raftViewIndex int) int {
    return raftViewIndex +  rf.snapshottedIndex
}

func (rf* Raft) convertToRaftLogViewIndex(kvServerViewIndex int) int {
    return kvServerViewIndex - rf.snapshottedIndex
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// Start() should return immediately, without waiting for the log appends 
// to complete
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    index := -1
    term := -1
    isLeader := true

    rf.mu.Lock()
    isLeader = rf.state == "Leader"
    if isLeader {
        newEntry := Entry{}
        newEntry.Command = command
        newEntry.Term = rf.currentTerm

        index = rf.convertToGlobalViewIndex(len(rf.log))
        term = rf.currentTerm
        rf.log = append(rf.log, newEntry)

        rf.persist()
        //go rf.startAppendEntries()

        DLCPrintf("Leader (%d) append entries and now log is from %v to %v(index=%d in Raft Log view)", rf.me, rf.log[1], rf.log[len(rf.log)-1], len(rf.log)-1)
    }
    rf.mu.Unlock()

    return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
    z := atomic.LoadInt32(&rf.dead)
    return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {

    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.applyCh = applyCh

    const Log_CAPACITY int = 100
    rf.initRaftNodeToFollower(Log_CAPACITY)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go rf.electionTimeoutListener()

    return rf
}


//
// generate random election time
//
func generateElectionTime() int {
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(150)*2 + 300
}
