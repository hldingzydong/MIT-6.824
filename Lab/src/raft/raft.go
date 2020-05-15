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
// import "fmt"
import "bytes"
import "../labgob"


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

/*
 * ApplyMessage sent to applyCh
 */
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
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

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // state
    state       State

    // persistent field
    currentTerm int               // currentTerm
    votedFor    int               // candidateId that received vote in current term (or null if none)
    leaderId    int               // leader's Id
    log         []Entry           // log entries


    // volatile filed
    commitIndex int 
    lastApplied int

    // election time
    electionTime int              // election time

    // leader's field
    nextIndex   []int
    matchIndex  []int

    // channel to communicate between threads
    followerChan    chan int
    candidateChan   chan int
    leaderChan      chan int
    resetElectionTime   bool

    // apply channel 
    applyCh         chan ApplyMsg
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

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for i:=0; i<len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
        rf.matchIndex[i] = 0
    }

    rf.resetElectionTime = false
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    DLCPrintf("persist Server(%d) state(currentTerm=%d, votedFor=%d) done", rf.me, rf.currentTerm, rf.votedFor)
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
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

    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.log = log
    DLCPrintf("Read Server(%d) state(currentTerm=%d, votedFor=%d) from persister done", rf.me, rf.currentTerm, rf.votedFor)
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

    lastLogIndex := len(rf.log) - 1
    lastLogTerm := rf.log[lastLogIndex].Term
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
        rf.resetElectionTime = true
        rf.persist()
        rf.mu.Unlock()
        rf.followerChan <- 0
        return
    }else if reply.VotedGranted {
        rf.resetElectionTime = true
        rf.persist()
        rf.mu.Unlock()
        rf.followerChan <- 0
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
                args.LastLogIndex = len(rf.log)-1
                args.LastLogTerm = rf.log[args.LastLogIndex].Term
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
        rf.resetElectionTime = true
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
            rf.leaderChan <- 0
            return
        }else{ //如果遇到 higher term 或者 大多数的reply是false
            DRVPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote and back to follower", rf.me, rf.state, rf.currentTerm, rf.votedFor)
            rf.persist()
            rf.mu.Unlock()
            tmpMutex.Unlock()
            rf.followerChan <- 0
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
        DLCPrintf("Server (%d)[term=%d] received Append Entries request[LeaderTerm=%d] from Server (%d)", rf.me, rf.currentTerm, args.LeaderTerm, args.LeaderId)
    }else{
        DLCPrintf("Server (%d) received Heart Beat request from Server (%d)", rf.me, args.LeaderId)
    }

    if rf.currentTerm > args.LeaderTerm {
        reply.IsSuccess = false
        reply.FollowerTerm = rf.currentTerm
        rf.mu.Unlock()
        return
    }

    // 如果PrevLogIndex在log中不存在
    if len(rf.log) <= args.PrevLogIndex {
        reply.IsSuccess = false
        reply.FollowerTerm = args.LeaderTerm
        reply.ConflictTerm = rf.log[len(rf.log)-1].Term
        reply.ConflictIndex = len(rf.log)
    }else { 
        // 如果存在, 则比较PrevLogTerm
        lastTerm := rf.log[args.PrevLogIndex].Term
        if lastTerm != args.PrevLogTerm {
            reply.IsSuccess = false
            reply.FollowerTerm = args.LeaderTerm
            reply.ConflictTerm = lastTerm

            // 找到该Term中的第一个index
            i := args.PrevLogIndex
            for i>=1 && rf.log[i].Term == lastTerm{
                i--
            }
            reply.ConflictIndex = i+1
        }else{
            // 可以进行Append Entries
            reply.IsSuccess = true
            reply.FollowerTerm = args.LeaderTerm
            rf.leaderId = args.LeaderId

            if len(args.Entries) > 0 {
                // 先找到 conflict entry 的index
                conflictIndex := args.PrevLogIndex + 1
                conflictIndexInArgs := 0
                for conflictIndex < len(rf.log) && conflictIndexInArgs < len(args.Entries) && rf.log[conflictIndex].Term == args.Entries[conflictIndexInArgs].Term {
                    conflictIndex++
                    conflictIndexInArgs++
                }

                if len(rf.log) > 1 {
                    rf.log = rf.log[:conflictIndex]
                }

                for i:=conflictIndexInArgs; i<len(args.Entries);i++ {
                    rf.log = append(rf.log, args.Entries[i])
                }
                DAEPrintf("Server (%d) append entries and now log is %v", rf.me, rf.log)
            }

            // 此时应当还check lastApplied和args.CommitIndex
            if args.LeaderCommitIndex > rf.commitIndex {
                go func(commitIndex int, logLength int){
                    i := commitIndex+1

                    var newCommitIndex int
                    if args.LeaderCommitIndex < logLength-1 {
                        newCommitIndex = args.LeaderCommitIndex
                    }else{
                        newCommitIndex = logLength-1
                    }
        
                    if rf.lastApplied < newCommitIndex {
                        applyMsgArray := make([]ApplyMsg, newCommitIndex-i+1)

                        rf.mu.Lock()
                        for ; i <= newCommitIndex; i++ {
                            applyMsg := ApplyMsg{}
                            applyMsg.CommandValid = true
                            applyMsg.Command = rf.log[i].Command
                            applyMsg.CommandIndex = i

                            applyMsgArray[i-commitIndex-1] = applyMsg
                        }

                        DAEPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] apply message from index=%d to index=%d to applyCh", 
                            rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.commitIndex, newCommitIndex)
                        rf.commitIndex = newCommitIndex
                        rf.lastApplied = rf.commitIndex
                        rf.mu.Unlock()
            
                        for _, applyMsg := range applyMsgArray {
                            rf.applyCh <- applyMsg
                        }
                    }
                }(rf.commitIndex, len(rf.log))
            }
        }
    } 

    if rf.currentTerm < args.LeaderTerm || reply.IsSuccess {
        rf.currentTerm = args.LeaderTerm
        rf.resetElectionTime = true
        rf.persist()
        rf.mu.Unlock()
        rf.followerChan <- 0
        return
    }
    rf.persist()
    rf.mu.Unlock()
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

//
// As a leader, need to send Append Entries RPC periodically to followers
// when Entry is empty, it is a heartbeat
//
func (rf *Raft) sendAppendEntriesToMultipleFollowers() {
    meetHigherTerm := -1

    for !rf.killed() {
        DAEPrintf("Server (%d) start to send Append Entries", rf.me)
        
        for i := 0; i < len(rf.peers); i++ {
            if i == rf.me {
                continue
            }else{
                go func(followerId int) {
                    rf.mu.Lock()
                    if rf.state == "Leader"{
                        var args AppendEntriesArgs
                        var reply AppendEntriesReply

                        args.LeaderId = rf.me
                        args.LeaderTerm = rf.currentTerm
                        args.PrevLogIndex = rf.nextIndex[followerId]-1
                        args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
                        args.LeaderCommitIndex = rf.commitIndex
                        if rf.nextIndex[followerId] != len(rf.log) {
                            args.Entries = rf.log[rf.nextIndex[followerId]:]
                        }
                        rf.mu.Unlock()  

                        ok := rf.sendAppendEntries(followerId, &args, &reply)
                    
                        rf.mu.Lock()
                        if ok && rf.currentTerm == args.LeaderTerm {
                            if !reply.IsSuccess {
                                // 如果Leader发现了更高的term,就convert to follower
                                if reply.FollowerTerm > args.LeaderTerm && rf.state == "Leader" {
                                        meetHigherTerm = reply.FollowerTerm
                                }else{
                                    // find conflict term
                                    conflictIndex := 1
                                    for conflictIndex < len(rf.log) && rf.log[conflictIndex].Term < reply.ConflictTerm {
                                        conflictIndex++
                                    }
                                    if conflictIndex != len(rf.log) && rf.log[conflictIndex].Term == reply.ConflictTerm {
                                        DLCPrintf("549: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], conflictIndex + 1)
                                        rf.nextIndex[followerId] = conflictIndex + 1
                                    }else{
                                        DLCPrintf("552: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], reply.ConflictIndex)
                                        rf.nextIndex[followerId] = reply.ConflictIndex
                                    }
                                }
                            }else{
                                DLCPrintf("557: Server (%d) update nextIndex[%d] from %d to %d", rf.me, followerId, rf.nextIndex[followerId], args.PrevLogIndex + len(args.Entries) + 1)
                                rf.nextIndex[followerId] = args.PrevLogIndex + len(args.Entries) + 1
                                rf.matchIndex[followerId] = args.PrevLogIndex + len(args.Entries)
                            }
                        }
                        rf.mu.Unlock()
                    }else{
                        rf.mu.Unlock()
                    }
                }(i)
            }
        }

        rf.mu.Lock()
        if meetHigherTerm != -1 {
            DLCPrintf("Server (%d) is meet higher term and stop sending Heart Beat", rf.me)
            rf.currentTerm = meetHigherTerm
            rf.state = "Follower"
            rf.persist()
            rf.mu.Unlock()
            rf.followerChan <- 0
            return
        }

        if rf.state != "Leader" {
            DLCPrintf("Server (%d) is no longer Leader and stop sending Heart Beat", rf.me)
            rf.persist()
            rf.mu.Unlock()
            return
        }
        rf.mu.Unlock()

        go rf.commitEntries()

        time.Sleep(100 * time.Millisecond)
    }
}


func (rf *Raft) commitEntries() {
    var count int
    var applyMsgArray []ApplyMsg
    ableCommit := false

    rf.mu.Lock()
    i := rf.commitIndex + 1
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
                    if rf.matchIndex[j] >= i {
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
        applyMsgArray = make([]ApplyMsg, i-rf.commitIndex-1)
        for k := rf.commitIndex+1; k < i; k++ {
            applyMsg := ApplyMsg{}
            applyMsg.CommandValid = true
            applyMsg.Command = rf.log[k].Command
            applyMsg.CommandIndex = k

            applyMsgArray[k-rf.commitIndex-1] = applyMsg
        }
        DAEPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] apply message from index=%d to index=%d to applyCh", 
            rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.commitIndex, i-1)
        rf.commitIndex = i-1
        rf.lastApplied = rf.commitIndex
    }
    rf.mu.Unlock()

    if applyMsgArray != nil && len(applyMsgArray) > 0 {
        for _, applyMsg := range applyMsgArray {
            rf.applyCh <- applyMsg
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
    // rf.votedFor = -1
    rf.electionTime = generateElectionTime()
    rf.resetElectionTime = false
    rf.mu.Unlock()
    for rf.electionTime > 0 && !rf.resetElectionTime {
        time.Sleep(time.Duration(5) * time.Millisecond)
        rf.electionTime = rf.electionTime - 5
    }
    if !rf.resetElectionTime {
        //DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d, electionTime=%d] wake up", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)
        rf.candidateChan <- 0       
    }   
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
    rf.resetElectionTime = false
    rf.electionTime = generateElectionTime()
    rf.persist()
    rf.mu.Unlock()

    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d， electionTime=%d] start request votes", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)    
    // 启动一个线程, requestVote
    go rf.requestForVotes()
    
    // DLCrintf("Server (%d)[state=%s, term=%d, votedFor=%d, electionTime=%d] start to count down", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)
    for rf.electionTime > 0 && rf.state == "Candidate" && !rf.resetElectionTime{
        time.Sleep(time.Duration(5) * time.Millisecond)
        rf.electionTime = rf.electionTime - 5
    }

    rf.mu.Lock()
    if rf.electionTime <= 0 && rf.state == "Candidate" {
        DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d, electionTime=%d] election time out and begin a new election round", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)
        rf.mu.Unlock()
        rf.candidateChan <- 0
    }else{
        rf.mu.Unlock()
    }
}

//
// raft node convert to candidate and begin send heart beat
//
func (rf *Raft) convertToLeader() {
    rf.mu.Lock()
    DLCPrintf("Server (%d)[state=%s, term=%d, votedFor=%d] convert to Leader", rf.me, rf.state, rf.currentTerm, rf.votedFor)    
    rf.state = "Leader"
    for i:=0; i<len(rf.peers); i++ {
        rf.nextIndex[i] = len(rf.log)
        rf.matchIndex[i] = 0
    }
    rf.mu.Unlock()
    // 启动一个线程,定时给各个Follower发送HeartBeat Request 
    time.Sleep(50 * time.Millisecond)
    go rf.sendAppendEntriesToMultipleFollowers()
}


//
// Manage Server LifeCycle
//
func (rf *Raft) raftServerBeginAdventure() {
    rf.followerChan = make(chan int, 1)
    rf.candidateChan = make(chan int, 1)
    rf.leaderChan = make(chan int, 1)

    rf.followerChan <- 0

    for !rf.killed() {
        select{
            case <-rf.followerChan:
                rf.convertToFollower()

            case <-rf.candidateChan:
                rf.convertToCandidate()

            case <-rf.leaderChan:
                rf.convertToLeader()
        }
        // time.Sleep(10 * time.Millisecond)
    }
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

        index = len(rf.log)
        term = rf.currentTerm
        rf.log = append(rf.log, newEntry)

        DLCPrintf("Leader (%d) append entries and now log is %v", rf.me, rf.log)
    }
    rf.mu.Unlock()

    if isLeader {
        rf.persist()
    }

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

    //fmt.Printf("Server (%d), receiveRVCount = %d, receiveAECount = %d, sendRVCount = %d, sendAECount = %d\n", rf.me, rf.receiveRVCount, rf.receiveAECount, rf.sendRVCount, rf.sendAECount)

    // const Log_CAPACITY int = 1200
    // rf.initRaftNodeToFollower(Log_CAPACITY)
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

    const Log_CAPACITY int = 1200
    rf.initRaftNodeToFollower(Log_CAPACITY)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go rf.raftServerBeginAdventure()

    return rf
}


//
// generate random election time
//
func generateElectionTime() int {
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(200) + 200
}
