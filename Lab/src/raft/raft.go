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
// import "bytes"
// import "../labgob"


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
	state 		State

	// persistent field
	currentTerm int 			  // currentTerm
	votedFor 	int   			  // candidateId that received vote in current term (or null if none)
	leaderId    int               // leader's Id
	log         []Entry  	      // log entries


	// volatile filed
	commitIndex int 
	lastApplied int

	// election time
	electionTime int 			  // election time

	// leader's field
	nextIndex   []int
	matchIndex  []int

	// channel to communicate between threads
	followerChan 	chan int
	candidateChan 	chan int
	leaderChan		chan int
	resetElectionTime   bool
}


//
// init raft node with log capacity
//
func (rf *Raft) initRaftNodeToFollower(logCapacity int) {
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

/***************************************************************************
							Request Vote RPC
***************************************************************************/

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	CandidateTerm 		 	int   
	CandidateId  		    int
	LastLogIndex 			int
	LastLogTerm   			int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	FollowerTerm  			int
	VotedGranted 			bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] receive RequestVote from Server (%d)[CandidateTerm=%d]",
				rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.CandidateTerm)
		

	reply.FollowerTerm = rf.currentTerm
	// 先比较term,再看是否vote,如果都OK则比较谁更up-to-date

	// 如果candidate的term比follower要小,就拒绝
	if rf.currentTerm > args.CandidateTerm {
		reply.VotedGranted = false
		//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] refuse RequestVote from Server (%d)[CandidateTerm=%d]",
		//		rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.CandidateTerm)
		rf.mu.Unlock()
		return
	} else if rf.currentTerm == args.CandidateTerm {
		if  rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VotedGranted = false
			//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] refuse RequestVote from Server (%d)[CandidateTerm=%d]",
			//	rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.CandidateTerm)
			rf.mu.Unlock()
			return
		} 

		// 如果candidate的log不如follower的up-to-date，就拒绝
		var lastLogTerm = -1
		if rf.log != nil {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		// 谁的lastLogTerm更大，谁更up-to-date
		if args.LastLogTerm < lastLogTerm {
			reply.VotedGranted = false
			//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d, lastLogTerm=%d] refuse RequestVote from Server (%d)[CandidateTerm=%d, LastLogTerm=%d]",
			//		rf.me, rf.state, rf.currentTerm, rf.votedFor, lastLogTerm, args.CandidateId, args.CandidateTerm, args.LastLogTerm)
			rf.mu.Unlock()
			return
		}
		// 如果lastLogTerm相等,则比较log的长短
		if args.LastLogTerm == lastLogTerm {
			var lastLogIndex = len(rf.log) - 1
			if args.LastLogIndex < lastLogIndex {
				reply.VotedGranted = false
				//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d, lastLogTerm=%d, lastLogIndex=%d] refuse RequestVote from Server (%d)[CandidateTerm=%d, LastLogTerm=%d, LastLogIndex=%d]",
				//	rf.me, rf.state, rf.currentTerm, rf.votedFor, lastLogTerm, lastLogIndex, args.CandidateId, args.CandidateTerm, args.LastLogTerm, args.LastLogIndex)
				rf.mu.Unlock()
				return
			}
		}
	}

	// it is ok to vote for this candidate and update follower state
	reply.VotedGranted = true
	rf.currentTerm = args.CandidateTerm
	rf.votedFor = args.CandidateId
	rf.resetElectionTime = true
	rf.mu.Unlock()
	// 不管raft node现在的状态是什么, 对于一个合理的RequestVote的RPC,都要将其状态设置为Follower
	// 如果raft node是Candidate/Leader, 那遇到一个比自己Term要高的，需要滚回到Follower
	// (不可能Term相同,因为如果Term相同,则该Candidate已经为自己投过票，就会拒绝该RPC)
	// 根据test的需要,需要将election time设置为 500ms ～ 800ms 之间的随机数字, 而HeartBeat设置为
	// 每200ms一次HeartBeat,这样每秒内5次HeartBeat,满足test需要
	rf.followerChan <- 0
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


//
//  Raft Node send Request Vote To Multiple Server
//
func (rf *Raft) requestForVotes() {
	count := 0
	finished := 0
	meetHigherTerm := false

	var tmpMutex sync.Mutex
	cond := sync.NewCond(&tmpMutex)

	var args RequestVoteArgs

	rf.mu.Lock()
	args.CandidateTerm = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log)-1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers) && rf.state == "Candidate"; i++ {
		if i == rf.me {
			continue
		}else{
			go func(follower int) {
				var reply RequestVoteReply

				vote := false
				for vote == false {
					// DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] send RequestVote To Server (%d)", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower)
					vote = rf.sendRequestVote(follower, &args, &reply)
					//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] receive RequestVoteReply from Server (%d)", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower)
					if vote == true {
						tmpMutex.Lock()
						finished++;
						if reply.VotedGranted == true {
							//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] receive Vote from Server (%d)", rf.me, rf.state, rf.currentTerm, rf.votedFor, follower)
							count++;
						} else if reply.FollowerTerm > args.CandidateTerm {
							meetHigherTerm = true
						}
						tmpMutex.Unlock()
					}
				}
				// 告知主线程 Server[i]已reply
				cond.Broadcast()
			}(i)
		}
	}

	tmpMutex.Lock()
	for count < len(rf.peers)/2 && finished != len(rf.peers) && !meetHigherTerm {
		cond.Wait()
	}

	rf.mu.Lock()
	rf.resetElectionTime = true
	rf.mu.Unlock()

	//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote", rf.me, rf.state, rf.currentTerm, rf.votedFor)				
	if count >= len(rf.peers)/2 {
		rf.leaderChan <- 0
		DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote and become leader", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	}else{
		rf.followerChan <- 0
		DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] finished RequestVote and back to follower", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	}
	tmpMutex.Unlock()
}

/***************************************************************************
							Append Entries RPC
***************************************************************************/
//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	LeaderTerm				int
	LeaderId				int
	PrevLogIndex			int
	PrevLogTerm				int
	LeaderCommitIndex 		int
	Entries 				[]Entry
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	FollowerTerm  			int
	IsSuccess 			    bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if len(args.Entries) == 0 {
		DPrintf("raft/raft.go: Server (%d) received Heart Beat request from Server (%d)", rf.me, args.LeaderId)
	}else{
		DPrintf("raft/raft.go: Server (%d) received Append Entries request from Server (%d)", rf.me, args.LeaderId)
	}

	reply.FollowerTerm = rf.currentTerm
	if rf.currentTerm > args.LeaderTerm {
		reply.IsSuccess = false
		//DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] refuse Leader (%d)[LeaderTerm=%d] because Leader's term is small",
		//		rf.me, rf.state, rf.currentTerm, rf.votedFor, args.LeaderId, args.LeaderTerm)
		rf.mu.Unlock()
		return
	}/*else{
		if len(rf.log) < args.PrevLogIndex+1 {
			reply.IsSuccess = false
			rf.mu.Unlock()
			return
		}else{
			if rf.log[args.PrevLogIndex].Term < args.PrevLogTerm {
				reply.IsSuccess = false
				rf.mu.Unlock()
				return
			}
		}  
	}*/

	// 可以进行AppendEntries
	reply.IsSuccess = true
	rf.leaderId = args.LeaderId 
	rf.resetElectionTime = true
	rf.mu.Unlock()

	rf.followerChan <- 0
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	var failedCount int
	var tmpMutex sync.Mutex

	var meetHigherTerm bool
	var wg sync.WaitGroup
	// 每200ms一次 HeartBeat
	for !rf.killed() {
		if rf.state != "Leader" {
			DPrintf("Server (%d) stop sending Heart Beat", rf.me)
			break
		}

		DPrintf("Server (%d) start to send Heart Beat", rf.me)
		meetHigherTerm = false
		failedCount = 0

		for i := 0; i < len(rf.peers) && rf.state == "Leader"; i++ {
			if i == rf.me {
				continue
			}else{
				wg.Add(1)
				go func(follower int) {
					var args AppendEntriesArgs
					var reply AppendEntriesReply

					rf.mu.Lock()
					args.LeaderId = rf.me
					args.LeaderTerm = rf.currentTerm
					args.PrevLogIndex = rf.nextIndex[follower]
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					// args.LeaderCommitIndex = rf
					rf.mu.Unlock()

					ok := rf.sendAppendEntries(follower, &args, &reply)
					if ok == true && reply.IsSuccess == false {
						tmpMutex.Lock()
						failedCount++
						tmpMutex.Unlock()
						
						if reply.FollowerTerm > args.LeaderTerm {
							meetHigherTerm = true
						}
					}else if ok == false{
						tmpMutex.Lock()
						failedCount++
						tmpMutex.Unlock()
					}
					wg.Done()
				}(i)
			}
		}

		wg.Wait()

		if meetHigherTerm || failedCount > len(rf.peers)/2 {
			if meetHigherTerm {
				DPrintf("Server (%d) stop sending Heart Beat for meeting higher term", rf.me)
			}else{
				DPrintf("Server (%d) stop sending Heart Beat for losing connection", rf.me)
			}
			rf.followerChan <- 0
			break
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
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
	DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] convert to Follower", rf.me, rf.state, rf.currentTerm, rf.votedFor)	
	rf.state = "Follower"
	rf.electionTime = generateElectionTime()
	rf.resetElectionTime = false
	rf.mu.Unlock()
	// 模拟倒计时
	// 当reset election time时, 重新开始sleep
	for rf.electionTime > 0 && !rf.resetElectionTime {
		time.Sleep(time.Duration(5) * time.Millisecond)
		rf.electionTime = rf.electionTime - 5
	}
	if !rf.resetElectionTime {
		// notify to become Candidate
		DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] wake up", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		rf.candidateChan <- 0		
	}	
}

//
// raft node convert to candidate and begin request votes
//
func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] convert to Candidate", rf.me, rf.state, rf.currentTerm, rf.votedFor)	
	rf.state = "Candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTime = false
	rf.electionTime = generateElectionTime()
	rf.mu.Unlock()

	rf.beginNewElectionRound()
}

func (rf *Raft) beginNewElectionRound() {
	DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d， electionTime=%d] start request votes", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.electionTime)	
	// 启动一个线程, requestVote
	go rf.requestForVotes()
	
	for rf.electionTime > 0 && rf.state == "Candidate" && !rf.resetElectionTime{
		time.Sleep(time.Duration(5) * time.Millisecond)
		rf.electionTime = rf.electionTime - 5
	}

	if rf.electionTime <= 0 && rf.state == "Candidate" {
		DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] election time out and begin a new election round", rf.me, rf.state, rf.currentTerm, rf.votedFor)
		rf.mu.Lock()
		rf.resetElectionTime = false
		rf.electionTime = generateElectionTime()
		rf.currentTerm++
		rf.mu.Unlock()
		rf.beginNewElectionRound()
	}
}

//
// raft node convert to candidate and begin send heart beat
//
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	rf.state = "Leader"
	rf.mu.Unlock()

	DPrintf("raft/raft.go: Server (%d)[state=%s, term=%d, votedFor=%d] become leader", rf.me, rf.state, rf.currentTerm, rf.votedFor)
	// 启动一个线程,定时给各个Follower发送HeartBeat Request 
	go rf.sendHeartBeat()
}


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
	}

	close(rf.followerChan)
	close(rf.candidateChan)
	close(rf.leaderChan)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	const Log_CAPACITY int = 100
	rf.initRaftNodeToFollower(Log_CAPACITY)
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

	const Log_CAPACITY int = 100
	rf.initRaftNodeToFollower(Log_CAPACITY)

	go rf.raftServerBeginAdventure()
	//rf.followerChan <- 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}


//
// generate random election time
//
func generateElectionTime() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(150)*3 + 300
}
