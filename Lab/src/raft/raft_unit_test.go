package raft

import "fmt"
import "testing"
import "../labrpc"

func TestRequestVote(t *testing.T) {
	fmt.Println("--------------------------------------")
	fmt.Println("Begin To Test Request Vote RPC Handler")
	fmt.Println("--------------------------------------")

	rf := &Raft{}
	rf.peers = make([]*labrpc.ClientEnd, 1)
	rf.persister = nil
	rf.me = 0
	rf.initRaftNodeToFollower(1)

	var requestVoteArgs RequestVoteArgs
	var requestVoteReply RequestVoteReply

	requestVoteArgs.CandidateTerm = 1
	requestVoteArgs.CandidateId = 1
	requestVoteArgs.LastLogIndex = 0
	requestVoteArgs.LastLogTerm = 0

	rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	DRVPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	

	requestVoteArgs.CandidateTerm = 1
	requestVoteArgs.CandidateId = 2
	rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	DRVPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	

	requestVoteArgs.CandidateTerm = 2
	requestVoteArgs.CandidateId = 2
	rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	DRVPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	

	fmt.Println("--------------------------------------")
	fmt.Println("End To Test Request Vote RPC Handler")
	fmt.Println("--------------------------------------")
}
