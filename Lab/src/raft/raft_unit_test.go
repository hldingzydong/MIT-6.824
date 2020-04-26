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

	err := rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	if err == nil {
		DPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	}else{
		fmt.Println(err)
	}

	requestVoteArgs.CandidateTerm = 1
	requestVoteArgs.CandidateId = 2
	err = rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	if err == nil {
		DPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	}else{
		fmt.Println(err)
	}

	requestVoteArgs.CandidateTerm = 2
	requestVoteArgs.CandidateId = 2
	err = rf.RequestVote(&requestVoteArgs, &requestVoteReply)
	if err == nil {
		DPrintf("raft/raft_unit_test.go: requestVoteReply.FollowerTerm = %d, requestVoteReply.VotedGranted = %t\n", requestVoteReply.FollowerTerm, requestVoteReply.VotedGranted)
	}else{
		fmt.Println(err)
	}

	fmt.Println("--------------------------------------")
	fmt.Println("End To Test Request Vote RPC Handler")
	fmt.Println("--------------------------------------")
}