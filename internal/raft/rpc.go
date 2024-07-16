package raft

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}
type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []map[string]interface{}
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type ClientReqRequest struct {
	Entries []map[string]interface{}
}
type ClientReqResponse struct {
	CommitIndex int
	Success     bool
}
