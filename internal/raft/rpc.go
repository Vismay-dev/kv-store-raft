package raft

type RequestVoteRequest struct {
	Term        int
	CandidateId int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term     int
	LeaderId int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}
