package raft

type RequestVoteRequest struct {
	term 			int
	candidateId		int
}

type RequestVoteResponse struct {
	term 			int
	voteGranted		bool
}

type AppendEntriesRequest struct {
	term 			int
}

type AppendEntriesResponse struct {
	term 			int
	success			bool
}