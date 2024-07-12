package raft

type Node struct {
	id 			int
	currentTerm int
	votedFor    int
}