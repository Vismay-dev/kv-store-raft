package raft

import (
	"encoding/gob"
	"fmt"
	"os"
)

type PersistedData struct {
	CurrentTerm int
	VotedFor    int
	Log         []map[string]interface{}
}

// called within a lock - don't use withLock() here

func (rf *Raft) persist() error {
	storagePath := rf.storage
	obj := PersistedData{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}

	filename := fmt.Sprintf("%s/%d", storagePath, rf.me)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(obj); err != nil {
		return err
	}

	return nil
}

// called within a lock - don't use withLock() here

func (rf *Raft) readPersist() error {
	storagePath := rf.storage
	filename := fmt.Sprintf("%s/%d", storagePath, rf.me)

	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var obj PersistedData

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&obj); err != nil {
		return err
	}

	rf.currentTerm = obj.CurrentTerm
	rf.votedFor = obj.VotedFor
	rf.log = obj.Log

	return nil
}
