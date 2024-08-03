package raft

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

type PersistedData struct {
	CurrentTerm int
	VotedFor    int
	Log         []map[string]interface{}
}

func (rf *Raft) persist() error {
	cwd, _ := os.Getwd()
	projectRoot := filepath.Dir(filepath.Dir(cwd))
	storagePath := filepath.Join(projectRoot, "server_store")

	filename := fmt.Sprintf("%s/%d", storagePath, rf.me)

	obj := PersistedData{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}

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

func (rf *Raft) readPersist() error {
	cwd, _ := os.Getwd()
	projectRoot := filepath.Dir(filepath.Dir(cwd))
	storagePath := filepath.Join(projectRoot, "server_store")

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
