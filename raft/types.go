package raft

import (
	"sync"
	"time"
)

// Server states
const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

// Constants for timeouts and heartbeats
const (
	HeartbeatInterval    = 100 * time.Millisecond 
	ElectionTimeoutMin   = 300 * time.Millisecond 
	ElectionTimeoutMax   = 600 * time.Millisecond 
	RPCTimeout           = 200 * time.Millisecond 
	MaxLogEntriesPerRPC  = 100                    
)

// Command types
const (
	CmdPut    = "PUT"
	CmdAppend = "APPEND"
	CmdGet    = "GET"
)

type LogEntry struct {
	Term    int         
	Index   int         
	Command Command     
}

type Command struct {
	Type  string      
	Key   string      
	Value interface{} 
}

type KeyValueStore struct {
	mu    sync.RWMutex
	data  map[string]string
}

// new instance of the key-value store
func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
	}
}

func (kv *KeyValueStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KeyValueStore) Append(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] += value
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.data[key]
	return value, exists
}

func (kv *KeyValueStore) ApplyCommand(cmd Command) interface{} {
	switch cmd.Type {
	case CmdPut:
		if value, ok := cmd.Value.(string); ok {
			kv.Put(cmd.Key, value)
			return true
		}
		return false
	case CmdAppend:
		if value, ok := cmd.Value.(string); ok {
			kv.Append(cmd.Key, value)
			return true
		}
		return false
	case CmdGet:
		value, exists := kv.Get(cmd.Key)
		if !exists {
			return nil
		}
		return value
	default:
		return nil
	}
}

// RequestVoteArgs holds the arguments for RequestVote RPC
type RequestVoteArgs struct {
	Term         int 
	CandidateID  int 
	LastLogIndex int 
	LastLogTerm  int 
}

// reply for RequestVote RPC
type RequestVoteReply struct {
	Term        int  
	VoteGranted bool 
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        
	LeaderID     int        
	PrevLogIndex int        
	PrevLogTerm  int        
	Entries      []LogEntry 
	LeaderCommit int        
}

//reply for AppendEntries RPC
type AppendEntriesReply struct {
	Term          int  
	Success       bool 
	ConflictIndex int  
	ConflictTerm  int  
}

// client requests
type ClientRequestArgs struct {
	Command Command 
}

// replies for client requests
type ClientRequestReply struct {
	Success bool       
	Leader  int        
	Result  interface{}
	Error   string     
}