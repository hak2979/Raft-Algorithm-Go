package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// RaftNode represents a node in the Raft cluster
type RaftNode struct {
	mu              sync.Mutex
	id              int                    
	peers           map[int]string         
	state           string                 
	currentTerm     int                    
	votedFor        int                    
	log             []LogEntry             
	commitIndex     int                    
	lastApplied     int                    
	nextIndex       map[int]int            
	matchIndex      map[int]int            
	electionTimeout time.Duration          
	heartbeatTimer  *time.Timer            
	electionTimer   *time.Timer            
	applyCh         chan LogEntry       
	kvStore         *KeyValueStore      
	rpcServer       *rpc.Server         
	listener        net.Listener 
	shutdown        chan struct{}          
	logger          *logger          }

// NewRaftNode initializes a new Raft node
func NewRaftNode(id int, peerAddresses map[int]string, kvStore *KeyValueStore) *RaftNode {
	node := &RaftNode{
		id:              id,
		peers:           peerAddresses,
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		log:             make([]LogEntry, 1), // Start with a dummy entry at index 0
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		electionTimeout: randomElectionTimeout(),
		applyCh:         make(chan LogEntry, 100),
		kvStore:         kvStore,
		shutdown:        make(chan struct{}),
		logger:          newLogger(id),
	}

	// Initialize log with a dummy entry at index 0
	node.log[0] = LogEntry{Term: 0}

	// Initialize nextIndex and matchIndex
	for peerID := range peerAddresses {
		if peerID != id {
			node.nextIndex[peerID] = 1
			node.matchIndex[peerID] = 0
		}
	}

	return node
}

func randomElectionTimeout() time.Duration {
	// Define the min and max in milliseconds
	min := 300
	max := 600
	ms := min + rand.Intn(max-min)
	return time.Duration(ms) * time.Millisecond
}

// resetElectionTimer resets for every election
func (r *RaftNode) resetElectionTimer() {
	if r.electionTimer == nil {
		r.electionTimer = time.NewTimer(r.electionTimeout)
	} else {
		r.electionTimer.Reset(r.electionTimeout)
	}
}


func (r *RaftNode) Start(address string) error {
	// Register RPC handlers
	r.rpcServer = rpc.NewServer()
	r.rpcServer.RegisterName("Raft", r)

	// Start  RPC 
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start RPC server: %v", err)
	}
	r.listener = listener

	// Smain loop
	go r.run()


	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-r.shutdown:
					return // Shutdown requested
				default:
					r.logger.Printf("RPC accept error: %v", err)
				}
				continue
			}
			go r.rpcServer.ServeConn(conn)
		}
	}()

	// Start the log applier routine
	go r.applyLogEntries()

	r.logger.Printf("Raft node %d started at %s", r.id, address)
	return nil
}


func (r *RaftNode) applyLogEntries() {
	for {
		select {
		case <-r.shutdown:
			return
		default:
			r.mu.Lock()
			if r.commitIndex > r.lastApplied {
				r.lastApplied++
				entry := r.log[r.lastApplied]
				r.mu.Unlock()
				
				// Apply to state machine
				r.kvStore.ApplyCommand(entry.Command)
				r.logger.Printf("Applied log entry %d to state machine", entry.Index)
			} else {
				r.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// applyLogEntry applies a log entry to the state machine
func (r *RaftNode) applyLogEntry(entry LogEntry) {
	r.kvStore.ApplyCommand(entry.Command)
	r.logger.Printf("Applied log entry %d to state machine", entry.Index)
}

// Stop stops the Raft node
func (r *RaftNode) Stop() {
	close(r.shutdown)
	if r.listener != nil {
		r.listener.Close()
	}
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
	}
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	r.logger.Printf("Raft node %d stopped", r.id)
}


func (r *RaftNode) tryAdvanceCommitIndex() {
	// Only the leader can advance the commit index
	if r.state != Leader {
		return
	}
	
	
	for n := r.commitIndex + 1; n < len(r.log); n++ {
		// Only commit entries from the current term
		if r.log[n].Term != r.currentTerm {
			continue
		}
		
		// Count replications
		count := 1 // Count ALSO OWN self
		for id := range r.peers {
			if id != r.id && r.matchIndex[id] >= n {
				count++
			}
		}
		
		//checking majority
		if count > len(r.peers)/2 {
			r.commitIndex = n
			r.logger.Printf("Advanced commit index to %d", n)
		} else {
			break
		}
	}
}

// check index
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// check index
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//It do elcetion lead ....
func (r *RaftNode) run() {
	// Generate the first election timeout
	r.electionTimeout = randomElectionTimeout()
	r.logger.Printf("Initial election timeout set to %d ms", r.electionTimeout.Milliseconds())
	
	r.electionTimer = time.NewTimer(r.electionTimeout)
	
	r.readPersistState()

	// Begin the election timeout countdown
	r.logger.Printf("Starting Raft node in %s state", r.state)

	for {
		select {
		case <-r.shutdown:
			r.logger.Println("Shutting down Raft node main loop")
			return
		case <-r.electionTimer.C:
			r.logger.Printf("ELECTION TIMER FIRED! Current state: %s", r.state)
			r.mu.Lock()
			if r.state != Leader {
				r.logger.Printf("Election timeout triggered, starting election")
				r.startElection()
			} else {
				// Reset the timer even if we're a leader (shouldn't happen normally)
				r.resetElectionTimer() 
			}
			r.mu.Unlock()
		case entry := <-r.applyCh:
			r.applyLogEntry(entry)
		}
	}
}

func (r *RaftNode) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	// Reset election timer for next election if this one fails
	r.resetElectionTimer()

	r.logger.Printf("Starting election for term %d (votes: 1/%d)", 
		r.currentTerm, len(r.peers)/2+1)

	// Prepare RequestVote arguments
	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Count votes, including self-vote
	votes := 1
	majority := len(r.peers)/2 + 1

	// Send RequestVote RPCs to all peers
	for peerID, peerAddr := range r.peers {
		if peerID == r.id {
			continue
		}

		r.logger.Printf("Requesting vote from peer %d at %s", peerID, peerAddr)

		go func(peerID int, peerAddr string) {
			var reply RequestVoteReply
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				//r.logger.Printf("Error connecting to peer %d at %s: %v", peerID, peerAddr, err)
				return
			}
			defer client.Close()

			err = client.Call("Raft.RequestVote", args, &reply)
			if err != nil {
				//r.logger.Printf("Error requesting vote from peer %d: %v", peerID, err)
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

	
			if r.state != Candidate || r.currentTerm != args.Term {
				r.logger.Printf("Ignoring vote from peer %d - no longer a candidate or term changed", peerID)
				return
			}

			if reply.Term > r.currentTerm {
				r.logger.Printf("Discovered higher term from peer %d, converting to follower", peerID)
				r.becomeFollower(reply.Term)
				return
			}

			//Vote
			if reply.VoteGranted {
				votes++
				r.logger.Printf("Received vote from peer %d, total votes: %d/%d", 
					peerID, votes, majority)
				
				// Lead
				if votes >= majority && r.state == Candidate {
					r.logger.Printf("Won election with %d/%d votes!", votes, len(r.peers))
					r.becomeLeader()
				}
			} else {
				r.logger.Printf("Peer %d rejected our vote request", peerID)
			}
		}(peerID, peerAddr)
	}
}
//Add add of lead
func (r *RaftNode) becomeFollower(term int) {
	r.logger.Printf("Becoming follower for term %d", term)
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.resetElectionTimer()
	
	// Stop sending heartbeats if we were a leader
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
	}
}


func (r *RaftNode) becomeLeader() {
	if r.state != Candidate {
		return
	}

	r.logger.Printf("Becoming leader for term %d", r.currentTerm)
	r.state = Leader

	// Initialize nextIndex and matchIndex for all peers
	for peerID := range r.peers {
		if peerID != r.id {
			r.nextIndex[peerID] = len(r.log)
			r.matchIndex[peerID] = 0
		}
	}

	// Stop the election timer
	r.electionTimer.Stop()


	r.addLogEntry(Command{Type: "NO-OP"})

	r.persistState()

	// Start sending heartbeats
	r.broadcastHeartbeats()
	r.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	go func() {
		for {
			select {
			case <-r.heartbeatTimer.C:
				r.mu.Lock()
				if r.state == Leader {
					r.broadcastHeartbeats()
					r.heartbeatTimer.Reset(HeartbeatInterval)
				} else {
					// We're no longer the leader, stop this goroutine
					r.mu.Unlock()
					return
				}
				r.mu.Unlock()
			case <-r.shutdown:
				return
			}
		}
	}()
}

func (r *RaftNode) broadcastHeartbeats() {
	for peerID, peerAddr := range r.peers {
		if peerID == r.id {
			continue
		}

		go func(peerID int, peerAddr string) {
			r.mu.Lock()
			prevLogIndex := r.nextIndex[peerID] - 1
			if prevLogIndex >= len(r.log) {
				prevLogIndex = len(r.log) - 1
			}
			prevLogTerm := r.log[prevLogIndex].Term
			entries := make([]LogEntry, 0)
			
			for i := r.nextIndex[peerID]; i < len(r.log) && len(entries) < MaxLogEntriesPerRPC; i++ {
				entries = append(entries, r.log[i])
			}
			
			args := AppendEntriesArgs{
				Term:         r.currentTerm,
				LeaderID:     r.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: r.commitIndex,
			}
			r.mu.Unlock()
			
			var reply AppendEntriesReply
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				r.logger.Printf("Error connecting to peer %d: %v", peerID, err)
				return
			}
			defer client.Close()
			
			err = client.Call("Raft.AppendEntries", args, &reply)
			if err != nil {
				r.logger.Printf("Error sending AppendEntries to peer %d: %v", peerID, err)
				return
			}
			
			r.mu.Lock()
			defer r.mu.Unlock()
			
			if r.state != Leader || r.currentTerm != args.Term {
				return
			}
			
			// If the peer has a higher term, update our term and become a follower
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				return
			}
			
			if reply.Success {
				// Update nextIndex and matchIndex for the peer
				if len(entries) > 0 {
					r.nextIndex[peerID] = args.PrevLogIndex + len(entries) + 1
					r.matchIndex[peerID] = args.PrevLogIndex + len(entries)
				}
				
				r.tryAdvanceCommitIndex()
			} else {
		
				if reply.ConflictIndex > 0 {
					r.nextIndex[peerID] = reply.ConflictIndex
				} else {
				


					r.nextIndex[peerID] = max(1, r.nextIndex[peerID]-1)
				}
			}
		}(peerID, peerAddr)
	}
}

// RequestVote handles the RequestVote RPC
func (r *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.logger.Printf("Received RequestVote from node %d for term %d", args.CandidateID, args.Term)
	
	// If the term is greater than our current term, become a follower
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	
	// Initialize reply with our current term
	reply.Term = r.currentTerm
	reply.VoteGranted = false
	
	// If the candidate's term is less than our term, deny the vote
	if args.Term < r.currentTerm {
		return nil
	}
	
	// If we've already voted for someone else in this term, deny the vote
	if r.votedFor != -1 && r.votedFor != args.CandidateID {
		return nil
	}
	
	// Check if the candidate's log is at least as up-to-date as ours
	lastLogIndex := len(r.log) - 1
	lastLogTerm := r.log[lastLogIndex].Term
	
	if lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		// Our log is more up-to-date, deny the vote
		return nil
	}
	
	// Grant the vote
	reply.VoteGranted = true
	r.votedFor = args.CandidateID
	r.resetElectionTimer()
	r.logger.Printf("Granted vote to node %d for term %d", args.CandidateID, args.Term)
	
	return nil
}

// AppendEntries handles the AppendEntries RPC
func (r *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	reply.Term = r.currentTerm
	reply.Success = false
	
	if args.Term < r.currentTerm {
		return nil
	}
	
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	
	r.resetElectionTimer()
	
	// If we a candidate, step down
	if r.state == Candidate {
		r.becomeFollower(args.Term)
	}
	
	if args.PrevLogIndex >= len(r.log) {
		reply.ConflictIndex = len(r.log)
		return nil
	}
	
	if args.PrevLogIndex > 0 && r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := r.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 1; i-- {
			if r.log[i-1].Term != term {
				reply.ConflictIndex = i
				break
			}
		}
		return nil
	}
	
	// Success
	reply.Success = true
	
	// Process log entries
	if len(args.Entries) > 0 {
		r.logger.Printf("Received %d log entries from leader", len(args.Entries))
		
		// Find insertion point
		insertIndex := args.PrevLogIndex + 1
	
		for i, entry := range args.Entries {
			if insertIndex+i >= len(r.log) {
				// Append new entries
				r.log = append(r.log, args.Entries[i:]...)
				break
			} else if r.log[insertIndex+i].Term != entry.Term {
				// Conflicting entry, truncate log and append new entries
				r.log = r.log[:insertIndex+i]
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
		}
	}
	
	if args.LeaderCommit > r.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(r.log)-1)
		if newCommitIndex > r.commitIndex {
			r.commitIndex = newCommitIndex
			r.logger.Printf("Updated commit index to %d", r.commitIndex)
		}
	}
	
	return nil
}

// ClientRequest handles client requests
func (r *RaftNode) ClientRequest(args ClientRequestArgs, reply *ClientRequestReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	reply.Success = false
	reply.Leader = -1
	
	// If we're not the leader, redirect to the leader if known
	if r.state != Leader {
		reply.Leader = r.votedFor
		return nil
	}
	
	// We are the leader
	reply.Leader = r.id
	
	// Handle GET requests immediately
	if args.Command.Type == CmdGet {
		result := r.kvStore.ApplyCommand(args.Command)
		reply.Success = true
		reply.Result = result
		return nil
	}
	
	logIndex := len(r.log)
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   logIndex,
		Command: args.Command,
	}
	
	// Append to local log
	r.log = append(r.log, entry)
	r.matchIndex[r.id] = logIndex
	
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.mu.Lock()
		defer r.mu.Unlock()
		
		if r.commitIndex >= logIndex {
			// Command has been committed
			result := r.kvStore.ApplyCommand(args.Command)
			reply.Success = true
			reply.Result = result
		} else {
			reply.Error = "command timed out"
		}
	}()
	
	return nil
}

func (r *RaftNode) addLogEntry(cmd Command) (int, int) {

	// Returns the log index and term
	
	logIndex := len(r.log)
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   logIndex,
		Command: cmd,
	}
	
	// Append to local log
	r.log = append(r.log, entry)
	r.matchIndex[r.id] = logIndex
	
	go r.broadcastHeartbeats()
	
	return logIndex, r.currentTerm
}

func (r *RaftNode) applyCommittedEntries() {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry := r.log[r.lastApplied]
		r.kvStore.ApplyCommand(entry.Command)
		r.logger.Printf("Applied entry %d to state machine: %v", r.lastApplied, entry.Command)
	}
}

func (r *RaftNode) persistState() {

	r.logger.Printf("State would be persisted: term=%d, votedFor=%d, logEntries=%d", 
		r.currentTerm, r.votedFor, len(r.log))
}

func (r *RaftNode) readPersistState() {

	//r.logger.Printf("Would read persisted state (not implemented in this version)")
}

// checkIfLeader returns whether this node is the leader
func (r *RaftNode) checkIfLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

// getStats returns statistics
func (r *RaftNode) getStats() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	stats := map[string]interface{}{
		"nodeId":        r.id,
		"state":         r.state,
		"term":          r.currentTerm,
		"votedFor":      r.votedFor,
		"logLength":     len(r.log),



		"commitIndex":   r.commitIndex,
		"lastApplied":   r.lastApplied,


		"peerCount":     len(r.peers),
	}
	
	if r.state == Leader {
		// Add leader-specific stats
		nextIndexStats := make(map[string]int)
		matchIndexStats := make(map[string]int)
		
		for peerID, nextIdx := range r.nextIndex {
			if peerID != r.id {
				peerKey := fmt.Sprintf("%d", peerID)
				nextIndexStats[peerKey] = nextIdx
				matchIndexStats[peerKey] = r.matchIndex[peerID]
			}
		}
		
		stats["nextIndex"] = nextIndexStats
		stats["matchIndex"] = matchIndexStats
	}
	
	return stats
}