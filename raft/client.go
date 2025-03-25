package raft

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// Client represents a client that interacts with the Raft cluster
type Client struct {
	serverAddresses []string       // List of server addresses in the cluster
	leaderID        int            // Current known leader ID (-1 if unknown)
	leaderAddr      string         // Current leader address (empty if unknown)
	rpcTimeout      time.Duration  // Timeout for RPC calls
	mu              sync.Mutex     // Mutex for protecting shared state
}

// NewClient creates a new client that connects to the Raft cluster
func NewClient(serverAddresses []string) *Client {
	return &Client{
		serverAddresses: serverAddresses,
		leaderID:        -1,
		leaderAddr:      "",
		rpcTimeout:      RPCTimeout,
	}
}

// findLeader attempts to find the current leader by querying all servers
func (c *Client) findLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Try each server until we find the leader
	for _, addr := range c.serverAddresses {
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			continue // Can't connect to this server, try next one
		}
		
		// Create a dummy request just to find the leader
		args := ClientRequestArgs{
			Command: Command{Type: CmdGet, Key: "dummy-key"},
		}
		var reply ClientRequestReply
		
		err = client.Call("Raft.ClientRequest", args, &reply)
		client.Close()
		
		if err == nil && reply.Leader >= 0 {
			// Found a server that knows about the leader
			c.leaderID = reply.Leader
			
			// If this server is the leader, remember its address
			if reply.Success {
				c.leaderAddr = addr
				return true
			}
			
			// Otherwise, try to find the leader's address
			for i, serverAddr := range c.serverAddresses {
				if i == reply.Leader {
					c.leaderAddr = serverAddr
					return true
				}
			}
		}
	}
	
	return false
}

// executeCommand executes a command on the Raft cluster
func (c *Client) executeCommand(cmd Command) (interface{}, error) {
	// First, check if we know the leader
	c.mu.Lock()
	leaderAddr := c.leaderAddr
	c.mu.Unlock()
	
	// If we don't know the leader, try to find it
	if leaderAddr == "" {
		if !c.findLeader() {
			return nil, fmt.Errorf("failed to find leader")
		}
		
		c.mu.Lock()
		leaderAddr = c.leaderAddr
		c.mu.Unlock()
	}
	
	// Connect to the leader
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		// If we can't connect to the leader, clear our cached leader info and try again
		c.mu.Lock()
		c.leaderID = -1
		c.leaderAddr = ""
		c.mu.Unlock()
		
		return c.executeCommand(cmd) // Recursive call to retry with new leader
	}
	defer client.Close()
	
	// Prepare the request
	args := ClientRequestArgs{Command: cmd}
	var reply ClientRequestReply
	
	// Execute the command
	err = client.Call("Raft.ClientRequest", args, &reply)
	if err != nil {
		return nil, err
	}
	
	// If this server is not the leader, update our leader info and try again
	if !reply.Success && reply.Leader >= 0 && reply.Leader != c.leaderID {
		c.mu.Lock()
		c.leaderID = reply.Leader
		c.leaderAddr = ""  // Clear this so we'll look up the address in the next call
		c.mu.Unlock()
		
		return c.executeCommand(cmd) // Recursive call to retry with new leader
	}
	
	// If there was an error from the server, return it
	if !reply.Success && reply.Error != "" {
		return nil, fmt.Errorf(reply.Error)
	}
	
	return reply.Result, nil
}

// Get retrieves a value for a key
func (c *Client) Get(key string) (string, error) {
	cmd := Command{
		Type: CmdGet,
		Key:  key,
	}
	
	result, err := c.executeCommand(cmd)
	if err != nil {
		return "", err
	}
	
	if result == nil {
		return "", fmt.Errorf("key not found: %s", key)
	}
	
	return result.(string), nil
}

// Put sets a value for a key
func (c *Client) Put(key, value string) error {
	cmd := Command{
		Type:  CmdPut,
		Key:   key,
		Value: value,
	}
	
	_, err := c.executeCommand(cmd)
	return err
}

// Append adds a value to an existing key
func (c *Client) Append(key, value string) error {
	cmd := Command{
		Type:  CmdAppend,
		Key:   key,
		Value: value,
	}
	
	_, err := c.executeCommand(cmd)
	return err
}