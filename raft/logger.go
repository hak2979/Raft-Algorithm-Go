package raft

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// Simple Logs
type logger struct {
	nodeID int
	logger *log.Logger
	mu     sync.Mutex
}

// creates a new logger with the given node ID
func newLogger(nodeID int) *logger {
	return &logger{
		nodeID: nodeID,
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (l *logger) Printf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prefix := fmt.Sprintf("[Node %d] ", l.nodeID)
	l.logger.Printf(prefix+format, v...)
}

//  logs a mesg with the node ID
func (l *logger) Println(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prefix := fmt.Sprintf("[Node %d] ", l.nodeID)
	args := append([]interface{}{prefix}, v...)
	l.logger.Println(args...)
}