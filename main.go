package main

import ("fmt")
import ("math/rand")
import ("os")
import ("os/signal")
import ("strconv")
import ("syscall")
import ("time")

import ("block/raft")

func main() {
	// It will only run if lets say nodes are greater than 2 so that there should be proper consenus
	if len(os.Args) < 3 {
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())

	// It will just read all args
	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Invalid node ID: %v\n", err)
		os.Exit(1)
	}

	// Use a different seed for each node for things like timeout
	rand.Seed(time.Now().UnixNano() + int64(nodeID))

	port := os.Args[2]
	address := fmt.Sprintf("localhost:%s", port)


	//Mapped all ports
	peerAddresses := make(map[int]string)
	

	peerAddresses[nodeID] = address
	

	peerID := 1
	for i := 3; i < len(os.Args); i++ {
		peerPort := os.Args[i]
		
		if peerID == nodeID {
			peerID++
		}
		
		peerAddresses[peerID] = fmt.Sprintf("localhost:%s", peerPort)
		peerID++
	}

	fmt.Printf("Starting node %d at %s with peers: %v\n", nodeID, address, peerAddresses)

	// Create key-value store
	kvStore := raft.NewKeyValueStore()

	// Create and start Raft node
	node := raft.NewRaftNode(nodeID, peerAddresses, kvStore)
	err = node.Start(address)
	if err != nil {
		fmt.Printf("Failed to start node: %v\n", err)
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	<-c
	fmt.Println("Shutting down...")
	node.Stop()
	fmt.Println("Node stopped")
}