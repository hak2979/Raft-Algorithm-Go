package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	
	"block/raft"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <serverAddr1> [serverAddr2] [serverAddr3] ...")
		os.Exit(1)
	}

	// Collect server addresses from command-line arguments
	serverAddresses := os.Args[1:]
	
	// Create a new client with the server addresses
	client := raft.NewClient(serverAddresses)
	
	fmt.Println("Raft Client started. Type commands in the format: GET key, PUT key value, or APPEND key value")
	fmt.Println("Type 'exit' to quit")
	
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		
		input := scanner.Text()
		if input == "exit" {
			break
		}
		
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}
		
		command := strings.ToUpper(parts[0])
		
		switch command {
		case "GET":
			if len(parts) != 2 {
				fmt.Println("Usage: GET key")
				continue
			}
			key := parts[1]
			value, err := client.Get(key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s = %s\n", key, value)
			}
			
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Usage: PUT key value")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			err := client.Put(key, value)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Successfully set %s = %s\n", key, value)
			}
			
		case "APPEND":
			if len(parts) < 3 {
				fmt.Println("Usage: APPEND key value")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			err := client.Append(key, value)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Successfully appended to %s\n", key)
			}
			
		default:
			fmt.Println("Unknown command. Available commands: GET, PUT, APPEND")
		}
	}
}