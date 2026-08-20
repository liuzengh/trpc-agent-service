package main

import (
	"fmt"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
)

func main() {
	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
	}
}
