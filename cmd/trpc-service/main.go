package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

const (
	demoUserID    = "demo-user"
	demoSessionID = "demo-session"
)

func main() {
	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")

	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		return
	}

	r, err := agent.NewRunner()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init runner: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Printf("agent ready (user=%s, session=%s), type a message to chat, "+
		"empty line or Ctrl+C to exit\n", demoUserID, demoSessionID)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			break
		}
		if err := chat(ctx, r, text); err != nil {
			fmt.Fprintf(os.Stderr, "chat: %v\n", err)
			if ctx.Err() != nil {
				break
			}
		}
		fmt.Println()
	}
}

// chat sends one user message and streams the response chunks to stdout.
// Events must be drained until the channel closes, otherwise the agent
// goroutine may block on channel writes.
func chat(ctx context.Context, r runner.Runner, text string) error {
	events, err := r.Run(ctx, demoUserID, demoSessionID,
		model.NewUserMessage(text))
	if err != nil {
		return err
	}
	for ev := range events {
		if ev.IsError() {
			return fmt.Errorf("agent error: %s", ev.Response.Error.Message)
		}
		if ev.Response.Object == model.ObjectTypeChatCompletionChunk {
			fmt.Print(ev.Response.Choices[0].Delta.Content)
		}
	}
	return ctx.Err()
}
