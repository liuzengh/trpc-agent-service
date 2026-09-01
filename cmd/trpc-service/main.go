package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

const (
	demoUserID    = "demo-user"
	demoSessionID = "demo-session"
)

func main() {
	configPath := flag.String("config", config.DefaultPath, "path to YAML config file")
	flag.Parse()

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	reg, err := agent.NewRegistry(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init runners: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	current := reg.Default()
	fmt.Printf("agent ready, tenants=%v, current=%s (user=%s, session=%s)\n",
		reg.IDs(), current, demoUserID, demoSessionID)
	fmt.Println("commands: /tenant [id] switch or list tenants; empty line or Ctrl+C exits")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[%s] > ", current)
		if !scanner.Scan() {
			break
		}
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			break
		}
		if next, ok := handleCommand(reg, &current, text); ok {
			fmt.Println(next)
			continue
		}
		r, _ := reg.Runner(current)
		if err := chat(ctx, r, text); err != nil {
			fmt.Fprintf(os.Stderr, "chat: %v\n", err)
			if ctx.Err() != nil {
				break
			}
		}
		fmt.Println()
	}
}

// handleCommand interprets "/"-prefixed REPL commands. It returns a message
// to print and ok=true when the line was a command rather than chat input.
func handleCommand(reg *agent.Registry, current *string, text string) (string, bool) {
	if !strings.HasPrefix(text, "/") {
		return "", false
	}
	fields := strings.Fields(text)
	switch fields[0] {
	case "/tenant":
		if len(fields) == 1 {
			return fmt.Sprintf("tenants: %v (current: %s)", reg.IDs(), *current), true
		}
		if _, ok := reg.Runner(fields[1]); !ok {
			return fmt.Sprintf("unknown tenant %q, available: %v", fields[1], reg.IDs()), true
		}
		*current = fields[1]
		return fmt.Sprintf("switched to tenant %s", *current), true
	case "/exit", "/quit":
		os.Exit(0)
	}
	return fmt.Sprintf("unknown command %q", fields[0]), true
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
