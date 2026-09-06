package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("trpc-wecomsample", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	mode := flags.String("mode", "", "sampling mode: sessions, inspect-sessions, inspect-status, messages, group-messages, group-reply, group-reply-read")
	snapshotFile := flags.String("sessions-file", "", "private session snapshot file")
	authorized := flags.Bool("authorized", false, "explicit authorization to read the specified test sample")
	allowSend := flags.Bool("allow-send", false, "separate authorization to send the fixed test marker once to the test group")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return fmt.Errorf("invalid sampling arguments")
	}
	if !*authorized {
		return fmt.Errorf("sampling requires explicit authorization and a supported mode")
	}
	if (*mode == "group-reply") != *allowSend {
		return fmt.Errorf("only group-reply requires and accepts separate -allow-send authorization")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if *mode == "inspect-sessions" {
		candidates, err := wecommcp.InspectSessionCandidates(*snapshotFile)
		if err != nil {
			return err
		}
		return encoder.Encode(candidates)
	}
	if *mode == "inspect-status" {
		status, err := wecommcp.InspectSampleStatus(*snapshotFile)
		if err != nil {
			return err
		}
		return encoder.Encode(status)
	}
	if *mode != "sessions" && *mode != "messages" && *mode != "group-messages" && *mode != "group-reply" && *mode != "group-reply-read" {
		return fmt.Errorf("unsupported sampling mode")
	}
	if _, err := config.LoadDotEnv(".env"); err != nil {
		return fmt.Errorf("cannot load local sample configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var report wecommcp.SnapshotReport
	var err error
	if *mode == "sessions" {
		report, err = wecommcp.ReadSessionSnapshot(ctx, os.Getenv("WECOM_MCP_URL"), "data")
	} else if *mode == "group-messages" {
		report, err = wecommcp.ReadUniqueTestGroupMessageSnapshot(ctx, os.Getenv("WECOM_MCP_URL"), *snapshotFile, "data")
	} else if *mode == "group-reply" {
		report, err = wecommcp.SendUniqueTestGroupReply(ctx, os.Getenv("WECOM_MCP_URL"), *snapshotFile, "data")
	} else if *mode == "group-reply-read" {
		report, err = wecommcp.ReadUniqueTestGroupReply(ctx, os.Getenv("WECOM_MCP_URL"), *snapshotFile, "data")
	} else {
		report, err = wecommcp.ReadUniqueTestMessageSnapshot(ctx, os.Getenv("WECOM_MCP_URL"), *snapshotFile, "data")
	}
	if err != nil {
		return err
	}
	return encoder.Encode(report)
}
