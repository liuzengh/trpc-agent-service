package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
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
	flags := flag.NewFlagSet("trpc-wecomcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	envFile := flags.String("env-file", ".env", "local dotenv file")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("invalid checker arguments")
	}
	if _, err := config.LoadDotEnv(*envFile); err != nil {
		return fmt.Errorf("cannot load dotenv configuration (details omitted)")
	}
	endpoint := strings.TrimSpace(os.Getenv("WECOM_MCP_URL"))
	if endpoint == "" {
		return fmt.Errorf("WECOM_MCP_URL is required in the local .env")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := wecommcp.Discover(ctx, endpoint)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
