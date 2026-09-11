package main

import (
	"flag"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/deploy/permissions"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	f := flag.NewFlagSet("permissions", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	format := f.String("format", "sql", "sql or redis-acl")
	schema := f.String("schema", "agent_platform", "dedicated migrated schema (not public)")
	prefix := f.String("role-prefix", "trpc", "new NOLOGIN group/user prefix")
	keyPrefix := f.String("redis-prefix", "trpc-agent-production", "platform Redis namespace")
	if f.Parse(args) != nil || f.NArg() != 0 {
		return fmt.Errorf("invalid policy arguments")
	}
	var text string
	var err error
	switch *format {
	case "sql":
		text, err = permissions.SQL(*schema, *prefix)
	case "redis-acl":
		text, err = permissions.Redis(*prefix, *keyPrefix)
	default:
		return fmt.Errorf("unsupported policy format")
	}
	if err != nil {
		return err
	}
	_, err = io.WriteString(out, text)
	return err
}
