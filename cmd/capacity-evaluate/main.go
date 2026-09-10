package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice/capacity"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) > 1 {
		return errors.New("usage: capacity-evaluate [report.json]")
	}
	reader := stdin
	var file *os.File
	if len(args) == 1 {
		opened, err := os.Open(args[0])
		if err != nil {
			return err
		}
		file, reader = opened, opened
		defer file.Close()
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var report capacity.Report
	if err := decoder.Decode(&report); err != nil {
		return fmt.Errorf("decode capacity report: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("capacity report has trailing content")
	}
	result := capacity.Evaluate(report)
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}
	return result.Error()
}
